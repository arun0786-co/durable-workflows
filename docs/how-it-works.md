# How it works

## 1. The problem

A business process like "reserve stock, charge the card, wait three days for the warehouse, refund if it never ships" outlives any single process. Writing it as a cron job plus a status column means hand-writing a state machine. Every step needs to be retry-safe, and every crash point needs thinking through.

Durable execution keeps the process as ordinary code instead:

```python
async def order(ctx, order_id, items, cents):
    res = await ctx.activity(reserve, order_id, items)
    ch = await ctx.activity(charge, order_id, cents)
    which, _ = await ctx.first(ctx.wait_signal("shipped"), ctx.sleep(3 * 86400))
    if which == 1:
        await ctx.activity(refund, order_id, ch["charge"])
```

The engine makes it survive crashes. The code can be killed at any line and continues on another machine as if nothing happened. This repo implements the model that Temporal and Azure Durable Functions use: an event history plus deterministic replay.

## 2. History and replay (`runtime.py`)

A workflow never keeps state anywhere except in its local variables, and those are rebuilt by re-running the code. The engine stores an append-only history per run:

```
 1  started            input ["order-2", ...]
 2  task_done          time 1000.0
 3    activity_scheduled seq 1 reserve(...)
 4  activity_done      seq 1 result {...}
 5  task_done          time 1000.4
 6    activity_scheduled seq 2 charge(...)
```

Events come in two kinds:
- **Inputs** come from outside the code: started, activity results, timers firing, signals, cancellation.
- **Commands** are what the code decided: schedule an activity, start a timer, start a child, complete.

Each time new inputs arrive, a *workflow task* runs the code. It appends `task_done` followed by the commands the code produced, in one transaction. So the history is a sequence of batches: some inputs, then `task_done`, then that task's commands.

To run a workflow task, the worker replays the code from the start (`Replayer.run`):
1. For each recorded batch, apply its inputs in history order (resolving futures, buffering signals), then run the coroutines until they are all blocked.
2. Each command the code emits during replay is compared with the next recorded one. A match gets the recorded result instead of running again. A mismatch is non-determinism.
3. After the last recorded batch, apply the inputs no task has seen yet and run. The commands emitted now are new, and are returned for the store to append.

Two things make the replay take exactly the same path:
- **Same batches.** Inputs are applied in the same groups as the first time. `ctx.first(signal, timer)` therefore picks the same winner even if both arrived together.
- **A deterministic scheduler.** `Scheduler` is a FIFO run queue over plain coroutines; no asyncio. Awaiting anything that is not a durable future throws a `TypeError` into the workflow, so `asyncio.sleep` is refused.

What the code may use is on `ctx`:
- `now()` returns the time recorded in the batch's `task_done`.
- `random` is seeded with the run id.
- `side_effect(fn)` records fn's value as a marker and returns the record on replay.
- `spawn`, `gather` and `first` give concurrency; `first` abandons the losers (a pending signal wait gives up its place).
- `continue_as_new` starts a fresh run with an empty history, for loops that would grow forever. `drain_signals` collects signals that arrived but were not waited for, so none are lost across the restart.

Cancellation throws `Cancelled` into the main coroutine at whatever it is awaiting. The workflow can catch it and run compensations.

## 3. Changing code under running workflows

Deploying new workflow code while runs are in flight is the hard operational problem of this model. If the new code produces different commands for an existing history, replay detects it (`NonDeterminism`). The workflow task then fails without writing anything and is retried later. The run is not failed, so rolling back the deploy fixes it (`test_incompatible_change_stops_the_run_until_fixed`).

To change code on purpose, `ctx.patched("id")` branches:
- **Running new code for the first time:** it records a marker and returns True.
- **Replaying a history written before the patch:** the next recorded command is not that marker, so it returns False and the old path is taken.

A run that was in flight at the deploy takes the new path from the point it reaches next (`test_replay.py`).

## 4. The store (`store.py`)

SQLite in WAL mode, `synchronous=FULL`. Each state change is one `BEGIN IMMEDIATE` transaction, so a process killed at any point leaves either all of a change or none of it. The tables:
- `runs`: status, last event id, a needs-task flag, and the workflow task lease.
- `history`.
- `activities`: pending ones, with attempt number and lease.
- `timers`.

Work is handed out by lease: a worker takes a workflow task or an activity for N seconds, and a lease that runs out makes the work available again. **Leases are not what makes it correct.** A worker paused by GC can wake up after its lease expired and still try to report. Two checks at commit time make that harmless:
- **Workflow task.** It commits only if no event was appended since the worker read the history (`last_event` unchanged). Otherwise the code has not seen everything that the new `task_done` would claim it saw. The commit returns `conflict` and the task runs again.
- **Activity result.** It is accepted only with the current lease token. A result from an attempt that timed out and was retried is refused.

Without the first check, a signal that arrives while a workflow task runs gets filed into a batch the code never saw. Replay then diverges (section 7).

The workflow task's commands are carried out in the same transaction: activity rows inserted, timers inserted, children started, the run closed and the parent notified. If they were split from the history append, a crash between the two would leave a history saying "activity scheduled" with no activity row, and the run would wait forever (also caught, section 7).

Retries: a failed attempt is rescheduled with exponential backoff, unless the error type is listed as non-retryable or the attempts are used up. Then an `activity_failed` input goes to the workflow. A lease that runs out counts as a failed attempt (`Timeout`).

## 5. What "exactly once" means here

- **Workflow state transitions happen exactly once.** That follows from the two commit checks plus single transactions.
- **Activities run at least once.** A worker can die after the side effect and before reporting, and the activity then runs again. No engine can avoid this without the outside system's help.
- **Effects happen exactly once only if the outside system deduplicates.** Each activity gets `activity_context().idempotency_key`: the same for every attempt of that activity, different for everything else.

The simulator found a subtler problem one level up, in my first saga. A transfer withdrew from one account and deposited into another. If the deposit ran out of retries, it refunded the withdrawal. But "ran out of retries" does not mean "did not happen". The last attempt can apply the deposit and then lose the reply. In 6 of 1,000 seeds that version created money: the deposit went through *and* the source was refunded.

The fix is the try/cancel pattern. Every operation carries a business key, and compensation is `void(key)`. The bank reverses the operation if it was applied, and refuses it if it arrives later, from an attempt still in flight. After the change, 0 of 2,000 seeds fail.

## 6. Making it fast: eager dispatch and the sticky cache

**Eager dispatch.** Profiling one worker on a file database showed 70% of the time inside SQLite, mostly commits. A three-activity workflow took 14 transactions: a take and a report for each of 4 workflow tasks and 3 activities. Now two of those happen inside other transactions:
- The commit of a workflow task also leases (to the same worker) the activities it just scheduled.
- The commit of an activity result also leases the workflow task it made due.

That brings it to 8 transactions. A crash loses the eagerly leased work, which then waits for its lease to run out like any other. The simulator's crash fault drops it to test exactly that.

**Sticky cache.** Without it, every workflow task reads and replays the whole history, so a run of n steps costs O(n²). A 3,000-activity workflow took 31.6 s, of which one replay of the final 9,003 events is 9 ms (about 1 µs per event).

With the cache, a worker keeps the replayer of each run it committed a task for. On the next task it tells the store which event it has, reads only the later ones, and feeds them to the kept replayer (`Replayer.advance`). If someone else moved the run on meanwhile, the later events contain commands, and the worker falls back to a full replay. The 3,000-activity run now takes 0.18 s.

The cache hides non-deterministic code: code that is not re-run cannot diverge. Temporal has the same property. The simulator therefore finishes every run with a full replay from scratch, and compares it with what was recorded.

**A bug found while adding the cache.** `task_done` recorded the time at which the worker *reported*, but the code had run with the clock read before it ran. A replay then saw a different `ctx.now()` than the first execution. Nothing failed until `ctx.now()` went into a command: with the deadline time passed as an activity argument, all 200 of 200 approval seeds stopped with `NonDeterminism`. The fix is that the task carries the time the code saw, and that is what `task_done` records.

## 7. Deterministic simulation testing (`sim.py`, `scenarios.py`)

The approach comes from FoundationDB: run the whole system in one thread, with every source of non-determinism taken from one seeded random generator. That covers which worker moves, the clock, crashes and network failures. Then run thousands of seeds, and replay any failing one exactly.

Each worker is an `Actor` that moves one phase at a time: take a task, run it, report it. Between phases, the simulator chooses:
- **Step.** Move a random actor one phase.
- **Crash.** An actor drops its in-flight task and its local queues; whatever it held stays leased until the lease runs out.
- **Pause.** An actor stalls holding a task and comes back much later with a stale lease, as after a long GC pause or a network partition.
- **Cut a transaction.** A crash hook raises in the middle of a transaction, which then rolls back.
- **Jump the clock** by up to 30 s. Leases expire and timers fire.
- **Fail activity calls**, before or *after* their side effect: the effect happened, but the caller hears an error.
- **Deliver signals** at scheduled times.

After 1,500 such steps the faults stop. Every workflow must then finish (liveness).

The checks:
- **The scenario's own invariants.**
  - Transfers: the bank's balances equal what the workflow results imply, and money is conserved.
  - Approvals: an order is "shipped" if and only if its approval signal precedes the timer in the history, and its side effects happened exactly once.
  - Counters: every signal is counted once, across continue-as-new.
  - Fan-out: children's results add up.
- **Every history is well formed.**
  - Event ids are contiguous.
  - Every activity is scheduled at most once and has at most one result.
  - Every timer fires at most once.
  - Commands only follow `task_done`.
  - A closed run ends with its closing event.
- **Every run replays from scratch** to exactly the recorded commands.

**Testing the tests.** Each check should fail on the bug it is meant to catch. `sim.py --mutation` breaks the engine or the workload on purpose:
- `naive_saga` treats "retries ran out" as "did not happen".
- `no_idempotency`: the bank applies repeated requests twice.
- `skip_event_check` commits a workflow task even though events arrived during it.
- `split_commit` writes the history and the task tables in two transactions.

`tests/test_sim.py` asserts that each one is caught within a few hundred seeds. The README has the rates.

## 8. Real processes, real kill -9 (`chaos.py`)

The simulator models crashes; it does not exercise SQLite's own crash recovery or locking between processes. `chaos.py` runs real worker processes on a database file. The bank is a second SQLite file with a key table, standing in for the outside system. A random worker is killed with SIGKILL every fraction of a second and restarted, until every transfer has finished. Then the same balance and history checks run.

## 9. Limits, and what Temporal has that this does not

- **One SQLite file, one writer at a time.** Throughput stops growing at one worker process (README). Scaling out needs a database with row-level locking (Postgres with `SELECT ... FOR UPDATE SKIP LOCKED`) or sharding runs across stores, as Temporal shards by workflow id.
- **Not implemented:**
  - cancelling a running activity, and propagating cancellation to child workflows;
  - schedule-to-start timeouts;
  - search over workflows;
  - worker versioning by build id;
  - a UI.
- **Payloads are JSON.** Workflow and activity arguments must be JSON values.
- **Any exception escaping workflow code fails the workflow.** Temporal instead retries the workflow task for errors that are not explicit application failures, so that a bug can be fixed without failing the run.

## References

- H. Garcia-Molina, K. Salem. Sagas. SIGMOD 1987.
- J. Zhou et al. FoundationDB: A Distributed Unbundled Transactional Key Value Store. SIGMOD 2021. (Section on simulation testing.)
- Temporal documentation: workflows, event history, determinism, versioning. https://docs.temporal.io
