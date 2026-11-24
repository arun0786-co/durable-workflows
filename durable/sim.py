"""Deterministic simulation testing.

One seed drives everything: which worker moves next, when the clock jumps,
which worker dies between taking a task and reporting it, which transaction
is cut off half way, which activity call fails before or after its side
effect, when signals arrive. The whole engine runs in one thread on an
in-memory database, so a failing seed replays exactly.

    python -m durable.sim --seeds 2000
    python -m durable.sim --seeds 200 --mutation split_commit   # a deliberately broken engine

Each run has a fault phase, then a healing phase without faults in which
every workflow must finish. Then the checks: the scenario's own invariants
(money conserved...), and for every run of every workflow, a well-formed
history that replays to exactly the same commands.
"""

import argparse
import random
import time
from collections import Counter

from .client import Client
from .runtime import COMMANDS, Replayer
from .scenarios import SCENARIOS, Flaky
from .store import Crash, NotFound, Store
from .worker import Worker, name_of


class SimClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class Actor:
    """One worker process, moved one phase at a time."""

    def __init__(self, worker):
        self.w = worker
        self.job = None  # [kind, task, outcome]

    def step(self, rng):
        w = self.w
        if self.job is None:
            r = rng.random()
            if r < 0.4:
                t = w.take_workflow_task()
                self.job = t and ["wf", t, None]
            elif r < 0.85:
                t = w.take_activity()
                self.job = t and ["act", t, None]
            else:
                return w.timer_step()
            return self.job is not None
        kind, task, outcome = self.job
        if outcome is None:
            self.job[2] = w.run_workflow_task(task) if kind == "wf" else w.run_activity(task)
        else:
            self.job = None
            if kind == "wf":
                w.report_workflow_task(task, outcome)
            else:
                w.report_activity(task, outcome)
        return True


class Faults:
    def __init__(self, crash=0.01, tx_crash=0.01, pause=0.01, jump=0.03, fail_before=0.05, fail_after=0.05):
        self.crash, self.tx_crash, self.pause, self.jump = crash, tx_crash, pause, jump
        self.fail_before, self.fail_after = fail_before, fail_after


def history_problems(wid, run, hist, fn):
    """Checks one run's history: structure, then a full replay."""
    bad = []
    types = [e["type"] for e in hist]
    if [e["id"] for e in hist] != list(range(1, len(hist) + 1)):
        bad.append(f"{wid}: event ids not contiguous")
    sched = Counter(e["seq"] for e in hist if e["type"] == "activity_scheduled")
    results = Counter(e["seq"] for e in hist if e["type"] in ("activity_done", "activity_failed"))
    if any(n > 1 for n in sched.values()):
        bad.append(f"{wid}: an activity was scheduled twice")
    if any(n > 1 for n in results.values()):
        bad.append(f"{wid}: an activity has two results")
    if set(results) - set(sched):
        bad.append(f"{wid}: a result for an activity that was never scheduled")
    timers = Counter(e["seq"] for e in hist if e["type"] == "timer_fired")
    if any(n > 1 for n in timers.values()):
        bad.append(f"{wid}: a timer fired twice")
    for i, t in enumerate(types):
        if t in COMMANDS and (i == 0 or types[i - 1] not in COMMANDS | {"task_done"}):
            bad.append(f"{wid}: command {t} at event {i + 1} not right after a task_done")
            break
    closing = [i for i, t in enumerate(types) if t in ("completed", "failed", "continued", "cancelled")]
    if run["status"] != "running" and closing != [len(types) - 1]:
        bad.append(f"{wid}: closing event missing, repeated or not last")
    try:
        r = Replayer(fn, wid, run["run_id"], hist)
        extra = r.run(hist[-1].get("time", 0.0))
        if run["status"] != "running" and extra:
            bad.append(f"{wid}: replaying the closed run produced new commands {extra}")
    except Exception as e:
        bad.append(f"{wid}: replay failed: {type(e).__name__}: {e}")
    return bad


def simulate(seed, scenario="transfers", workers=3, fault_steps=1500, faults=None, mutation=None, heal_steps=20000):
    rng = random.Random(seed)
    faults = faults or Faults()
    clock = SimClock()
    store = Store()
    ids = iter(range(10**9))
    store.new_id = lambda: f"run{next(ids)}"
    if mutation in ("skip_event_check", "split_commit"):
        store.unsafe.add(mutation)
    flaky = Flaky(rng, faults.fail_before, faults.fail_after)
    kw = {}
    if scenario == "transfers":
        kw = {"idempotent": mutation != "no_idempotency", "naive": mutation == "naive_saga"}
    sc = SCENARIOS[scenario](rng, flaky, **kw)
    acts = sc.activities()
    actors = [Actor(Worker(store, sc.workflows, acts, clock=clock, owner=f"w{i}", lease=10, task_retry=2))
              for i in range(workers)]
    client = Client(store, clock)
    sc.start(client)
    signals = list(sc.signals)

    in_faults = [True]

    def hook(point):
        if in_faults[0] and rng.random() < faults.tx_crash:
            raise Crash(point)
    store.crash_hook = hook

    def deliver(upto):
        while signals and signals[0][0] <= upto:
            _, wid, name, payload = signals.pop(0)
            try:
                client.signal(wid, name, payload)
            except NotFound:
                pass  # it finished first

    paused = []
    for _ in range(fault_steps):
        r = rng.random()
        deliver(clock.t)
        if r < faults.jump:
            clock.t += rng.choice([rng.uniform(0, 3), rng.uniform(5, 30)])
        elif r < faults.jump + faults.crash and actors:
            a = rng.choice(actors)  # dies: whatever it held stays leased until the lease runs out
            a.job = None
            a.w.drop_local()
        elif r < faults.jump + faults.crash + faults.pause and actors:
            a = rng.choice(actors)  # stalls (a long GC pause) and comes back later with a stale lease
            if a in actors and a.job:
                actors.remove(a)
                paused.append(a)
        else:
            if paused and (rng.random() < 0.05 or not actors):
                actors.append(paused.pop(0))
            a = rng.choice(actors)
            try:
                a.step(rng)
            except Crash:
                a.job = None
                a.w.drop_local()
            clock.t += rng.uniform(0, 0.2)

    in_faults[0] = False
    flaky.on = False
    actors += paused
    for a in actors:  # whatever is in flight reports now; stale reports are refused
        while a.job:
            a.step(rng)
    for _ in range(heal_steps):
        deliver(clock.t)
        w = rng.choice(actors).w
        if w.timer_step() | w.workflow_step() | w.activity_step():
            continue
        if not signals and not store.runs("running"):
            break
        nxt = [d for d in (store.next_due(), signals[0][0] if signals else None) if d is not None]
        if not nxt:
            break
        clock.t = max(clock.t + 0.01, min(nxt))

    problems = []
    still = store.runs("running")
    for run in still:
        problems.append(f"{run['workflow_id']}: never finished (task error: {run['task_error']})")
    fns = {name_of(f): f for f in sc.workflows}
    histories = {}
    for run in store.runs():
        hist = store.history(run["run_id"])
        histories.setdefault(run["workflow_id"], []).append(hist)
        problems += history_problems(run["workflow_id"], run, hist, fns[run["workflow"]])
    if not still:
        problems += sc.check(client, histories)
    stats = Counter()
    for a in actors:
        stats.update(a.w.stats)
    return problems, stats, clock.t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=200)
    ap.add_argument("--first", type=int, default=0)
    ap.add_argument("--scenario", default="all")
    ap.add_argument("--mutation", default=None, choices=[None, "naive_saga", "no_idempotency", "skip_event_check", "split_commit"])
    a = ap.parse_args()
    names = list(SCENARIOS) if a.scenario == "all" else [a.scenario]
    for name in names:
        t0 = time.perf_counter()
        failing, totals = [], Counter()
        for seed in range(a.first, a.first + a.seeds):
            problems, stats, _ = simulate(seed, name, mutation=a.mutation)
            totals.update(stats)
            if problems:
                failing.append((seed, problems))
        secs = time.perf_counter() - t0
        print(f"{name}: {a.seeds} seeds, {len(failing)} failing, {secs:.1f} s; "
              + ", ".join(f"{k} {v}" for k, v in sorted(totals.items())))
        for seed, problems in failing[:3]:
            print(f"  seed {seed}: {problems[0]}" + (f" (+{len(problems) - 1} more)" if len(problems) > 1 else ""))


if __name__ == "__main__":
    main()
