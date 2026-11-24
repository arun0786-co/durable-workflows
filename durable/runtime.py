"""Runs workflow code deterministically by replaying its history.

A workflow is an `async def` that only awaits durable operations: ctx.activity,
ctx.sleep, ctx.wait_signal, ctx.child, and tasks built from those. Its code
is re-run from the start every time something happens. The history says what
already happened, so on replay:

- every operation the code starts (a *command*) is checked against the one
  recorded at that point, and gets the recorded result instead of running
  again;
- the results arrive in the same batches and the same order as the first
  time, so the code takes the same path;
- once the history runs out, new commands are collected and returned, for
  the store to append to the history in one transaction.

History events are dicts. Inputs come from outside the workflow (started,
activity_done, timer_fired, signal...). `task_done` closes a batch of inputs:
it is written by the workflow task that processed them, followed by the
commands that task produced.
"""

import json
import random
import uuid
from collections import deque

from .errors import ActivityError, Cancelled, ChildError, ContinueAsNew, NonDeterminism, error_info

INPUTS = {"started", "activity_done", "activity_failed", "timer_fired", "signal", "child_done", "child_failed",
          "cancel_requested"}
COMMANDS = {"activity_scheduled", "timer_started", "child_started", "marker", "completed", "failed", "continued",
            "cancelled"}
CLOSING = {"completed", "failed", "continued", "cancelled"}


def norm(x):
    """What a value looks like after a trip through the history (JSON)."""
    return json.loads(json.dumps(x))


class Future:
    __slots__ = ("done", "value", "error", "callbacks", "cancelled")

    def __init__(self):
        self.done = False
        self.value = None
        self.error = None
        self.callbacks = []
        self.cancelled = False

    def resolve(self, value=None):
        if not self.done:
            self.done, self.value = True, value
            self._wake()

    def fail(self, error):
        if not self.done:
            self.done, self.error = True, error
            self._wake()

    def _wake(self):
        cbs, self.callbacks = self.callbacks, []
        for cb in cbs:
            cb(self)

    def __await__(self):
        if not self.done:
            yield self
        if self.error is not None:
            raise self.error
        return self.value


class Task:
    __slots__ = ("coro", "future", "waiting", "pending")

    def __init__(self, coro):
        self.coro = coro
        self.future = Future()
        self.waiting = None  # the future this task is parked on
        self.pending = None  # an exception to throw in at the next resume


class Scheduler:
    """A FIFO run queue: the same inputs always give the same interleaving."""

    def __init__(self):
        self.ready = deque()

    def spawn(self, coro):
        t = Task(coro)
        self.ready.append(t)
        return t

    def throw(self, task, exc):
        if task.future.done:
            return
        task.pending = exc
        if task.waiting is not None:
            task.waiting = None
            self.ready.append(task)

    def _wake(self, task, fut):
        if task.waiting is fut:
            task.waiting = None
            self.ready.append(task)

    def run(self):
        while self.ready:
            task = self.ready.popleft()
            if task.future.done:
                continue
            exc, task.pending = task.pending, None
            try:
                y = task.coro.throw(exc) if exc is not None else task.coro.send(None)
            except StopIteration as e:
                task.future.resolve(e.value)
                continue
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:  # includes ContinueAsNew
                task.future.fail(e)
                continue
            if not isinstance(y, Future):
                task.pending = TypeError(
                    f"workflow code awaited {y!r}; only durable operations (ctx.activity, ctx.sleep, ...) can be awaited")
                self.ready.append(task)
            elif y.done:
                self.ready.append(task)
            else:
                task.waiting = y
                y.callbacks.append(lambda f, t=task: self._wake(t, f))


def _same(cmd, rec):
    if cmd["type"] != rec["type"]:
        return False
    t = cmd["type"]
    if t == "activity_scheduled":
        return (cmd["seq"], cmd["name"], cmd["args"]) == (rec["seq"], rec["name"], rec["args"])
    if t == "timer_started":
        return cmd["seq"] == rec["seq"]
    if t == "child_started":
        return (cmd["seq"], cmd["workflow"], cmd["input"]) == (rec["seq"], rec["workflow"], rec["input"])
    if t == "marker":
        return (cmd["seq"], cmd["kind"], cmd.get("patch")) == (rec["seq"], rec["kind"], rec.get("patch"))
    if t == "completed":
        return cmd["result"] == rec["result"]
    if t == "continued":
        return cmd["input"] == rec["input"]
    return True


def describe(ev):
    t = ev["type"]
    if t == "activity_scheduled":
        return f"activity {ev['name']}{tuple(ev['args'])} (seq {ev['seq']})"
    if t == "timer_started":
        return f"timer (seq {ev['seq']})"
    if t == "child_started":
        return f"child workflow {ev['workflow']} (seq {ev['seq']})"
    if t == "marker":
        return f"{ev['kind']} marker {ev.get('patch') or ''} (seq {ev['seq']})".replace("  ", " ")
    return t


class Replayer:
    def __init__(self, fn, workflow_id, run_id, history):
        self.fn = fn
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.history = history
        self.sched = Scheduler()
        self.ctx = Context(self)
        self.seq = 0
        self.futures = {}
        self.signals = {}
        self.signal_waiters = {}
        self.expected = None  # recorded commands still to match in this batch; None once past the history
        self.new = []
        self.now = 0.0
        self.main = None
        self.closed = None
        self.fatal = None
        self.cancel_requested = False
        self.queries = {}

    # ---- driving

    def run(self, now):
        """Replays the history, then processes the inputs that no workflow
        task has seen yet. Returns the commands to append after a new
        task_done(now)."""
        h, i, inputs = self.history, 0, []
        while i < len(h):
            ev = h[i]
            if ev["type"] == "task_done":
                j = i + 1
                while j < len(h) and h[j]["type"] in COMMANDS:
                    j += 1
                self._batch(inputs, ev["time"], deque(h[i + 1:j]))
                inputs, i = [], j
            else:
                inputs.append(ev)
                i += 1
        self._batch(inputs, now, None)
        return self.new

    def _batch(self, inputs, now, expected):
        self.now, self.expected = now, expected
        for ev in inputs:
            self._apply(ev)
        self.sched.run()
        if self.fatal:
            raise self.fatal
        if expected:
            raise NonDeterminism(f"the history has {describe(expected[0])}, which the workflow code no longer produces")

    def _apply(self, ev):
        if self.closed:
            return
        t = ev["type"]
        if t == "started":
            self.main = self.sched.spawn(self.fn(self.ctx, *ev["input"]))
            self.main.future.callbacks.append(self._main_done)
        elif t == "activity_done" or t == "child_done":
            self.futures[ev["seq"]].resolve(ev["result"])
        elif t == "activity_failed":
            self.futures[ev["seq"]].fail(ActivityError(ev["error"]["type"], ev["error"]["message"]))
        elif t == "child_failed":
            self.futures[ev["seq"]].fail(ChildError(ev["error"]["type"], ev["error"]["message"]))
        elif t == "timer_fired":
            self.futures[ev["seq"]].resolve(None)
        elif t == "signal":
            waiters = self.signal_waiters.get(ev["name"])
            while waiters and waiters[0].cancelled:
                waiters.popleft()
            if waiters:
                waiters.popleft().resolve(ev["payload"])
            else:
                self.signals.setdefault(ev["name"], deque()).append(ev["payload"])
        elif t == "cancel_requested":
            self.cancel_requested = True
            if self.main and not self.main.future.done:
                self.sched.throw(self.main, Cancelled("cancellation requested"))
        else:
            raise ValueError(f"unknown event {t}")

    def _main_done(self, fut):
        e = fut.error
        if e is None:
            cmd = {"type": "completed", "result": norm(fut.value)}
        elif isinstance(e, NonDeterminism):
            return
        elif isinstance(e, ContinueAsNew):
            cmd = {"type": "continued", "input": norm(e.args_)}
        elif isinstance(e, Cancelled) and self.cancel_requested:
            cmd = {"type": "cancelled"}
        else:
            cmd = {"type": "failed", "error": error_info(e)}
        self.command(cmd)
        self.closed = cmd

    # ---- commands

    def next_seq(self):
        self.seq += 1
        return self.seq

    def command(self, cmd):
        """Records a new command, or checks it against the history while
        replaying. Returns the recorded version."""
        if self.fatal:
            raise self.fatal
        if self.closed:
            return None  # the workflow has finished; leftover tasks change nothing
        if self.expected is None:
            self.new.append(cmd)
            return cmd
        if not self.expected:
            self.fatal = NonDeterminism(f"the workflow code produced {describe(cmd)} where the history has none")
            raise self.fatal
        rec = self.expected.popleft()
        if not _same(cmd, rec):
            self.fatal = NonDeterminism(f"the history has {describe(rec)}, the workflow code produced {describe(cmd)}")
            raise self.fatal
        return rec

    def peek_patch(self, patch_id):
        """While replaying: is the next recorded command this patch marker?"""
        e = self.expected
        return bool(e) and e[0]["type"] == "marker" and e[0]["kind"] == "patch" and e[0].get("patch") == patch_id


class Context:
    """The `ctx` argument of a workflow: the only way it may touch the world."""

    def __init__(self, r):
        self._r = r
        self._rng = None
        self._patches = {}

    @property
    def workflow_id(self):
        return self._r.workflow_id

    @property
    def run_id(self):
        return self._r.run_id

    @property
    def replaying(self):
        return self._r.expected is not None

    @property
    def cancel_requested(self):
        return self._r.cancel_requested

    def now(self):
        """The time of the workflow task this code runs in: the same on every
        replay."""
        return self._r.now

    @property
    def random(self):
        """A random.Random seeded from the run id."""
        if self._rng is None:
            self._rng = random.Random(self._r.run_id)
        return self._rng

    def uuid4(self):
        return str(uuid.UUID(int=self.random.getrandbits(128), version=4))

    def activity(self, fn, *args, retry=None, timeout=None, queue=None):
        """Schedules an activity and returns a future for its result.
        retry: a RetryPolicy; timeout: seconds per attempt (start to close)."""
        r = self._r
        seq = r.next_seq()
        name = fn if isinstance(fn, str) else getattr(fn, "__durable_name__", fn.__name__)
        opts = {}
        if retry is not None:
            opts["retry"] = retry.to_dict()
        if timeout is not None:
            opts["timeout"] = timeout
        if queue is not None:
            opts["queue"] = queue
        r.command({"type": "activity_scheduled", "seq": seq, "name": name, "args": norm(list(args)), "options": opts})
        return r.futures.setdefault(seq, Future())

    def sleep(self, seconds):
        """A durable timer: survives restarts, fires seconds after now()."""
        r = self._r
        seq = r.next_seq()
        r.command({"type": "timer_started", "seq": seq, "fire_at": r.now + seconds})
        return r.futures.setdefault(seq, Future())

    def child(self, workflow, *args, id=None):
        """Starts a child workflow; the future gets its result."""
        r = self._r
        seq = r.next_seq()
        name = workflow if isinstance(workflow, str) else getattr(workflow, "__durable_name__", workflow.__name__)
        # the default id uses the run id: seq numbers restart after continue_as_new
        r.command({"type": "child_started", "seq": seq, "workflow": name, "input": norm(list(args)),
                   "child_id": id or f"{r.run_id}/{seq}"})
        return r.futures.setdefault(seq, Future())

    def wait_signal(self, name):
        """The next signal `name` sent to this workflow, in arrival order."""
        r = self._r
        f = Future()
        buf = r.signals.get(name)
        if buf:
            f.resolve(buf.popleft())
        else:
            r.signal_waiters.setdefault(name, deque()).append(f)
        return f

    def drain_signals(self, name):
        """The signals `name` that arrived and nobody waited for yet, without
        waiting. Call before continue_as_new so none are lost."""
        buf = self._r.signals.pop(name, None)
        return list(buf) if buf else []

    def side_effect(self, fn):
        """Runs fn once and records its value; replays return the record."""
        r = self._r
        seq = r.next_seq()
        if r.expected is not None:
            return r.command({"type": "marker", "seq": seq, "kind": "side_effect"})["value"]
        return r.command({"type": "marker", "seq": seq, "kind": "side_effect", "value": norm(fn())})["value"]

    def patched(self, patch_id):
        """For changing workflow code while old runs are in flight: True in
        runs that reach this point with the new code, False when replaying a
        history written by the old code.

            if ctx.patched("fraud-check"):
                await ctx.activity(fraud_check, order)
        """
        if patch_id in self._patches:
            return self._patches[patch_id]
        r = self._r
        if r.expected is not None and not r.peek_patch(patch_id):
            self._patches[patch_id] = False
            return False
        r.command({"type": "marker", "seq": r.next_seq(), "kind": "patch", "patch": patch_id})
        self._patches[patch_id] = True
        return True

    def set_query(self, name, fn):
        """Registers fn(*args) to answer client.query(workflow_id, name)."""
        self._r.queries[name] = fn

    def spawn(self, coro):
        """Runs a coroutine concurrently; returns a future for its result."""
        return self._r.sched.spawn(coro).future

    async def gather(self, *aws):
        futs = [self.spawn(a) if not isinstance(a, Future) else a for a in aws]
        return [await f for f in futs]

    def first(self, *aws):
        """A future for (index, value) of whichever finishes first. The
        others are abandoned (a pending wait_signal gives up its place)."""
        futs = [self.spawn(a) if not isinstance(a, Future) else a for a in aws]
        out = Future()

        def done(i, f):
            if out.done:
                return
            for j, g in enumerate(futs):
                if j != i:
                    g.cancelled = True
            if f.error is not None:
                out.fail(f.error)
            else:
                out.resolve((i, f.value))

        for i, f in enumerate(futs):
            if f.done:
                done(i, f)
                break
        else:
            for i, f in enumerate(futs):
                f.callbacks.append(lambda g, i=i: done(i, g))
        return out

    def continue_as_new(self, *args):
        """Ends this run and starts a new one with these arguments and an
        empty history (for workflows that would otherwise grow forever)."""
        raise ContinueAsNew(args)
