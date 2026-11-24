"""Workers: take workflow tasks and activities from the store, run them,
report back. Each kind of work is split into the same three phases (take,
run, report) so that the simulator can interleave and crash them."""

import asyncio
import contextvars
import inspect
import os
import threading
import time
import uuid
from collections import OrderedDict, deque

from .errors import error_info
from .runtime import INPUTS, Replayer, norm
from .store import Store

_current = contextvars.ContextVar("durable_activity")


def name_of(fn):
    return getattr(fn, "__durable_name__", fn.__name__)


def workflow(name=None):
    """Optional decorator to register a workflow under another name."""
    def deco(fn):
        fn.__durable_name__ = name or fn.__name__
        return fn
    return deco


activity = workflow


class ActivityContext:
    def __init__(self, task, worker):
        self.task = task
        self._worker = worker

    @property
    def idempotency_key(self):
        return self.task.idempotency_key

    @property
    def attempt(self):
        return self.task.attempt

    def heartbeat(self):
        """Extends the lease. Returns False if this attempt no longer counts
        (it should stop)."""
        return self._worker.store.heartbeat(self.task, self._worker.clock())


def activity_context():
    """Inside an activity: its idempotency key, attempt number, heartbeat."""
    return _current.get()


class Worker:
    def __init__(self, store, workflows=(), activities=(), clock=time.time, owner=None, queues=("default",),
                 lease=30.0, task_retry=5.0, eager=True, sticky=True, cache_size=1000):
        self.store = store
        self.workflows = {name_of(f): f for f in workflows}
        self.activities = {name_of(f): f for f in activities}
        self.clock = clock
        self.owner = owner or f"{os.uname().nodename}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self.queues = tuple(queues)
        self.lease = lease
        self.task_retry = task_retry
        # eager dispatch: work this worker leased for itself while reporting
        # other work, in the same transaction (saves a take per step)
        self.eager = eager
        self.local_wf, self.local_act = deque(), deque()
        # sticky execution: replayers kept after a committed task, so the next
        # task of the same run reads and runs only the new events
        self.sticky, self.cache_size = sticky, cache_size
        self.cache = OrderedDict()  # run_id -> (replayer, last event id it has seen)
        self.stats = {"workflow_tasks": 0, "conflicts": 0, "lost": 0, "task_failures": 0, "activities": 0,
                      "activity_rejected": 0, "eager": 0, "cache_hits": 0}

    # ---- workflow tasks

    def take_workflow_task(self):
        if self.local_wf:
            try:
                return self.local_wf.popleft()
            except IndexError:  # another thread got it first
                pass
        return self.store.acquire_workflow_task(self.owner, self.clock(), self.lease, self._known())

    def _known(self):
        return {r: upto for r, (_, upto) in list(self.cache.items())} if self.sticky else None

    def run_workflow_task(self, task):
        """Runs the workflow code; returns (kind, commands or error text, the
        time the code saw, the replayer)."""
        t = self.clock()
        fn = self.workflows.get(task.workflow)
        if fn is None:
            return "error", f"no workflow named {task.workflow} on this worker", t, None
        cached = self.cache.pop(task.run_id, None)
        try:
            if task.first > 1:
                if cached and cached[1] == task.first - 1 and all(e["type"] in INPUTS for e in task.history):
                    self.stats["cache_hits"] += 1
                    return "ok", cached[0].advance(task.history, t), t, cached[0]
                # someone else moved the run on meanwhile: read it all
                task.history = self.store.history(task.run_id)[:task.last_event]
            r = Replayer(fn, task.workflow_id, task.run_id, task.history)
            return "ok", r.run(t), t, r
        except Exception as e:
            return "error", f"{type(e).__name__}: {e}", t, None

    def report_workflow_task(self, task, outcome):
        kind, val, t, rep = outcome
        now = self.clock()
        if kind == "error":
            self.stats["task_failures"] += 1
            self.store.fail_workflow_task(task, val, now, self.task_retry)
            return "failed"
        res, acts = self.store.complete_workflow_task(task, val, now, eager=4 if self.eager else 0,
                                                      queues=self.queues, task_time=t)
        if res == "ok" and self.sticky and rep is not None and not rep.closed:
            self.cache[task.run_id] = (rep, task.last_event + 1 + len(val))
            while len(self.cache) > self.cache_size:
                self.cache.popitem(last=False)
        self.local_act.extend(acts)
        self.stats["eager"] += len(acts)
        self.stats["workflow_tasks"] += res == "ok"
        self.stats["conflicts"] += res == "conflict"
        self.stats["lost"] += res == "lost"
        return res

    def workflow_step(self):
        task = self.take_workflow_task()
        if task is None:
            return False
        self.report_workflow_task(task, self.run_workflow_task(task))
        return True

    # ---- activities

    def take_activity(self):
        if self.local_act:
            try:
                return self.local_act.popleft()
            except IndexError:
                pass
        return self.store.acquire_activity(self.owner, self.clock(), self.queues)

    def run_activity(self, task):
        """Runs the user function; returns ("ok", result) or ("error", info)."""
        fn = self.activities.get(task.name)
        if fn is None:
            return "error", {"type": "NotRegistered", "message": f"no activity named {task.name} on this worker"}
        token = _current.set(ActivityContext(task, self))
        try:
            out = fn(*task.args)
            if inspect.isawaitable(out):
                out = asyncio.run(out)
            return "ok", norm(out)
        except Exception as e:
            return "error", error_info(e)
        finally:
            _current.reset(token)

    def report_activity(self, task, outcome):
        kind, val = outcome
        now = self.clock()
        if kind == "ok":
            ok, wt = self.store.complete_activity(task, val, now, sticky=self.eager, lease=self.lease,
                                                  known=self._known())
            if wt is not None:
                self.local_wf.append(wt)
                self.stats["eager"] += 1
        else:
            ok = self.store.fail_activity(task, val, now)
        self.stats["activities"] += 1
        self.stats["activity_rejected"] += not ok
        return ok

    def activity_step(self):
        task = self.take_activity()
        if task is None:
            return False
        self.report_activity(task, self.run_activity(task))
        return True

    def timer_step(self):
        return self.store.fire_timers(self.clock()) > 0

    # ---- a real process

    def run(self, stop=None, activity_threads=4, poll=0.02):
        """Polls until `stop` (a threading.Event) is set: one thread for
        workflow tasks and timers, `activity_threads` for activities, each
        with its own database connection."""
        stop = stop or threading.Event()

        def loop(step_names):
            w = self if self.store.path == ":memory:" else self._clone()
            steps = [getattr(w, n) for n in step_names]
            while not stop.is_set():
                busy = False
                for s in steps:
                    busy = s() or busy
                if not busy:
                    stop.wait(poll)

        threads = [threading.Thread(target=loop, args=(["timer_step", "workflow_step"],), daemon=True)]
        threads += [threading.Thread(target=loop, args=(["activity_step"],), daemon=True) for _ in range(activity_threads)]
        for t in threads:
            t.start()
        return threads

    def _clone(self):
        w = Worker(Store(self.store.path, self.store.synchronous), clock=self.clock, owner=self.owner,
                   queues=self.queues, lease=self.lease, task_retry=self.task_retry, eager=self.eager,
                   sticky=self.sticky, cache_size=self.cache_size)
        w.workflows, w.activities, w.stats = self.workflows, self.activities, self.stats
        w.local_wf, w.local_act, w.cache = self.local_wf, self.local_act, self.cache
        return w

    def shutdown(self):
        """Hands back eagerly leased work that has not started."""
        for q in (self.local_wf, self.local_act):
            while q:
                self.store.release(q.popleft())

    def drop_local(self):
        """Forgets eagerly leased work and cached replayers (what a crash does)."""
        self.local_wf.clear()
        self.local_act.clear()
        self.cache.clear()
