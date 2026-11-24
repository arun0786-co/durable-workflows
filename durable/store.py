"""Durable state in SQLite: runs, their event histories, activity tasks and
timers. Every state change is one transaction, so a process that dies at any
point leaves either all of a change or none of it.

Workers take work by lease: a workflow task or an activity is handed to one
worker until its lease runs out. Leases only avoid wasted work. Safety comes
from two checks at commit time:

- a workflow task commits only if no event was appended to the run since the
  worker read its history (otherwise the code has not seen everything that
  the new task_done marker would claim it saw);
- an activity result is accepted only from the attempt that holds the
  current lease token.
"""

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass

from .policy import RetryPolicy

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    workflow TEXT NOT NULL,
    status TEXT NOT NULL,          -- running, completed, failed, cancelled, continued
    result TEXT,                   -- json: the value, or the error
    parent TEXT,                   -- json {run_id, seq} for child workflows
    last_event INTEGER NOT NULL,
    needs_task INTEGER NOT NULL,   -- events arrived that the workflow code has not seen
    task_after REAL NOT NULL,      -- backoff after a failed workflow task
    lease_owner TEXT,
    lease_until REAL,
    lease_token INTEGER NOT NULL DEFAULT 0,
    task_error TEXT,
    created REAL NOT NULL,
    closed REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_run ON runs(workflow_id) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS runs_by_id ON runs(workflow_id, created);
CREATE INDEX IF NOT EXISTS runs_due ON runs(task_after) WHERE needs_task = 1;
CREATE TABLE IF NOT EXISTS history (
    run_id TEXT NOT NULL,
    id INTEGER NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (run_id, id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS activities (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    workflow_id TEXT NOT NULL,
    name TEXT NOT NULL,
    args TEXT NOT NULL,
    queue TEXT NOT NULL,
    retry TEXT,
    timeout REAL NOT NULL,
    attempt INTEGER NOT NULL,
    available_at REAL NOT NULL,
    lease_owner TEXT,
    lease_until REAL,
    lease_token INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS activities_due ON activities(queue, available_at);
CREATE TABLE IF NOT EXISTS timers (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    fire_at REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS timers_due ON timers(fire_at);
"""

DEFAULT_ACTIVITY_TIMEOUT = 60.0


class Crash(BaseException):
    """Raised by a crash hook to simulate the process dying at that point."""


@dataclass
class WorkflowTask:
    run_id: str
    workflow_id: str
    workflow: str
    owner: str
    token: int
    last_event: int
    history: list


@dataclass
class ActivityTask:
    run_id: str
    workflow_id: str
    seq: int
    name: str
    args: list
    attempt: int
    owner: str
    token: int
    timeout: float

    @property
    def idempotency_key(self):
        """The same for every attempt of this activity, different for every
        other activity: what an external system should deduplicate on."""
        return f"{self.workflow_id}/{self.run_id}/{self.seq}"


class WorkflowExists(Exception):
    pass


class NotFound(Exception):
    pass


class Store:
    def __init__(self, path=":memory:", synchronous="FULL"):
        self.path = path
        self.synchronous = synchronous
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=60)
        self.db.row_factory = sqlite3.Row
        if path != ":memory:":
            self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(f"PRAGMA synchronous={synchronous}")
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()  # one transaction at a time per connection
        self.unsafe = set()  # deliberately broken behaviour, for testing the tests
        self.crash_hook = None  # called with a point name; may raise Crash
        self.new_id = lambda: uuid.uuid4().hex  # the simulator makes these deterministic

    def _point(self, name):
        if self.crash_hook:
            self.crash_hook(name)

    @contextmanager
    def tx(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            self.db.execute("COMMIT")

    # ---- history

    def _read(self, q, args=()):
        with self.lock:
            return self.db.execute(q, args).fetchall()

    def history(self, run_id):
        rows = self._read("SELECT id, data FROM history WHERE run_id = ? ORDER BY id", (run_id,))
        return [{"id": r["id"], **json.loads(r["data"])} for r in rows]

    def _append(self, db, run_id, events):
        last = db.execute("SELECT last_event FROM runs WHERE run_id = ?", (run_id,)).fetchone()["last_event"]
        for ev in events:
            last += 1
            db.execute("INSERT INTO history (run_id, id, data) VALUES (?, ?, ?)", (run_id, last, json.dumps(ev)))
        db.execute("UPDATE runs SET last_event = ? WHERE run_id = ?", (last, run_id))
        return last

    def _input(self, db, run_id, ev):
        """Appends an event from outside the workflow and flags the run for a
        workflow task. Dropped if the run has closed."""
        row = db.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None or row["status"] != "running":
            return False
        self._append(db, run_id, [ev])
        db.execute("UPDATE runs SET needs_task = 1 WHERE run_id = ?", (run_id,))
        return True

    # ---- runs

    def _new_run(self, db, workflow, workflow_id, args, now, parent=None):
        run_id = self.new_id()
        db.execute(
            "INSERT INTO runs (run_id, workflow_id, workflow, status, parent, last_event, needs_task, task_after, created)"
            " VALUES (?, ?, ?, 'running', ?, 0, 1, ?, ?)",
            (run_id, workflow_id, workflow, json.dumps(parent) if parent else None, now, now))
        self._append(db, run_id, [{"type": "started", "workflow": workflow, "input": args}])
        return run_id

    def start(self, workflow, args, workflow_id, now, if_exists="return"):
        """Starts a run. Starting an id that already exists returns its
        latest run (so a client may retry a start safely), or raises
        WorkflowExists with if_exists="error"."""
        args = json.loads(json.dumps(list(args)))
        with self.tx() as db:
            row = db.execute("SELECT run_id FROM runs WHERE workflow_id = ? ORDER BY created DESC, rowid DESC LIMIT 1",
                             (workflow_id,)).fetchone()
            if row:
                if if_exists == "error":
                    raise WorkflowExists(workflow_id)
                return row["run_id"]
            return self._new_run(db, workflow, workflow_id, args, now)

    def _open_run(self, db, workflow_id):
        row = db.execute("SELECT run_id FROM runs WHERE workflow_id = ? AND status = 'running'", (workflow_id,)).fetchone()
        if row is None:
            raise NotFound(f"no running workflow {workflow_id}")
        return row["run_id"]

    def signal(self, workflow_id, name, payload, now):
        with self.tx() as db:
            self._input(db, self._open_run(db, workflow_id), {"type": "signal", "name": name, "payload": payload})

    def cancel(self, workflow_id, now):
        with self.tx() as db:
            self._input(db, self._open_run(db, workflow_id), {"type": "cancel_requested"})

    def describe(self, workflow_id):
        rows = self._read("SELECT * FROM runs WHERE workflow_id = ? ORDER BY created DESC, rowid DESC LIMIT 1",
                          (workflow_id,))
        if not rows:
            raise NotFound(workflow_id)
        d = dict(rows[0])
        d["result"] = json.loads(d["result"]) if d["result"] else None
        return d

    def runs(self, status=None):
        q = "SELECT * FROM runs" + (" WHERE status = ?" if status else "") + " ORDER BY created, rowid"
        return [dict(r) for r in self._read(q, (status,) if status else ())]

    def _close(self, db, run_id, status, result, now):
        db.execute("UPDATE runs SET status = ?, result = ?, needs_task = 0, closed = ? WHERE run_id = ?",
                   (status, json.dumps(result), now, run_id))
        db.execute("DELETE FROM activities WHERE run_id = ?", (run_id,))
        db.execute("DELETE FROM timers WHERE run_id = ?", (run_id,))
        parent = db.execute("SELECT parent FROM runs WHERE run_id = ?", (run_id,)).fetchone()["parent"]
        if parent and status != "continued":
            p = json.loads(parent)
            if status == "completed":
                ev = {"type": "child_done", "seq": p["seq"], "result": result}
            else:
                ev = {"type": "child_failed", "seq": p["seq"],
                      "error": result if status == "failed" else {"type": "Cancelled", "message": "child cancelled"}}
            self._input(db, p["run_id"], ev)

    # ---- workflow tasks

    def acquire_workflow_task(self, owner, now, lease=30.0):
        with self.tx() as db:
            row = db.execute(
                "SELECT run_id, workflow_id, workflow FROM runs WHERE needs_task = 1 AND task_after <= ?"
                " AND (lease_until IS NULL OR lease_until <= ?) ORDER BY task_after LIMIT 1", (now, now)).fetchone()
            if row is None:
                return None
            return self._lease_workflow_task(db, row, owner, now, lease)

    def _lease_workflow_task(self, db, row, owner, now, lease):
        db.execute("UPDATE runs SET lease_owner = ?, lease_until = ?, lease_token = lease_token + 1 WHERE run_id = ?",
                   (owner, now + lease, row["run_id"]))
        r = db.execute("SELECT lease_token, last_event FROM runs WHERE run_id = ?", (row["run_id"],)).fetchone()
        hist = [{"id": h["id"], **json.loads(h["data"])} for h in
                db.execute("SELECT id, data FROM history WHERE run_id = ? ORDER BY id", (row["run_id"],))]
        return WorkflowTask(row["run_id"], row["workflow_id"], row["workflow"], owner, r["lease_token"],
                            r["last_event"], hist)

    def complete_workflow_task(self, task, commands, now, eager=0, queues=("default",)):
        """Appends task_done + the commands and carries them out. Returns
        (status, activities): status is "ok", "conflict" (events arrived
        meanwhile: run the task again) or "lost" (another worker owns the
        task now). With eager > 0, up to that many of the activities just
        scheduled are leased to the same worker in the same transaction."""
        if "split_commit" in self.unsafe:
            return self._complete_split(task, commands, now), []
        with self.tx() as db:
            r = db.execute("SELECT lease_token, last_event, status FROM runs WHERE run_id = ?", (task.run_id,)).fetchone()
            if r["lease_token"] != task.token or r["status"] != "running":
                return "lost", []
            if r["last_event"] != task.last_event and "skip_event_check" not in self.unsafe:
                db.execute("UPDATE runs SET lease_owner = NULL, lease_until = NULL WHERE run_id = ?", (task.run_id,))
                return "conflict", []
            self._append(db, task.run_id, [{"type": "task_done", "time": now}] + commands)
            self._point("workflow_commit")
            db.execute("UPDATE runs SET needs_task = 0, lease_owner = NULL, lease_until = NULL, task_error = NULL"
                       " WHERE run_id = ?", (task.run_id,))
            for cmd in commands:
                self._carry_out(db, task, cmd, now)
            taken = []
            for cmd in commands:
                if len(taken) == eager:
                    break
                if cmd["type"] == "activity_scheduled" and cmd.get("options", {}).get("queue", "default") in queues:
                    row = db.execute("SELECT * FROM activities WHERE run_id = ? AND seq = ?",
                                     (task.run_id, cmd["seq"])).fetchone()
                    if row is not None:  # gone if the same commands closed the run
                        taken.append(self._lease_activity(db, row, row["attempt"], task.owner, now))
        return "ok", taken

    def _complete_split(self, task, commands, now):
        # the broken variant: history and side tables in separate transactions
        with self.tx() as db:
            r = db.execute("SELECT lease_token, last_event FROM runs WHERE run_id = ?", (task.run_id,)).fetchone()
            if r["lease_token"] != task.token:
                return "lost"
            if r["last_event"] != task.last_event:
                db.execute("UPDATE runs SET lease_owner = NULL, lease_until = NULL WHERE run_id = ?", (task.run_id,))
                return "conflict"
            self._append(db, task.run_id, [{"type": "task_done", "time": now}] + commands)
            db.execute("UPDATE runs SET needs_task = 0, lease_owner = NULL, lease_until = NULL WHERE run_id = ?",
                       (task.run_id,))
        self._point("between_commits")
        with self.tx() as db:
            for cmd in commands:
                self._carry_out(db, task, cmd, now)
        return "ok"

    def _carry_out(self, db, task, cmd, now):
        t = cmd["type"]
        if t == "activity_scheduled":
            o = cmd.get("options", {})
            db.execute(
                "INSERT INTO activities (run_id, seq, workflow_id, name, args, queue, retry, timeout, attempt, available_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
                (task.run_id, cmd["seq"], task.workflow_id, cmd["name"], json.dumps(cmd["args"]),
                 o.get("queue", "default"), json.dumps(o.get("retry")), o.get("timeout", DEFAULT_ACTIVITY_TIMEOUT), now))
        elif t == "timer_started":
            db.execute("INSERT INTO timers (run_id, seq, fire_at) VALUES (?, ?, ?)", (task.run_id, cmd["seq"], cmd["fire_at"]))
        elif t == "child_started":
            if db.execute("SELECT 1 FROM runs WHERE workflow_id = ?", (cmd["child_id"],)).fetchone():
                self._input(db, task.run_id, {"type": "child_failed", "seq": cmd["seq"],
                                              "error": {"type": "WorkflowExists", "message": cmd["child_id"]}})
            else:
                self._new_run(db, cmd["workflow"], cmd["child_id"], cmd["input"], now,
                              parent={"run_id": task.run_id, "seq": cmd["seq"]})
        elif t == "completed":
            self._close(db, task.run_id, "completed", cmd["result"], now)
        elif t == "failed":
            self._close(db, task.run_id, "failed", cmd["error"], now)
        elif t == "cancelled":
            self._close(db, task.run_id, "cancelled", None, now)
        elif t == "continued":
            parent = db.execute("SELECT parent FROM runs WHERE run_id = ?", (task.run_id,)).fetchone()["parent"]
            self._close(db, task.run_id, "continued", None, now)
            self._new_run(db, task.workflow, task.workflow_id, cmd["input"], now,
                          parent=json.loads(parent) if parent else None)

    def release(self, task):
        """Gives back a leased task that was never started (graceful shutdown)."""
        with self.tx() as db:
            if isinstance(task, WorkflowTask):
                db.execute("UPDATE runs SET lease_owner = NULL, lease_until = NULL WHERE run_id = ? AND lease_token = ?",
                           (task.run_id, task.token))
            else:
                db.execute("UPDATE activities SET lease_owner = NULL, lease_until = NULL"
                           " WHERE run_id = ? AND seq = ? AND lease_token = ?", (task.run_id, task.seq, task.token))

    def fail_workflow_task(self, task, error, now, retry_in):
        """The workflow code could not run (non-determinism, a bug in replay):
        nothing is written, the task is retried later, e.g. after a fix is
        deployed."""
        with self.tx() as db:
            db.execute("UPDATE runs SET lease_owner = NULL, lease_until = NULL, task_after = ?, task_error = ?"
                       " WHERE run_id = ? AND lease_token = ?", (now + retry_in, error, task.run_id, task.token))

    # ---- activities

    def acquire_activity(self, owner, now, queues=("default",)):
        marks = ",".join("?" * len(queues))
        with self.tx() as db:
            while True:
                row = db.execute(
                    f"SELECT * FROM activities WHERE queue IN ({marks}) AND available_at <= ?"
                    " AND (lease_until IS NULL OR lease_until <= ?) ORDER BY available_at LIMIT 1",
                    (*queues, now, now)).fetchone()
                if row is None:
                    return None
                attempt = row["attempt"]
                if row["lease_until"] is not None:
                    # the worker of the last attempt went quiet past its lease: a timeout
                    policy = RetryPolicy.from_dict(json.loads(row["retry"]))
                    if attempt >= policy.max_attempts:
                        self._activity_failed(db, row["run_id"], row["seq"],
                                              {"type": "Timeout", "message": f"no result after {attempt} attempts"})
                        continue
                    attempt += 1
                return self._lease_activity(db, row, attempt, owner, now)

    def _lease_activity(self, db, row, attempt, owner, now):
        db.execute("UPDATE activities SET attempt = ?, lease_owner = ?, lease_until = ?,"
                   " lease_token = lease_token + 1 WHERE run_id = ? AND seq = ?",
                   (attempt, owner, now + row["timeout"], row["run_id"], row["seq"]))
        return ActivityTask(row["run_id"], row["workflow_id"], row["seq"], row["name"], json.loads(row["args"]),
                            attempt, owner, row["lease_token"] + 1, row["timeout"])

    def _activity_failed(self, db, run_id, seq, error):
        db.execute("DELETE FROM activities WHERE run_id = ? AND seq = ?", (run_id, seq))
        self._input(db, run_id, {"type": "activity_failed", "seq": seq, "error": error})

    def _current(self, db, task):
        row = db.execute("SELECT * FROM activities WHERE run_id = ? AND seq = ?", (task.run_id, task.seq)).fetchone()
        if row is None or row["lease_token"] != task.token:
            return None
        return row

    def complete_activity(self, task, result, now, sticky=False, lease=30.0):
        """Returns (accepted, workflow_task). accepted is False if this
        attempt no longer counts (it timed out and was retried, or the run
        closed). With sticky, the workflow task this result makes due is
        leased to the same worker in the same transaction, if it is free."""
        with self.tx() as db:
            if self._current(db, task) is None:
                return False, None
            db.execute("DELETE FROM activities WHERE run_id = ? AND seq = ?", (task.run_id, task.seq))
            self._input(db, task.run_id, {"type": "activity_done", "seq": task.seq, "result": result})
            self._point("activity_commit")
            wt = None
            if sticky:
                row = db.execute("SELECT run_id, workflow_id, workflow FROM runs WHERE run_id = ? AND needs_task = 1"
                                 " AND task_after <= ? AND (lease_until IS NULL OR lease_until <= ?)",
                                 (task.run_id, now, now)).fetchone()
                if row is not None:
                    wt = self._lease_workflow_task(db, row, task.owner, now, lease)
            return True, wt

    def fail_activity(self, task, error, now):
        with self.tx() as db:
            row = self._current(db, task)
            if row is None:
                return False
            policy = RetryPolicy.from_dict(json.loads(row["retry"]))
            if error["type"] in policy.non_retryable or task.attempt >= policy.max_attempts:
                self._activity_failed(db, task.run_id, task.seq, {**error, "attempts": task.attempt})
            else:
                db.execute("UPDATE activities SET attempt = attempt + 1, available_at = ?, lease_owner = NULL,"
                           " lease_until = NULL, last_error = ? WHERE run_id = ? AND seq = ?",
                           (now + policy.delay(task.attempt), json.dumps(error), task.run_id, task.seq))
            return True

    def heartbeat(self, task, now):
        """Extends the lease of a long activity. False: stop, it no longer counts."""
        with self.tx() as db:
            cur = db.execute("UPDATE activities SET lease_until = ? WHERE run_id = ? AND seq = ? AND lease_token = ?",
                             (now + task.timeout, task.run_id, task.seq, task.token))
            return cur.rowcount == 1

    # ---- timers

    def fire_timers(self, now, limit=100):
        with self.tx() as db:
            rows = db.execute("SELECT run_id, seq FROM timers WHERE fire_at <= ? ORDER BY fire_at, run_id, seq LIMIT ?",
                              (now, limit)).fetchall()
            for r in rows:
                db.execute("DELETE FROM timers WHERE run_id = ? AND seq = ?", (r["run_id"], r["seq"]))
                self._input(db, r["run_id"], {"type": "timer_fired", "seq": r["seq"]})
            return len(rows)

    def next_due(self):
        """The earliest time at which some piece of work becomes due."""
        q = """SELECT MIN(t) FROM (
            SELECT MIN(fire_at) AS t FROM timers
            UNION ALL SELECT MIN(COALESCE(lease_until, available_at)) FROM activities
            UNION ALL SELECT MIN(MAX(task_after, COALESCE(lease_until, 0))) FROM runs WHERE needs_task = 1)"""
        return self._read(q)[0][0]
