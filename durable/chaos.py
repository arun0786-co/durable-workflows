"""kill -9 test with real processes: worker processes share a SQLite file,
the bank is a second SQLite file standing for an outside system, and a
random worker is killed with SIGKILL every fraction of a second (and a new
one started) until every transfer has finished.

    python -m durable.chaos --transfers 300 --workers 4 --kill-every 0.3
"""

import argparse
import json
import os
import random
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from .client import Client
from .scenarios import FAST, Frozen, Insufficient, Voided, transfer
from .sim import history_problems
from .store import Store
from .worker import Worker, activity_context


class SqliteBank:
    def __init__(self, path):
        self.path = path
        self.local = threading.local()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS balances (acct TEXT PRIMARY KEY, bal INTEGER NOT NULL, frozen INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS ops (key TEXT PRIMARY KEY, acct TEXT, delta INTEGER);
            CREATE TABLE IF NOT EXISTS voided (key TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS calls (n INTEGER);
            INSERT INTO calls SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM calls);""")

    @property
    def db(self):  # one connection per thread: activities run on several
        if not hasattr(self.local, "db"):
            self.local.db = sqlite3.connect(self.path, isolation_level=None, timeout=60)
        return self.local.db

    def _tx(self, fn):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            out = fn()
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")
        return out

    def apply(self, key, acct, delta):
        def go():
            self.db.execute("UPDATE calls SET n = n + 1")
            if self.db.execute("SELECT 1 FROM voided WHERE key = ?", (key,)).fetchone():
                raise Voided(key)
            bal, frozen = self.db.execute("SELECT bal, frozen FROM balances WHERE acct = ?", (acct,)).fetchone()
            if self.db.execute("SELECT 1 FROM ops WHERE key = ?", (key,)).fetchone():
                return bal
            if frozen:
                raise Frozen(acct)
            if bal + delta < 0:
                raise Insufficient(acct)
            self.db.execute("UPDATE balances SET bal = bal + ? WHERE acct = ?", (delta, acct))
            self.db.execute("INSERT INTO ops VALUES (?, ?, ?)", (key, acct, delta))
            return bal + delta
        return self._tx(go)

    def void(self, key):
        def go():
            self.db.execute("UPDATE calls SET n = n + 1")
            if self.db.execute("SELECT 1 FROM voided WHERE key = ?", (key,)).fetchone():
                return
            self.db.execute("INSERT INTO voided VALUES (?)", (key,))
            op = self.db.execute("SELECT acct, delta FROM ops WHERE key = ?", (key,)).fetchone()
            if op:
                self.db.execute("UPDATE balances SET bal = bal - ? WHERE acct = ?", (op[1], op[0]))
        self._tx(go)

    def balances(self):
        return dict(self.db.execute("SELECT acct, bal FROM balances"))


def bank_activities(bank):
    def pause():
        time.sleep(random.uniform(0, 0.02))  # widen the window for a kill to land mid-call

    def withdraw(key, acct, amount):
        pause()
        r = bank.apply(key, acct, -amount)
        pause()
        return r

    def deposit(key, acct, amount):
        pause()
        r = bank.apply(key, acct, amount)
        pause()
        return r

    def void(key):
        pause()
        bank.void(key)
        pause()

    return [withdraw, deposit, void]


def worker_main(db, bank_path):
    w = Worker(Store(db), [transfer], bank_activities(SqliteBank(bank_path)), lease=2, task_retry=1)
    w.run(activity_threads=4, poll=0.01)
    while True:
        time.sleep(3600)


def run(transfers=200, workers=4, kill_every=0.3, seed=0, timeout=300, workdir=None):
    rng = random.Random(seed)
    d = workdir or tempfile.mkdtemp(prefix="durable-chaos-")
    db, bank_path = os.path.join(d, "engine.db"), os.path.join(d, "bank.db")
    bank = SqliteBank(bank_path)
    accounts = {f"acct{i}": rng.randint(100, 500) for i in range(8)}
    frozen = rng.choice(list(accounts))
    for a, bal in accounts.items():
        bank.db.execute("INSERT INTO balances VALUES (?, ?, ?)", (a, bal, int(a == frozen)))
    store = Store(db)
    client = Client(store)
    plan = [(f"t{i}", *rng.sample(list(accounts), 2), rng.randint(1, 30)) for i in range(transfers)]
    for wid, src, dst, amount in plan:
        client.start(transfer, src, dst, amount, id=wid)

    env = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def spawn():
        return subprocess.Popen([sys.executable, "-m", "durable.chaos", "--worker", db, bank_path], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    procs = [spawn() for _ in range(workers)]
    kills, t0 = 0, time.monotonic()
    try:
        while store.runs("running"):
            if time.monotonic() - t0 > timeout:
                break
            time.sleep(rng.uniform(0, 2 * kill_every))
            i = rng.randrange(len(procs))
            os.kill(procs[i].pid, signal.SIGKILL)
            procs[i].wait()
            kills += 1
            procs[i] = spawn()
    finally:
        for p in procs:
            p.kill()
            p.wait()
    secs = time.monotonic() - t0

    problems = [f"{r['workflow_id']}: never finished ({r['task_error']})" for r in store.runs("running")]
    for r in store.runs():
        problems += history_problems(r["workflow_id"], r, store.history(r["run_id"]), transfer)
    expect = dict(accounts)
    statuses = {}
    for wid, src, dst, amount in plan:
        res = store.describe(wid)["result"] or {}
        statuses[res.get("status")] = statuses.get(res.get("status"), 0) + 1
        if res.get("status") == "done":
            expect[src] -= amount
            expect[dst] += amount
    actual = SqliteBank(bank_path).balances()
    if actual != expect:
        problems.append(f"balances {actual} != {expect} implied by the results")
    calls = bank.db.execute("SELECT n FROM calls").fetchone()[0]
    ops = bank.db.execute("SELECT COUNT(*) FROM ops").fetchone()[0] + bank.db.execute("SELECT COUNT(*) FROM voided").fetchone()[0]
    return {"transfers": transfers, "workers": workers, "kills": kills, "seconds": round(secs, 1),
            "statuses": statuses, "bank_calls": calls, "bank_effects": ops, "problems": problems, "dir": d}


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker_main(sys.argv[2], sys.argv[3])
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("--transfers", type=int, default=200)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--kill-every", type=float, default=0.3)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    res = run(a.transfers, a.workers, a.kill_every, a.seed)
    print(json.dumps({k: v for k, v in res.items() if k != "problems"}), flush=True)
    for p in res["problems"][:10]:
        print("PROBLEM", p)
    if a.out:
        with open(a.out, "w") as f:
            json.dump(res, f, indent=1)
    sys.exit(1 if res["problems"] else 0)


if __name__ == "__main__":
    main()
