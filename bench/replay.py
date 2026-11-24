"""What replay costs. Without the sticky cache every workflow task reads and
replays the run's whole history, so a run with n steps does O(n^2) work;
continue_as_new keeps the history short. With the cache a worker keeps the
replayer and feeds it only new events. In-memory store, one worker: this
measures the engine's CPU cost, not the disk.

    python bench/replay.py
"""

import json
import os
import platform
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from durable import Client, Store, Worker  # noqa: E402
from durable.runtime import Replayer  # noqa: E402


def inc(x):
    return x + 1


async def loop(ctx, n):
    x = 0
    for _ in range(n):
        x = await ctx.activity(inc, x)
    return x


async def loop_chunked(ctx, n, x=0, every=100):
    for _ in range(min(n, every)):
        x = await ctx.activity(inc, x)
    if n > every:
        ctx.continue_as_new(n - every, x, every)
    return x


def run(fn, n, sticky):
    store = Store()
    w = Worker(store, [fn], [inc], clock=lambda: 0.0, sticky=sticky)
    wid = Client(store, clock=lambda: 0.0).start(fn, n)
    t = time.perf_counter()
    while w.workflow_step() | w.activity_step():
        pass
    secs = time.perf_counter() - t
    d = store.describe(wid)
    assert d["result"] == n, d
    return secs, store.history(d["run_id"])


def main():
    rows = []
    for n in (10, 100, 1000, 3000):
        secs, hist = run(loop, n, False)
        reps = max(1, 2000 // n)
        t = time.perf_counter()
        for _ in range(reps):
            Replayer(loop, "x", "r", hist).run(0.0)
        rep = (time.perf_counter() - t) / reps
        csecs, _ = run(loop_chunked, n, False)
        ssecs, _ = run(loop, n, True)
        rows.append({"activities": n, "events": len(hist), "one replay ms": round(rep * 1000, 2),
                     "us per event": round(rep / len(hist) * 1e6, 2), "whole run s, no cache": round(secs, 2),
                     "no cache, continue_as_new every 100": round(csecs, 2), "sticky cache": round(ssecs, 2)})
        print(rows[-1], flush=True)
    out = os.path.join(os.path.dirname(__file__), "..", "results", "replay.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump({"machine": {"chip": platform.machine(), "python": platform.python_version(),
                               "load": os.getloadavg()[0]}, "rows": rows}, f, indent=1)


if __name__ == "__main__":
    main()
