"""Workflows per second on one SQLite file, with 1 to 8 worker processes,
and the latency of a single workflow on an idle system.

Each workflow runs three activities in sequence: 4 workflow tasks and 3
activities, 14 transactions without eager dispatch, 8 with it.

    python bench/throughput.py
"""

import json
import os
import platform
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from durable import Client, Store, Worker  # noqa: E402


def inc(x):
    return x + 1


async def three_steps(ctx, x):
    a = await ctx.activity(inc, x)
    b = await ctx.activity(inc, a)
    return await ctx.activity(inc, b)


def worker_main(db, sync, eager):
    Worker(Store(db, synchronous=sync), [three_steps], [inc], lease=30, eager=eager == "1").run(
        activity_threads=2, poll=0.002)
    while True:
        time.sleep(3600)


def spawn(db, sync, n, eager):
    env = dict(os.environ, PYTHONPATH=os.path.join(os.path.dirname(__file__), ".."))
    return [subprocess.Popen([sys.executable, __file__, "--worker", db, sync, "1" if eager else "0"], env=env)
            for _ in range(n)]


def throughput(procs, n, sync, eager):
    d = tempfile.mkdtemp(prefix="durable-bench-")
    db = os.path.join(d, "engine.db")
    store = Store(db, synchronous=sync)
    client = Client(store)
    for i in range(n):
        client.start(three_steps, i, id=f"w{i}")
    ps = spawn(db, sync, procs, eager)
    t0 = time.perf_counter()
    while store.runs("running"):
        time.sleep(0.05)
    secs = time.perf_counter() - t0
    for p in ps:
        p.kill()
        p.wait()
    assert all(r["status"] == "completed" for r in store.runs())
    return {"eager": eager, "processes": procs, "workflows": n, "seconds": round(secs, 2),
            "workflows/s": round(n / secs), "synchronous": sync}


def latency(sync, eager, n=200):
    d = tempfile.mkdtemp(prefix="durable-bench-")
    db = os.path.join(d, "engine.db")
    store = Store(db, synchronous=sync)
    client = Client(store)
    ps = spawn(db, sync, 1, eager)
    time.sleep(1.0)
    out = []
    for i in range(n):
        t = time.perf_counter()
        wid = client.start(three_steps, i)
        assert client.result(wid, poll=0.0005) == i + 3
        out.append((time.perf_counter() - t) * 1000)
    for p in ps:
        p.kill()
        p.wait()
    out.sort()
    return {"eager": eager, "workflows": n, "p50 ms": round(out[n // 2], 1), "p99 ms": round(out[int(n * 0.99)], 1),
            "synchronous": sync}


def main():
    rows = []
    for eager in (False, True):
        for procs in (1, 2, 4, 8):
            rows.append(throughput(procs, 2000, "FULL", eager))
            print(rows[-1], flush=True)
    rows.append(throughput(4, 2000, "NORMAL", True))
    print(rows[-1], flush=True)
    lat = [latency("FULL", False), latency("FULL", True)]
    for r in lat:
        print(r, flush=True)
    os.makedirs(os.path.join(os.path.dirname(__file__), "..", "results"), exist_ok=True)
    with open(os.path.join(os.path.dirname(__file__), "..", "results", "throughput.json"), "w") as f:
        json.dump({"machine": {"chip": platform.processor() or platform.machine(), "python": platform.python_version(),
                               "cpus": os.cpu_count(), "load": os.getloadavg()[0]},
                   "throughput": rows, "latency": lat}, f, indent=1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker_main(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main()
