"""Simulator results for the README: many seeds on the engine as written,
and fewer on each deliberately broken variant.

    python bench/sim.py
"""

import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from durable.scenarios import SCENARIOS  # noqa: E402
from durable.sim import simulate  # noqa: E402

MUTATIONS = {
    "naive_saga": ["transfers"],
    "no_idempotency": ["transfers"],
    "skip_event_check": list(SCENARIOS),
    "split_commit": list(SCENARIOS),
}


def batch(scenario, mutation, seeds):
    t = time.perf_counter()
    failing, stats, first = 0, Counter(), None
    for seed in range(seeds):
        problems, st, _ = simulate(seed, scenario, mutation=mutation)
        stats.update(st)
        if problems:
            failing += 1
            if first is None:
                first = {"seed": seed, "problem": problems[0]}
    return {"scenario": scenario, "mutation": mutation, "seeds": seeds, "failing": failing,
            "seconds": round(time.perf_counter() - t, 1), "first_failure": first, "stats": dict(stats)}


def main():
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    rows = []
    for sc in SCENARIOS:
        rows.append(batch(sc, None, seeds))
        print({k: v for k, v in rows[-1].items() if k != "stats"}, flush=True)
    for m, scs in MUTATIONS.items():
        for sc in scs:
            rows.append(batch(sc, m, min(seeds, 300)))
            print({k: v for k, v in rows[-1].items() if k != "stats"}, flush=True)
    out = os.path.join(os.path.dirname(__file__), "..", "results", "sim.json")
    with open(out, "w") as f:
        json.dump(rows, f, indent=1)


if __name__ == "__main__":
    main()
