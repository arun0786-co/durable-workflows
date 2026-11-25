"""README figures from the saved benchmark results. Nothing is re-run.

usage: uv run --with matplotlib python scripts/make_figures.py
reads  results/replay.json, results/throughput.json, results/sim.json
writes docs/images/replay.png, throughput.png, sim_matrix.png
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
OUT = ROOT / "docs" / "images"
ACCENT = "#1f4e79"
LIGHT = "#9db7d0"
GREY = "#9a9a9a"


def style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(alpha=0.25)


def finish(fig, name, note):
    fig.text(0.01, 0.01, note, fontsize=7, color="0.4")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(OUT / name, dpi=150)
    plt.close(fig)


def replay():
    rows = [r for r in json.load(open(RES / "replay.json"))["rows"] if r["activities"] >= 100]
    n = [r["activities"] for r in rows]
    series = [("whole run s, no cache", "no cache: every task replays the full history", GREY, "o"),
              ("no cache, continue_as_new every 100", "no cache, continue-as-new every 100", LIGHT, "s"),
              ("sticky cache", "sticky cache", ACCENT, "D")]
    fig, ax = plt.subplots(figsize=(7, 4))
    for key, label, color, m in series:
        y = [r[key] for r in rows]
        ax.plot(n, y, marker=m, color=color, label=label, lw=2)
        ax.annotate(f"{y[-1]:g} s", (n[-1], y[-1]), xytext=(6, -3), textcoords="offset points", fontsize=8)
    ax.set(xscale="log", yscale="log", xlabel="sequential activities in one workflow",
           ylabel="engine time for the whole run (s)")
    ax.set_xticks(n, [f"{x:,}" for x in n])
    ax.set_xlim(70, 6000)
    ax.set_title("replay cost: quadratic without the cache, linear with it", fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    style(ax)
    finish(fig, "replay.png", "Source: results/replay.json (bench/replay.py), one worker, in-memory database.")


def throughput():
    d = json.load(open(RES / "throughput.json"))
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), gridspec_kw={"width_ratios": [1.5, 1]})
    ax = axes[0]
    for eager, color, label in [(False, GREY, "no eager dispatch"), (True, ACCENT, "eager dispatch")]:
        pts = [r for r in d["throughput"] if r["eager"] == eager and r["synchronous"] == "FULL"]
        ax.plot([r["processes"] for r in pts], [r["workflows/s"] for r in pts], marker="o", color=color, lw=2, label=label)
    normal = [r for r in d["throughput"] if r["synchronous"] == "NORMAL"]
    for r in normal:
        ax.scatter([r["processes"]], [r["workflows/s"]], marker="*", s=120, color=ACCENT, zorder=3,
                   label="eager, synchronous=NORMAL")
    ax.set(xscale="log", xlabel="worker processes", ylabel="workflows / s", ylim=(0, 800))
    ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
    ax.set_title("2,000 queued workflows: more processes do not help", fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=8, loc="lower left")
    style(ax)
    ax = axes[1]
    lat = d["latency"]
    x = np.arange(2)
    for i, r in enumerate(lat):
        color = ACCENT if r["eager"] else GREY
        ax.bar([i - 0.18], [r["p50 ms"]], 0.34, color=color)
        ax.bar([i + 0.18], [r["p99 ms"]], 0.34, color=color, alpha=0.5)
        ax.text(i - 0.18, r["p50 ms"] + 0.4, f"p50 {r['p50 ms']}", ha="center", fontsize=8)
        ax.text(i + 0.18, r["p99 ms"] + 0.4, f"p99 {r['p99 ms']}", ha="center", fontsize=8)
    ax.set_xticks(x, ["no eager dispatch", "eager dispatch"])
    ax.set(ylabel="ms per workflow", ylim=(0, 24))
    ax.set_title("latency, idle worker, 200 workflows", fontsize=10, loc="left")
    style(ax)
    ax.grid(axis="x", visible=False)
    finish(fig, "throughput.png", f"Source: results/throughput.json (bench/throughput.py). Load average about "
                                  f"{d['machine']['load']:.0f} during the run, so numbers vary between runs.")


def sim_matrix():
    d = json.load(open(RES / "sim.json"))
    scen = ["transfers", "approvals", "counters", "fanout"]
    muts = [None, "naive_saga", "no_idempotency", "skip_event_check", "split_commit"]
    names = {None: "as written", "naive_saga": "compensation trusts\n\"retries ran out\"",
             "no_idempotency": "bank does not\ndeduplicate", "skip_event_check": "commit despite events\nduring the task",
             "split_commit": "history and tasks in\ntwo transactions"}
    cell = {(r["mutation"], r["scenario"]): r for r in d}
    frac = np.full((len(muts), len(scen)), np.nan)
    fig, ax = plt.subplots(figsize=(8, 4.4))
    for i, m in enumerate(muts):
        for j, s in enumerate(scen):
            r = cell.get((m, s))
            if r is None:
                ax.text(j, i, "not run", ha="center", va="center", fontsize=8, color="0.6")
                continue
            frac[i, j] = r["failing"] / r["seeds"]
            dark = frac[i, j] > 0.45
            ax.text(j, i, f"{r['failing']} / {r['seeds']}", ha="center", va="center", fontsize=9,
                    color="white" if dark else "#1d1d1f")
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list("acc", ["#ffffff", ACCENT])
    cmap.set_bad("#f4f4f4")
    ax.imshow(np.ma.masked_invalid(frac), cmap=cmap, vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(range(len(scen)), scen)
    ax.set_yticks(range(len(muts)), [names[m] for m in muts], fontsize=8)
    ax.xaxis.tick_top()
    ax.tick_params(which="both", length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_xticks(np.arange(-0.5, len(scen)), minor=True)
    ax.set_yticks(np.arange(-0.5, len(muts)), minor=True)
    ax.grid(which="minor", color="white", lw=2)
    ax.set_title("failing seeds per scenario, correct engine and four broken variants", fontsize=10, loc="left", pad=24)
    finish(fig, "sim_matrix.png", "Source: results/sim.json (bench/sim.py). Shade = fraction of seeds that fail.")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    replay()
    throughput()
    sim_matrix()
    for p in sorted(OUT.glob("*.png")):
        print(f"{p.relative_to(ROOT)}  {p.stat().st_size // 1024} KB")


if __name__ == "__main__":
    main()
