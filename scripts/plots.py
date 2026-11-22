from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS = Path("results")
PLOTS = RESULTS / "plots"


def _save(fig, name: str) -> Path:
    PLOTS.mkdir(parents=True, exist_ok=True)
    path = PLOTS / f"{name}.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def _read_csv(name: str) -> list[dict]:
    path = RESULTS / name
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _mean_by(rows: list[dict], key: str, value: str) -> dict[int, float]:
    """Average `value` per integer `key` (multi-run rows collapse into one point)."""
    buckets: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        if r.get(value):
            buckets[int(r[key])].append(float(r[value]))
    return {k: sum(v) / len(v) for k, v in sorted(buckets.items())}


def _tier(tier: str) -> list[dict]:
    return [r for r in _read_csv("bench_runs.csv") if r.get("tier") == tier]


def plot_speedup_vs_workers() -> Path | None:
    rows = _tier("1")
    up = _mean_by(rows, "workers", "upload_throughput_mibs")
    down = _mean_by(rows, "workers", "download_throughput_mibs")
    if not up:
        print("SKIPPED: no tier-1 data")
        return None
    workers = sorted(up)
    fig, ax = plt.subplots()
    ax.plot(
        workers, [up[w] / up[workers[0]] for w in workers], marker="o", label="upload"
    )
    ax.plot(
        workers,
        [down[w] / down[workers[0]] for w in workers],
        marker="s",
        label="download",
    )
    ax.plot(workers, workers, linestyle="--", color="gray", label="ideal (y=W)")
    ax.set_title("Speedup vs Workers (3 nodes)")
    ax.set_xlabel("Workers")
    ax.set_ylabel("Speedup")
    ax.legend()
    return _save(fig, "speedup_vs_workers")


def plot_efficiency_vs_workers() -> Path | None:
    rows = _tier("1")
    up = _mean_by(rows, "workers", "upload_throughput_mibs")
    down = _mean_by(rows, "workers", "download_throughput_mibs")
    if not up:
        print("SKIPPED: no tier-1 data")
        return None
    workers = sorted(up)
    fig, ax = plt.subplots()
    ax.plot(
        workers,
        [(up[w] / up[workers[0]]) / w for w in workers],
        marker="o",
        label="upload",
    )
    ax.plot(
        workers,
        [(down[w] / down[workers[0]]) / w for w in workers],
        marker="s",
        label="download",
    )
    ax.axhline(1.0, linestyle="--", color="gray", label="ideal (y=1.0)")
    ax.set_title("Efficiency vs Workers (3 nodes)")
    ax.set_xlabel("Workers")
    ax.set_ylabel("Efficiency (S/P)")
    ax.legend()
    return _save(fig, "efficiency_vs_workers")


def plot_throughput_vs_nodes() -> Path | None:
    up = _mean_by(_tier("2"), "nodes", "upload_throughput_mibs")
    down = _mean_by(_tier("2"), "nodes", "download_throughput_mibs")
    if not up:
        print("SKIPPED: no tier-2 data")
        return None
    nodes = sorted(up)
    fig, ax = plt.subplots()
    ax.plot(nodes, [up[n] for n in nodes], marker="o", label="upload")
    ax.plot(nodes, [down[n] for n in nodes], marker="s", label="download")
    ax.set_title("Throughput vs Nodes (lite profile, 4 workers)")
    ax.set_xlabel("Data nodes")
    ax.set_ylabel("Throughput (MiB/s)")
    ax.legend()
    return _save(fig, "throughput_vs_nodes")


def plot_scalability_nodes() -> Path | None:
    up = _mean_by(_tier("2"), "nodes", "upload_throughput_mibs")
    if not up:
        print("SKIPPED: no tier-2 data")
        return None
    nodes = sorted(up)
    base = up[nodes[0]]
    fig, ax = plt.subplots()
    ax.plot(nodes, [up[n] / base for n in nodes], marker="o", label="measured")
    ax.plot(
        nodes,
        [n / nodes[0] for n in nodes],
        linestyle="--",
        color="gray",
        label=f"ideal (linear from {nodes[0]} nodes)",
    )
    ax.set_title("Scalability (speedup vs 2 nodes)")
    ax.set_xlabel("Data nodes")
    ax.set_ylabel("Speedup")
    ax.legend()
    return _save(fig, "scalability_nodes")


def plot_tasks_per_sec() -> Path | None:
    tps = _mean_by(_tier("1"), "workers", "tasks_per_sec")
    if not tps:
        print("SKIPPED: no tier-1 data")
        return None
    workers = sorted(tps)
    fig, ax = plt.subplots()
    ax.plot(workers, [tps[w] for w in workers], marker="o")
    ax.set_title("Throughput: tasks completed per second (100 files)")
    ax.set_xlabel("Workers")
    ax.set_ylabel("Tasks/s")
    return _save(fig, "tasks_per_sec")


def plot_latency_per_op() -> Path | None:
    path = RESULTS / "latency_run1.json"
    if not path.exists():
        print("SKIPPED: no latency data")
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    ops = sorted(data)
    x = range(len(ops))
    fig, ax = plt.subplots()
    ax.bar([i - 0.2 for i in x], [data[o]["avg"] for o in ops], width=0.4, label="avg")
    ax.bar([i + 0.2 for i in x], [data[o]["p95"] for o in ops], width=0.4, label="p95")
    ax.set_xticks(list(x))
    ax.set_xticklabels(ops)
    ax.set_title("Latency per File Operation")
    ax.set_ylabel("Seconds")
    ax.legend()
    return _save(fig, "latency_per_op")


def plot_recovery_timeline() -> Path | None:
    rows = _read_csv("fault_summary.csv")
    if not rows:
        print("SKIPPED: no fault data")
        return None
    scenarios = [r["scenario"].upper() for r in rows]
    durations = [float(r["duration_s"]) for r in rows]
    fig, ax = plt.subplots()
    ax.bar(scenarios, durations)
    ax.set_title("Fault Recovery Timeline")
    ax.set_ylabel("Seconds")
    return _save(fig, "recovery_timeline")


def main() -> int:
    PLOTS.mkdir(parents=True, exist_ok=True)
    generated = []
    for fn in (
        plot_speedup_vs_workers,
        plot_efficiency_vs_workers,
        plot_throughput_vs_nodes,
        plot_scalability_nodes,
        plot_tasks_per_sec,
        plot_latency_per_op,
        plot_recovery_timeline,
    ):
        path = fn()
        if path is not None:
            generated.append(path)
            print(path)
    print(f"Generated {len(generated)} plots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
