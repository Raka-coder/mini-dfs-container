from __future__ import annotations

import csv
import json
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


def plot_speedup_vs_workers() -> Path | None:
    rows = [r for r in _read_csv("bench_runs.csv") if r.get("tier") == "1"]
    if not rows:
        print("SKIPPED: no tier-1 data")
        return None
    rows.sort(key=lambda r: int(r["workers"]))
    base_up = float(rows[0]["upload_throughput_mibs"])
    base_down = float(rows[0]["download_throughput_mibs"])
    workers = [int(r["workers"]) for r in rows]
    speedup_up = [float(r["upload_throughput_mibs"]) / base_up for r in rows]
    speedup_down = [float(r["download_throughput_mibs"]) / base_down for r in rows]

    fig, ax = plt.subplots()
    ax.plot(workers, speedup_up, marker="o", label="upload")
    ax.plot(workers, speedup_down, marker="o", label="download")
    ax.plot(workers, workers, linestyle="--", color="gray", label="ideal (y=W)")
    ax.set_title("Speedup vs Workers")
    ax.set_xlabel("Workers")
    ax.set_ylabel("Speedup")
    ax.legend()
    return _save(fig, "speedup_vs_workers")


def plot_efficiency_vs_workers() -> Path | None:
    rows = [r for r in _read_csv("bench_runs.csv") if r.get("tier") == "1"]
    if not rows:
        print("SKIPPED: no tier-1 data")
        return None
    rows.sort(key=lambda r: int(r["workers"]))
    base_up = float(rows[0]["upload_throughput_mibs"])
    base_down = float(rows[0]["download_throughput_mibs"])
    workers = [int(r["workers"]) for r in rows]
    eff_up = [ (float(r["upload_throughput_mibs"]) / base_up) / int(r["workers"]) for r in rows]
    eff_down = [ (float(r["download_throughput_mibs"]) / base_down) / int(r["workers"]) for r in rows]

    fig, ax = plt.subplots()
    ax.plot(workers, eff_up, marker="o", label="upload")
    ax.plot(workers, eff_down, marker="o", label="download")
    ax.axhline(1.0, linestyle="--", color="gray", label="ideal (y=1.0)")
    ax.set_title("Efficiency vs Workers")
    ax.set_xlabel("Workers")
    ax.set_ylabel("Efficiency")
    ax.legend()
    return _save(fig, "efficiency_vs_workers")


def plot_throughput_vs_nodes() -> Path | None:
    rows = [r for r in _read_csv("bench_runs.csv") if r.get("tier") == "2"]
    if not rows:
        print("SKIPPED: no tier-2 data")
        return None
    rows.sort(key=lambda r: int(r["nodes"]))
    nodes = [int(r["nodes"]) for r in rows]
    thr = [float(r["upload_throughput_mibs"]) for r in rows]
    fig, ax = plt.subplots()
    ax.plot(nodes, thr, marker="o")
    ax.set_title("Throughput vs Nodes")
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Throughput (MiB/s)")
    return _save(fig, "throughput_vs_nodes")


def plot_scalability_nodes() -> Path | None:
    rows = [r for r in _read_csv("bench_runs.csv") if r.get("tier") == "2"]
    if not rows:
        print("SKIPPED: no tier-2 data")
        return None
    rows.sort(key=lambda r: int(r["nodes"]))
    nodes = [int(r["nodes"]) for r in rows]
    dur = [float(r["upload_duration_s"]) for r in rows]
    fig, ax = plt.subplots()
    ax.plot(nodes, dur, marker="o")
    ax.set_title("Scalability (nodes vs duration)")
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Duration (s)")
    return _save(fig, "scalability_nodes")


def plot_latency_per_op() -> Path | None:
    path = RESULTS / "latency_run1.json"
    if not path.exists():
        print("SKIPPED: no latency data")
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    ops = sorted(data.keys())
    fig, ax = plt.subplots()
    ax.bar(ops, [data[o]["avg"] for o in ops])
    ax.set_title("Latency per Op (avg)")
    ax.set_ylabel("Seconds")
    return _save(fig, "latency_per_op")


def plot_recovery_timeline() -> Path | None:
    rows = _read_csv("fault_summary.csv")
    if not rows:
        print("SKIPPED: no fault data")
        return None
    scenarios = [r["scenario"] for r in rows]
    durations = [float(r["duration_s"]) for r in rows]
    fig, ax = plt.subplots()
    ax.bar(scenarios, durations)
    ax.set_title("Recovery Timeline")
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
        plot_latency_per_op,
        plot_recovery_timeline,
    ):
        p = fn()
        if p is not None:
            generated.append(p)
            print(p)
    print(f"Generated {len(generated)} plots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
