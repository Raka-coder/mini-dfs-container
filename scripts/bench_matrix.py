#!/usr/bin/env python3
"""Benchmark matrix orchestrator for DFS-X (Tier 1-3)."""
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

from client.bench import Bench

RESULTS_DIR = Path("results")
BENCH_RUNS_CSV = RESULTS_DIR / "bench_runs.csv"
LATENCY_DIR = RESULTS_DIR
EFFICIENCY_TABLE_MD = RESULTS_DIR / "efficiency_table.md"
AMDahl_CSV = RESULTS_DIR / "amdahl.csv"
SUMMARY_MD = RESULTS_DIR / "SUMMARY.md"


def run_tier1(bench: Bench, nodes: int, workers: int) -> dict:
    # Headline: full profile (100 file x 50 MB = 5 GB), N=3 node, worker W in {1,2,4,8}
    return {"tier": 1, "nodes": nodes, "profile": "full", "workers": workers}


def run_tier2(bench: Bench, nodes: int) -> dict:
    # Scalability: lite profile, worker W=4 tetap, variasi node N
    return {"tier": 2, "nodes": nodes, "profile": "lite", "workers": 4}


def run_tier3(bench: Bench, nodes: int) -> dict:
    # Optional: full profile cross-check pada N=2 dan N=4
    return {"tier": 3, "nodes": nodes, "profile": "full", "workers": 4}


def main() -> int:
    p = argparse.ArgumentParser(description="DFS-X Benchmark Matrix")
    p.add_argument("--tier", type=int, default=1, choices=[1, 2, 3])
    p.add_argument("--run-id", default="run1")
    args = p.parse_args()

    bench = Bench(master_addr="master:50051", workers=4)
    if args.tier == 1:
        results = [run_tier1(bench, 3, w) for w in (1, 2, 4, 8)]
    elif args.tier == 2:
        results = [run_tier2(bench, n) for n in (2, 3, 4, 8, 16)]
    else:
        results = [run_tier3(bench, n) for n in (2, 4)]

    rows = []
    for r in results:
        rows.append(
            {
                "tier": r["tier"],
                "nodes": r["nodes"],
                "profile": r["profile"],
                "workers": r["workers"],
                "status": "PENDING_RUN",
            }
        )
    with BENCH_RUNS_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tier", "nodes", "profile", "workers", "status"])
        w.writeheader()
        w.writerows(rows)

    latency_path = LATENCY_DIR / f"latency_{args.run_id}.json"
    latency_path.write_text(json.dumps(bench.latency.summary(), indent=2), encoding="utf-8")

    summary = {"tier": args.tier, "runs": len(results), "status": "PENDING_RUN"}
    SUMMARY_MD.write_text(f"# DFS-X Benchmark SUMMARY\n\ntier={args.tier}\nstatus={summary['status']}\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
