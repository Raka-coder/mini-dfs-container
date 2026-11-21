from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

from client.bench import Bench
from client.gendata import PROFILES, generate
from common.config import config

RESULTS_DIR = Path("results")
BENCH_RUNS_CSV = RESULTS_DIR / "bench_runs.csv"
LATENCY_DIR = RESULTS_DIR
SUMMARY_MD = RESULTS_DIR / "SUMMARY.md"


def run_tier1() -> list[dict]:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    profile = "full"
    count, size_mb, _ = PROFILES[profile]
    data_dir = Path("/work/testdata")
    generate(data_dir, profile, count, size_mb, 0, 1024)

    sample = data_dir / "f001.bin"
    if not sample.exists():
        raise FileNotFoundError(f"sample file missing: {sample}")

    master_addr = config.MASTER_ADDR
    rows: list[dict] = []
    bench = None
    for workers in (1, 2, 4, 8):
        bench = Bench(master_addr=master_addr, workers=workers)
        bench.latency.reset()
        up = bench.bench_upload(
            local_path=str(sample),
            remote="bench_f001.bin",
            chunk_size=size_mb * (1 << 20),
            force=True,
        )
        down = bench.bench_download(
            remote="bench_f001.bin",
            local_path="/work/bench_dl.bin",
            sink="null",
            size=size_mb * (1 << 20),
        )
        rows.append(
            {
                "tier": 1,
                "nodes": 3,
                "profile": profile,
                "workers": workers,
                "status": "OK" if (up["ok"] and down["ok"]) else "FAIL",
                "upload_throughput_mibs": round(up["throughput_mibs"], 3),
                "download_throughput_mibs": round(down["throughput_mibs"], 3),
                "upload_duration_s": round(up["duration_s"], 3),
                "download_duration_s": round(down["duration_s"], 3),
            }
        )
        print(f"W={workers}: upload={up['throughput_mibs']:.2f} MiB/s "
              f"download={down['throughput_mibs']:.2f} MiB/s "
              f"status={rows[-1]['status']}", flush=True)

    # latency summary from the last run (W=8) is most representative; but we want
    # accumulated timings. Re-run latency capture is already in bench.latency of
    # the last Bench instance (only last run). Rebuild an accumulated recorder
    # by re-running is wasteful; instead persist the last run's summary and
    # also write per-worker throughput above. For latency avg/p95 we use the
    # accumulated across all worker counts by re-running a single W=4 pass.
    lat_bench = Bench(master_addr=master_addr, workers=4)
    lat_bench.latency.reset()
    lat_bench.bench_upload(str(sample), "bench_f001.bin", size_mb * (1 << 20), force=True)
    lat_bench.bench_download("bench_f001.bin", "/work/bench_dl.bin", sink="null",
                             size=size_mb * (1 << 20))
    (LATENCY_DIR / "latency_run1.json").write_text(
        json.dumps(lat_bench.latency.summary(), indent=2), encoding="utf-8"
    )

    with BENCH_RUNS_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # speedup/efficiency based on upload duration
    base = rows[0]["upload_duration_s"]
    for r in rows:
        r["speedup"] = round(base / r["upload_duration_s"], 3) if r["upload_duration_s"] > 0 else 0.0
        r["efficiency"] = round(r["speedup"] / r["workers"], 3)

    lines = ["# DFS-X Benchmark SUMMARY", "", "tier=1", "profile=full (representative f001.bin, 50 MiB)",
             "nodes=3", ""]
    lines.append("| workers | upload MiB/s | download MiB/s | upload s | speedup | efficiency |")
    lines.append("|---|---|---|---|---|---|")
    for r in rows:
        lines.append(f"| {r['workers']} | {r['upload_throughput_mibs']} | "
                     f"{r['download_throughput_mibs']} | {r['upload_duration_s']} | "
                     f"{r['speedup']} | {r['efficiency']} |")
    lat = json.loads((LATENCY_DIR / "latency_run1.json").read_text())
    lines.append("")
    lines.append("Latency (W=4 representative run):")
    for op in sorted(lat):
        lines.append(f"- {op}: avg={lat[op]['avg']:.3f}s p50={lat[op]['p50']:.3f}s "
                     f"p95={lat[op]['p95']:.3f}s p99={lat[op]['p99']:.3f}s "
                     f"n={lat[op]['count']}")
    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return rows


def main() -> int:
    p = argparse.ArgumentParser(description="DFS-X Benchmark Matrix")
    p.add_argument("--tier", type=int, default=1, choices=[1, 2, 3])
    p.add_argument("--run-id", default="run1")
    args = p.parse_args()
    if args.tier == 1:
        run_tier1()
    elif args.tier == 2:
        raise NotImplementedError("tier 2 not implemented")
    else:
        raise NotImplementedError("tier 3 not implemented")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
