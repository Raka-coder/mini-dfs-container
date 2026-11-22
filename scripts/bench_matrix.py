#!/usr/bin/env python3
"""Benchmark matrix for DFS-X.

Tier 1: fixed node count, vary client workers -> speedup / efficiency.
Tier 2: fixed workers, vary node count (2/4/8/16) -> scalability.

The workload itself runs inside the ``dfs-client`` container via ``podman exec``
because published ports do not reach the master on this Podman/WSL2 host. The
client prints one ``BENCH_JSON`` line per run and this script appends a CSV row.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from common.logging_setup import setup_logging

logger = logging.getLogger("dfs.bench_matrix")

RESULTS = ROOT / "results"
BENCH_CSV = RESULTS / "bench_runs.csv"
CLUSTER = ROOT / "scripts" / "cluster.py"

FIELDS = [
    "tier",
    "nodes",
    "profile",
    "workers",
    "run_idx",
    "upload_ok",
    "download_ok",
    "duration_s",
    "total_bytes",
    "throughput_mibps",
    "tasks_per_sec",
    "latency_avg_s",
    "latency_p50_s",
    "latency_p95_s",
    "download_duration_s",
    "download_throughput_mibs",
]


class CsvSink:
    """Append-only CSV writer that flushes every row (long runs must not be lost)."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=FIELDS)
        self._writer.writeheader()
        self._fh.flush()

    def write(self, row: dict) -> None:
        self._writer.writerow(row)
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


def _run(
    cmd: list[str], timeout: int, check: bool = True
) -> subprocess.CompletedProcess:
    logger.info("RUN %s", " ".join(str(c) for c in cmd))
    proc = subprocess.run(
        cmd, check=False, text=True, capture_output=True, timeout=timeout
    )
    if check and proc.returncode != 0:
        logger.error("CMD_FAILED rc=%d stderr=%s", proc.returncode, proc.stderr[-2000:])
        raise SystemExit(proc.returncode)
    return proc


def _cli(*args: str, timeout: int = 3600) -> subprocess.CompletedProcess:
    return _run(
        ["podman", "exec", "dfs-client", "python", "-m", "client.cli", *args],
        timeout=timeout,
        check=False,
    )


def _cluster(action: str, *extra: str) -> None:
    _run([sys.executable, str(CLUSTER), action, *extra], timeout=1800)


def _bench_json(*args: str) -> dict:
    proc = _cli("bench", *args)
    for line in proc.stdout.splitlines():
        if line.startswith("BENCH_JSON "):
            return json.loads(line[len("BENCH_JSON ") :])
    logger.error("NO_BENCH_JSON rc=%d tail=%s", proc.returncode, proc.stdout[-2000:])
    raise SystemExit(1)


def _wait_ready(nodes: int) -> None:
    proc = _cli("wait-ready", "--min-nodes", str(nodes), "--timeout", "180")
    if proc.returncode != 0:
        logger.error("CLUSTER_NOT_READY nodes=%d", nodes)
        raise SystemExit(1)
    logger.info("CLUSTER_READY nodes=%d", nodes)


def _gen_data(profile: str) -> None:
    proc = _cli("gen-data", "--profile", profile, timeout=1800)
    if proc.returncode != 0:
        logger.error("GEN_DATA_FAILED profile=%s err=%s", profile, proc.stderr[-2000:])
        raise SystemExit(1)


def _rm_all() -> None:
    proc = _cli("rm", "--all", timeout=600)
    if proc.returncode != 0:
        logger.error("RM_ALL_FAILED err=%s", proc.stderr[-2000:])
        raise SystemExit(1)


def _one_run(
    sink: CsvSink,
    tier: int,
    nodes: int,
    profile: str,
    workers: int,
    run_idx: int,
    chunk_mb: int,
) -> None:
    _rm_all()
    up = _bench_json(
        "--mode",
        "upload-all",
        "--dir",
        "/work/testdata",
        "--workers",
        str(workers),
        "--chunk-mb",
        str(chunk_mb),
        "--force",
    )
    down = _bench_json(
        "--mode",
        "download-all",
        "--dir",
        "/work/testdata",
        "--workers",
        str(workers),
        "--sink",
        "null",
    )
    lat = up.get("latency", {}).get("upload", {})
    row = {
        "tier": tier,
        "nodes": nodes,
        "profile": profile,
        "workers": workers,
        "run_idx": run_idx,
        "upload_ok": int(bool(up.get("ok"))),
        "download_ok": int(bool(down.get("ok"))),
        "duration_s": round(float(up.get("duration_s", 0.0)), 3),
        "total_bytes": int(up.get("total_bytes", 0)),
        "throughput_mibps": round(float(up.get("throughput_mibs", 0.0)), 3),
        "tasks_per_sec": round(float(up.get("tasks_per_sec", 0.0)), 3),
        "latency_avg_s": round(float(lat.get("avg", 0.0)), 4),
        "latency_p50_s": round(float(lat.get("p50", 0.0)), 4),
        "latency_p95_s": round(float(lat.get("p95", 0.0)), 4),
        "download_duration_s": round(float(down.get("duration_s", 0.0)), 3),
        "download_throughput_mibs": round(float(down.get("throughput_mibs", 0.0)), 3),
    }
    sink.write(row)
    logger.info(
        "ROW tier=%d nodes=%d profile=%s W=%d run=%d up=%.2f MiB/s down=%.2f MiB/s tasks/s=%.2f",
        tier,
        nodes,
        profile,
        workers,
        run_idx,
        row["throughput_mibps"],
        row["download_throughput_mibs"],
        row["tasks_per_sec"],
    )


def main() -> int:
    setup_logging("dfs.bench_matrix")
    p = argparse.ArgumentParser(description="DFS-X benchmark matrix")
    p.add_argument("--tier", default="all", choices=["1", "2", "all"])
    p.add_argument("--runs", type=int, default=3, help="runs per configuration")
    p.add_argument("--tier1-nodes", type=int, default=3)
    p.add_argument("--workers", default="1,2,4,8")
    p.add_argument("--tier1-chunk-mb", type=int, default=8)
    p.add_argument("--tier2-nodes", default="2,4,8,16")
    p.add_argument("--tier2-workers", type=int, default=4)
    p.add_argument("--tier2-chunk-mb", type=int, default=1)
    args = p.parse_args()

    sink = CsvSink(BENCH_CSV)
    try:
        if args.tier in ("1", "all"):
            _gen_data("full")
            for workers in [int(w) for w in args.workers.split(",")]:
                for run_idx in range(1, args.runs + 1):
                    _one_run(
                        sink,
                        1,
                        args.tier1_nodes,
                        "full",
                        workers,
                        run_idx,
                        args.tier1_chunk_mb,
                    )

        if args.tier in ("2", "all"):
            for nodes in [int(n) for n in args.tier2_nodes.split(",")]:
                _cluster("down", "-v")
                _cluster("up", "--nodes", str(nodes))
                _wait_ready(nodes)
                _gen_data("lite")
                for run_idx in range(1, args.runs + 1):
                    _one_run(
                        sink,
                        2,
                        nodes,
                        "lite",
                        args.tier2_workers,
                        run_idx,
                        args.tier2_chunk_mb,
                    )
            # Leave the cluster at the tier-1 size for the fault test.
            _cluster("up", "--nodes", args.tier1_nodes)
            _wait_ready(args.tier1_nodes)
    finally:
        sink.close()

    logger.info("BENCH_MATRIX_DONE csv=%s", BENCH_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
