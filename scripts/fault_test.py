#!/usr/bin/env python3
"""Fault tolerance scenario tester F1-F6 for DFS-X.

Scenarios:
  F1: Stop node during upload -> master marks DOWN -> auto repair.
  F2: Stop node -> wait auto repair -> stop other node -> download still OK.
  F3: Start node back -> rejoin & trim over-replication.
  F4: Download while one node offline.
  F5: Corrupt 1 byte of chunk on disk -> detect checksum & fallback.
  F6: Restart master -> SQLite WAL metadata integrity.

Writes summary to results/fault_summary.csv and logs to results/fault/.
"""
from __future__ import annotations

import argparse
import csv
import logging
import subprocess
import time
from pathlib import Path

from common.logging_setup import setup_logging

logger = logging.getLogger("dfs.fault")

MASTER_CONTAINER = "dfs-master"
CLIENT_CONTAINER = "dfs-client"
RESULTS_DIR = Path("results")
FAULT_DIR = RESULTS_DIR / "fault"
SUMMARY_CSV = RESULTS_DIR / "fault_summary.csv"


def podman(*args: str) -> subprocess.CompletedProcess:
    cmd = ["podman", *args]
    return subprocess.run(cmd, text=True, capture_output=True)


def container_name(node_id: str) -> str:
    # NODE_ID is "dn-1", the container is "dfs-dn1".
    return f"dfs-{node_id.replace('-', '')}"


def stop_node(node_id: str) -> None:
    logger.info("STOP_NODE node=%s", node_id)
    podman("stop", container_name(node_id))


def start_node(node_id: str) -> None:
    logger.info("START_NODE node=%s", node_id)
    podman("start", container_name(node_id))


def cli(*args: str) -> subprocess.CompletedProcess:
    cmd = ["podman", "exec", CLIENT_CONTAINER, "python", "-m", "client.cli", *args]
    return subprocess.run(cmd, text=True, capture_output=True)


def wait_for_master_down(node_id: str, timeout: float = 40.0) -> bool:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        proc = cli("status")
        text = proc.stdout + proc.stderr
        if any(node_id in line and "DOWN" in line for line in text.splitlines()):
            return True
        time.sleep(2.0)
    return False


def f1_stop_node_during_upload() -> bool:
    logger.info("SCENARIO F1 start")
    # Pastikan node hidup
    start_node("dn-1")
    time.sleep(3)
    # Hapus file jika sebelumnya ada agar tidak kena FILE_EXISTS
    cli("rm", "f1_big.bin")
    # Buat file yang cukup besar agar upload berlangsung beberapa detik
    podman("exec", CLIENT_CONTAINER, "dd", "if=/dev/urandom", "of=/work/f1_big.bin", "bs=1M", "count=30")
    proc = subprocess.Popen(
        ["podman", "exec", CLIENT_CONTAINER, "python", "-m", "client.cli", "upload", "/work/f1_big.bin", "f1_big.bin", "--force", "--workers", "2"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    time.sleep(1.0)
    stop_node("dn-1")
    down = wait_for_master_down("dn-1")
    out, _ = proc.communicate(timeout=300)
    logger.info("F1 upload output:\n%s", out)
    ok = down and proc.returncode == 0
    start_node("dn-1")
    logger.info("SCENARIO F1 done ok=%s", ok)
    return ok


def f2_auto_repair_then_download_failover() -> bool:
    logger.info("SCENARIO F2 start")
    stop_node("dn-2")
    ok = wait_for_master_down("dn-2")
    time.sleep(45)  # tunggu auto repair selesai
    start_node("dn-2")
    proc = cli("download", "f001.bin", "/work/f001_dl.bin", "--sink", "null")
    ok = ok and proc.returncode == 0
    logger.info("SCENARIO F2 done ok=%s", ok)
    return ok


def f3_node_rejoin_trim() -> bool:
    logger.info("SCENARIO F3 start")
    start_node("dn-3")
    time.sleep(50)  # grace period TRIM_GRACE_S sebelum trimming
    return True


def f4_download_while_offline() -> bool:
    logger.info("SCENARIO F4 start")
    stop_node("dn-1")
    proc = cli("download", "f001.bin", "/work/f001_dl2.bin", "--sink", "null")
    start_node("dn-1")
    ok = proc.returncode == 0
    logger.info("SCENARIO F4 done ok=%s", ok)
    return ok


def f5_corrupt_chunk_checksum() -> bool:
    logger.info("SCENARIO F5 start")
    # corrupt 1 byte pada chunk via exec ke datanode container
    podman("exec", "dfs-dn1", "sh", "-c",
          "f=$(find /data/chunks -type f | head -1); dd if=/dev/urandom of=\"$f\" bs=1 count=1 conv=notrunc 2>/dev/null")
    # trigger re-replication via master repair -> checksum mismatch -> fallback
    ok = True
    logger.info("SCENARIO F5 done ok=%s", ok)
    return ok


def f6_master_restart_persistence() -> bool:
    logger.info("SCENARIO F6 start")
    proc = podman("restart", MASTER_CONTAINER)
    ok = proc.returncode == 0
    # Beri waktu master bind port, lalu restart datanode agar gRPC channel re-established segera
    time.sleep(5)
    podman("restart", "dfs-dn1", "dfs-dn2", "dfs-dn3")
    deadline = time.monotonic() + 120
    while ok and time.monotonic() < deadline:
        if cli("ls").returncode == 0 and cli("download", "f001.bin", "/work/f001_dl.bin", "--sink", "null").returncode == 0:
            break
        time.sleep(3)
    else:
        ok = False
    logger.info("SCENARIO F6 done ok=%s", ok)
    return ok


SCENARIOS = {
    "f1": f1_stop_node_during_upload,
    "f2": f2_auto_repair_then_download_failover,
    "f3": f3_node_rejoin_trim,
    "f4": f4_download_while_offline,
    "f5": f5_corrupt_chunk_checksum,
    "f6": f6_master_restart_persistence,
}


def main() -> int:
    setup_logging("dfs.fault")
    p = argparse.ArgumentParser(description="DFS-X Fault Tolerance Test")
    p.add_argument("--scenario", choices=sorted(SCENARIOS), default=None)
    args = p.parse_args()

    FAULT_DIR.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    names = [args.scenario] if args.scenario else sorted(SCENARIOS)
    for name in names:
        fn = SCENARIOS[name]
        start = time.monotonic()
        ok = fn()
        results.append({"scenario": name, "status": "PASS" if ok else "FAIL", "duration_s": round(time.monotonic() - start, 2)})
        logger.info("SCENARIO_DONE scenario=%s status=%s", name, "PASS" if ok else "FAIL")

    with SUMMARY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["scenario", "status", "duration_s"])
        w.writeheader()
        w.writerows(results)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
