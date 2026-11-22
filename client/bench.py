from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from client.download import Downloader
from client.upload import Uploader

logger = logging.getLogger("dfs.bench")


class LatencyRecorder:
    def __init__(self) -> None:
        self._timings: list[tuple[str, float]] = []

    def record(self, op: str, seconds: float) -> None:
        self._timings.append((op, seconds))

    def summary(self) -> dict:
        per: dict[str, list[float]] = {}
        for op, s in self._timings:
            per.setdefault(op, []).append(s)
        out: dict = {}
        for op, ts in per.items():
            out[op] = {
                "count": len(ts),
                "avg": sum(ts) / len(ts),
                "p50": sorted(ts)[len(ts) // 2],
                "p95": sorted(ts)[int(len(ts) * 0.95)],
                "p99": sorted(ts)[int(len(ts) * 0.99)],
            }
        return out

    def reset(self) -> None:
        self._timings.clear()

    def raw(self) -> list[tuple[str, float]]:
        """Return raw (op, seconds) samples for cross-run aggregation."""
        return list(self._timings)


class Bench:
    def __init__(self, master_addr: str, workers: int = 8) -> None:
        self.uploader = Uploader(master_addr, workers=workers)
        self.downloader = Downloader(master_addr, workers=workers)
        self.latency = LatencyRecorder()

    def bench_upload(
        self, local_path: str, remote: str, chunk_size: int, force: bool = False
    ) -> dict:
        t0 = time.monotonic()
        ok = self.uploader.upload(local_path, remote, chunk_size, force)
        dt = time.monotonic() - t0
        size = os.path.getsize(local_path)
        self.latency.record("upload", dt)
        return {
            "op": "upload",
            "ok": ok,
            "duration_s": dt,
            "throughput_mibs": size / dt / (1 << 20) if dt > 0 else 0.0,
            "bytes": size,
        }

    def bench_download(
        self, remote: str, local_path: str, sink: str = "disk", size: int = 0
    ) -> dict:
        t0 = time.monotonic()
        ok = self.downloader.download(remote=remote, local_path=local_path, sink=sink)
        dt = time.monotonic() - t0
        self.latency.record("download", dt)
        return {
            "op": "download",
            "ok": ok,
            "duration_s": dt,
            "throughput_mibs": size / dt / (1 << 20) if dt > 0 and size > 0 else 0.0,
            "bytes": size,
        }

    def bench_upload_all(
        self,
        local_dir: str,
        remote_prefix: str = "",
        chunk_size: int = 0,
        force: bool = False,
    ) -> dict:
        """Upload every *.bin in local_dir; record per-file latency and totals."""
        files = sorted(Path(local_dir).glob("*.bin"))
        t0 = time.monotonic()
        ok_count = 0
        total_bytes = 0
        for f in files:
            t_file = time.monotonic()
            ok = self.uploader.upload(
                str(f), f"{remote_prefix}{f.name}", chunk_size, force
            )
            self.latency.record("upload", time.monotonic() - t_file)
            if ok:
                ok_count += 1
            total_bytes += f.stat().st_size
        duration = time.monotonic() - t0
        return {
            "op": "upload_all",
            "ok": ok_count == len(files) and len(files) > 0,
            "files_ok": ok_count,
            "files_total": len(files),
            "duration_s": duration,
            "total_bytes": total_bytes,
            "throughput_mibs": total_bytes / duration / (1 << 20)
            if duration > 0
            else 0.0,
            "tasks_per_sec": len(files) / duration if duration > 0 else 0.0,
        }

    def bench_download_all(
        self, local_dir: str, remote_prefix: str = "", sink: str = "null"
    ) -> dict:
        """Download every remote file corresponding to local_dir/*.bin."""
        files = sorted(Path(local_dir).glob("*.bin"))
        t0 = time.monotonic()
        ok_count = 0
        total_bytes = 0
        for f in files:
            t_file = time.monotonic()
            ok = self.downloader.download(
                remote=f"{remote_prefix}{f.name}",
                local_path=f"/work/bench_dl_{f.name}",
                sink=sink,
            )
            self.latency.record("download", time.monotonic() - t_file)
            if ok:
                ok_count += 1
            total_bytes += f.stat().st_size
        duration = time.monotonic() - t0
        return {
            "op": "download_all",
            "ok": ok_count == len(files) and len(files) > 0,
            "files_ok": ok_count,
            "files_total": len(files),
            "duration_s": duration,
            "total_bytes": total_bytes,
            "throughput_mibs": total_bytes / duration / (1 << 20)
            if duration > 0
            else 0.0,
            "tasks_per_sec": len(files) / duration if duration > 0 else 0.0,
        }
