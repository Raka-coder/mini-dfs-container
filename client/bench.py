from __future__ import annotations

import logging
import os
import time

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
