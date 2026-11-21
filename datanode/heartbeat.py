from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import grpc

from common.config import config
from common.grpc_util import get_channel
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger("dfs.heartbeat")


class HeartbeatSender(threading.Thread):
    def __init__(
        self,
        node_id: str,
        address: str,
        master_addr: str,
        get_stats_fn: Callable[[], tuple[int, int, int, int]],  # used, phys, free, count
        get_chunks_fn: Callable[[], list[tuple[str, int]]],  # list of (chunk_id, size)
    ) -> None:
        super().__init__(name=f"heartbeat-{node_id}", daemon=True)
        self.node_id = node_id
        self.address = address
        self.master_addr = master_addr
        self.get_stats_fn = get_stats_fn
        self.get_chunks_fn = get_chunks_fn
        self.running = True
        self.force_full_report = True
        self._last_warn_time = 0.0

    def run(self) -> None:
        logger.info("HEARTBEAT_THREAD_START target=%s", self.master_addr)
        while self.running:
            try:
                channel = get_channel(self.master_addr)
                stub = dfs_pb2_grpc.MasterServiceStub(channel)
                used, phys, free, count = self.get_stats_fn()

                chunks_proto: list[dfs_pb2.ChunkInfo] = []
                if self.force_full_report:
                    chunks_data = self.get_chunks_fn()
                    chunks_proto = [
                        dfs_pb2.ChunkInfo(chunk_id=cid, size=sz)
                        for cid, sz in chunks_data
                    ]

                req = dfs_pb2.HeartbeatRequest(
                    node_id=self.node_id,
                    address=self.address,
                    used_bytes=used,
                    physical_bytes=phys,
                    free_bytes=free,
                    chunk_count=count,
                    full_report=self.force_full_report,
                    chunks=chunks_proto,
                )
                resp = stub.Heartbeat(req, timeout=5.0)
                if resp.request_full_report:
                    self.force_full_report = True
                else:
                    self.force_full_report = False

            except grpc.RpcError as e:
                now = time.monotonic()
                if now - self._last_warn_time > 10.0:  # rate-limit warning
                    logger.warning(
                        "HEARTBEAT_FAILED master=%s err=%s",
                        self.master_addr,
                        e.details() if hasattr(e, "details") else e,
                    )
                    self._last_warn_time = now
            except Exception as e:
                logger.error("HEARTBEAT_UNEXPECTED_ERROR err=%s", e)

            time.sleep(config.HEARTBEAT_INTERVAL_S)

    def stop(self) -> None:
        self.running = False
