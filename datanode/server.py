from __future__ import annotations

from concurrent import futures
import logging
import os
from pathlib import Path
import shutil
import signal
import sys
from typing import Iterator

import grpc
from prometheus_client import Counter, Gauge, start_http_server

from common.config import config
from common.grpc_util import FRAME, OPTIONS, get_channel
from common.logging_setup import setup_logging
from datanode.heartbeat import HeartbeatSender
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger("dfs.datanode")

_dn_bytes_in = Counter("dfs_dn_bytes_in", "DataNode bytes received")
_dn_bytes_out = Counter("dfs_dn_bytes_out", "DataNode bytes sent out")
_dn_chunks = Gauge("dfs_dn_chunks", "DataNode chunk count")
_dn_used_bytes = Gauge("dfs_dn_used_bytes", "DataNode used bytes")
_dn_free_bytes = Gauge("dfs_dn_free_bytes", "DataNode free bytes")


class DataNodeServiceImpl(dfs_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, data_dir: str, node_id: str) -> None:
        self.data_dir = Path(data_dir)
        self.node_id = node_id
        self.chunks_dir = self.data_dir / "chunks"
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.bytes_in = 0
        self.bytes_out = 0
        self._scan_initial()

    def _scan_initial(self) -> None:
        self.chunk_count = 0
        self.used_bytes = 0
        for p in self.chunks_dir.rglob("*"):
            if p.is_file() and not p.name.endswith(".tmp"):
                self.chunk_count += 1
                try:
                    self.used_bytes += p.stat().st_size
                except OSError:
                    pass
        logger.info(
            "DATANODE_SCANNED node=%s chunks=%d used_bytes=%d",
            self.node_id,
            self.chunk_count,
            self.used_bytes,
        )

    def get_stats(self) -> tuple[int, int, int, int]:
        """Return (used_bytes, physical_bytes, free_bytes, chunk_count)."""
        free = shutil.disk_usage(self.data_dir).free
        phys = self.used_bytes  # approx on container volume
        _dn_chunks.set(self.chunk_count)
        _dn_used_bytes.set(self.used_bytes)
        _dn_free_bytes.set(free)
        return self.used_bytes, phys, free, self.chunk_count

    def get_chunks_list(self) -> list[tuple[str, int]]:
        result: list[tuple[str, int]] = []
        for p in self.chunks_dir.rglob("*"):
            if p.is_file() and not p.name.endswith(".tmp"):
                try:
                    result.append((p.name, p.stat().st_size))
                except OSError:
                    pass
        return result

    def StoreChunk(
        self, request_iterator: Iterator[dfs_pb2.ChunkData], context: grpc.ServicerContext
    ) -> dfs_pb2.StoreResponse:
        import hashlib

        free = shutil.disk_usage(self.data_dir).free
        if free < config.MIN_FREE_MB * (1 << 20):
            logger.error("DISK_LOW node=%s free_mb=%.1f", self.node_id, free / (1 << 20))
            return dfs_pb2.StoreResponse(ok=False, error="DISK_LOW")

        chunk_id: str | None = None
        hasher = hashlib.sha256()
        bytes_written = 0
        tmp_path: Path | None = None

        try:
            for frame in request_iterator:
                if chunk_id is None:
                    chunk_id = frame.chunk_id
                    if not chunk_id:
                        return dfs_pb2.StoreResponse(ok=False, error="MISSING_CHUNK_ID")
                    # Partition chunks in two-letter prefix folder
                    prefix = chunk_id[:2] if len(chunk_id) >= 2 else "00"
                    target_dir = self.chunks_dir / prefix
                    target_dir.mkdir(parents=True, exist_ok=True)
                    tmp_path = target_dir / f"{chunk_id}.tmp"
                    f = tmp_path.open("wb")

                if frame.data:
                    f.write(frame.data)
                    hasher.update(frame.data)
                    bytes_written += len(frame.data)
                    self.bytes_in += len(frame.data)
                    _dn_bytes_in.inc(len(frame.data))

            if tmp_path and f:
                if config.FSYNC:
                    f.flush()
                    os.fsync(f.fileno())
                f.close()
                final_path = tmp_path.parent / chunk_id
                tmp_path.replace(final_path)

                self.chunk_count += 1
                self.used_bytes += bytes_written
                digest = hasher.hexdigest()
                logger.info(
                    "CHUNK_STORED chunk=%s bytes=%d sha256=%s",
                    chunk_id,
                    bytes_written,
                    digest[:16],
                )
                return dfs_pb2.StoreResponse(
                    ok=True,
                    checksum=digest,
                    bytes_written=bytes_written,
                )
            return dfs_pb2.StoreResponse(ok=False, error="EMPTY_STREAM")
        except Exception as e:
            logger.error("STORE_CHUNK_ERROR chunk=%s err=%s", chunk_id, e)
            if tmp_path and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            return dfs_pb2.StoreResponse(ok=False, error=str(e))

    def RetrieveChunk(
        self, request: dfs_pb2.RetrieveRequest, context: grpc.ServicerContext
    ) -> Iterator[dfs_pb2.ChunkData]:
        prefix = request.chunk_id[:2] if len(request.chunk_id) >= 2 else "00"
        chunk_path = self.chunks_dir / prefix / request.chunk_id
        if not chunk_path.exists():
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Chunk {request.chunk_id} not found")
            return

        with chunk_path.open("rb") as f:
            offset = 0
            while True:
                buf = f.read(FRAME)
                if not buf:
                    break
                is_last = f.tell() == chunk_path.stat().st_size
                self.bytes_out += len(buf)
                _dn_bytes_out.inc(len(buf))
                yield dfs_pb2.ChunkData(
                    chunk_id=request.chunk_id,
                    offset=offset,
                    data=buf,
                    last=is_last,
                )
                offset += len(buf)

    def ReplicateChunk(
        self, request: dfs_pb2.ReplicateRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.ReplicateResponse:
        # Source reads chunk and streams to target node
        import time

        t0 = time.perf_counter()
        prefix = request.chunk_id[:2] if len(request.chunk_id) >= 2 else "00"
        chunk_path = self.chunks_dir / prefix / request.chunk_id
        if not chunk_path.exists():
            return dfs_pb2.ReplicateResponse(ok=False, error="CHUNK_NOT_FOUND")

        total_bytes = chunk_path.stat().st_size

        def gen_stream() -> Iterator[dfs_pb2.ChunkData]:
            with chunk_path.open("rb") as f:
                offset = 0
                while True:
                    data = f.read(FRAME)
                    if not data:
                        break
                    is_last = f.tell() == total_bytes
                    yield dfs_pb2.ChunkData(
                        chunk_id=request.chunk_id,
                        offset=offset,
                        data=data,
                        last=is_last,
                    )
                    offset += len(data)

        try:
            target_chan = get_channel(request.target_address)
            target_stub = dfs_pb2_grpc.DataNodeServiceStub(target_chan)
            resp = target_stub.StoreChunk(gen_stream(), timeout=60.0)
            dur = time.perf_counter() - t0
            if not resp.ok:
                return dfs_pb2.ReplicateResponse(ok=False, error=resp.error, seconds=dur)
            if request.expected_checksum and resp.checksum != request.expected_checksum:
                logger.warning(
                    "CHECKSUM_MISMATCH chunk=%s expected=%s got=%s",
                    request.chunk_id,
                    request.expected_checksum,
                    resp.checksum,
                )
                return dfs_pb2.ReplicateResponse(
                    ok=False, error="CHECKSUM_MISMATCH", seconds=dur
                )
            return dfs_pb2.ReplicateResponse(
                ok=True, bytes=total_bytes, seconds=dur
            )
        except Exception as e:
            return dfs_pb2.ReplicateResponse(
                ok=False, error=str(e), seconds=time.perf_counter() - t0
            )

    def DeleteChunk(
        self, request: dfs_pb2.DeleteChunkRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.DeleteChunkResponse:
        prefix = request.chunk_id[:2] if len(request.chunk_id) >= 2 else "00"
        chunk_path = self.chunks_dir / prefix / request.chunk_id
        if chunk_path.exists():
            try:
                sz = chunk_path.stat().st_size
                chunk_path.unlink()
                self.chunk_count = max(0, self.chunk_count - 1)
                self.used_bytes = max(0, self.used_bytes - sz)
                return dfs_pb2.DeleteChunkResponse(ok=True)
            except OSError:
                pass
        return dfs_pb2.DeleteChunkResponse(ok=False)

    def GetStats(
        self, request: dfs_pb2.StatsRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.StatsResponse:
        used, phys, free, count = self.get_stats()
        return dfs_pb2.StatsResponse(
            node_id=self.node_id,
            used_bytes=used,
            physical_bytes=phys,
            free_bytes=free,
            chunk_count=count,
            bytes_in=self.bytes_in,
            bytes_out=self.bytes_out,
        )


def serve() -> None:
    setup_logging("dfs.datanode")
    node_id = config.NODE_ID or os.getenv("HOSTNAME", "dn-unknown")
    node_addr = config.NODE_ADDR or f"localhost:{config.DN_PORT}"

    service = DataNodeServiceImpl(config.DATA_DIR, node_id)
    start_http_server(config.METRICS_PORT)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=config.GRPC_WORKERS),
        options=OPTIONS,
    )
    dfs_pb2_grpc.add_DataNodeServiceServicer_to_server(service, server)
    bind_addr = f"0.0.0.0:{config.DN_PORT}"
    server.add_insecure_port(bind_addr)
    server.start()
    logger.info("DATANODE_STARTED node=%s bind=%s dir=%s", node_id, bind_addr, config.DATA_DIR)

    hb = HeartbeatSender(
        node_id=node_id,
        address=node_addr,
        master_addr=config.MASTER_ADDR,
        get_stats_fn=service.get_stats,
        get_chunks_fn=service.get_chunks_list,
    )
    hb.start()

    def handle_sigterm(*_: Any) -> None:
        logger.info("DATANODE_STOPPING node=%s", node_id)
        hb.stop()
        server.stop(grace=2.0)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
