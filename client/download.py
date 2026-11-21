from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor

import grpc

from common.checksum import combine_checksums, sha256_bytes
from common.grpc_util import get_channel
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger("dfs.client")


class Downloader:
    def __init__(self, master_addr: str, workers: int = 8) -> None:
        self.channel = get_channel(master_addr)
        self.stub = dfs_pb2_grpc.MasterServiceStub(self.channel)
        self.workers = workers

    def _retrieve_chunk(
        self, cl: dfs_pb2.ChunkLocation
    ) -> tuple[bytes, str] | None:
        """Fetch chunk from first alive replica that passes checksum; None on total failure."""
        for na in cl.replicas:
            content = b""
            try:
                stream = self._data_node_stub(na.address).RetrieveChunk(
                    dfs_pb2.RetrieveRequest(chunk_id=cl.chunk_id), timeout=60.0
                )
                for frame in stream:
                    content += frame.data
                checksum = sha256_bytes(content)
                if checksum == cl.checksum:
                    return content, checksum
                logger.warning("CHECKSUM_MISMATCH chunk=%s node=%s", cl.chunk_id, na.node_id)
            except Exception as e:  # noqa: BLE001 - failover on any retrieval error
                logger.warning(
                    "RETRIEVE_FAILED chunk=%s node=%s err=%s",
                    cl.chunk_id,
                    na.node_id,
                    e,
                )
        return None

    def _data_node_stub(self, address: str) -> dfs_pb2_grpc.DataNodeServiceStub:
        return dfs_pb2_grpc.DataNodeServiceStub(get_channel(address))

    def download(self, remote: str, local_path: str, sink: str = "disk") -> bool:
        locations = self.stub.GetReadLocations(
            dfs_pb2.LocationsRequest(filename=remote), timeout=5.0
        )
        if not locations.ok:
            logger.error("GET_LOCATIONS_FAILED file=%s err=%s", remote, locations.error)
            return False

        retrieved: list[tuple[dfs_pb2.ChunkLocation, bytes, str]] = []
        ok_all = True
        for cl in locations.chunks:
            got = self._retrieve_chunk(cl)
            if got is None:
                ok_all = False
                break
            retrieved.append((cl, got[0], got[1]))

        if not ok_all:
            logger.error("DOWNLOAD_FAILED file=%s", remote)
            return False

        file_checksum = combine_checksums([chk for _, _, chk in retrieved])
        if file_checksum != locations.file_checksum:
            logger.error("FILE_CHECKSUM_MISMATCH file=%s", remote)
            return False

        if sink == "disk":
            fd = os.open(local_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                offset = 0
                for cl, content, _chk in retrieved:
                    os.pwrite(fd, content, offset)
                    offset += len(content)
            finally:
                os.close(fd)

        logger.info("DOWNLOAD_OK file=%s chunks=%d", remote, len(retrieved))
        return True
