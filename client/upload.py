from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

from common.checksum import combine_checksums, sha256_bytes
from common.config import config
from common.grpc_util import FRAME, get_channel, with_retry

logger = logging.getLogger("dfs.client")


def chunk_bounds(file_size: int, chunk_size: int) -> list[tuple[int, int]]:
    """Split [0, file_size) into (start, size) chunks of at most chunk_size."""
    bounds: list[tuple[int, int]] = []
    if file_size <= 0:
        return bounds
    start = 0
    while start < file_size:
        size = min(chunk_size, file_size - start)
        bounds.append((start, size))
        start += size
    return bounds


class Uploader:
    def __init__(self, master_addr: str, workers: int = 8) -> None:
        from proto import dfs_pb2, dfs_pb2_grpc

        self.channel = get_channel(master_addr)
        self.stub = dfs_pb2_grpc.MasterServiceStub(self.channel)
        self.workers = workers

    def upload(
        self, local_path: str, remote: str, chunk_size: int, force: bool = False
    ) -> bool:
        from proto import dfs_pb2, dfs_pb2_grpc

        file_size = os.path.getsize(local_path)
        size = chunk_size if chunk_size > 0 else config.CHUNK_SIZE_MB * (1 << 20)

        allocate = self.stub.AllocateChunks(
            dfs_pb2.AllocateRequest(
                filename=remote,
                file_size=file_size,
                chunk_size=size,
                overwrite=force,
            ),
            timeout=5.0,
        )
        if not allocate.ok:
            logger.error("ALLOCATE_FAILED file=%s err=%s", remote, allocate.error)
            return False

        def store_to(plan: "dfs_pb2.ChunkPlan") -> tuple[str, list[str]]:
            content = b""
            with open(local_path, "rb") as f:
                f.seek(plan.index * size)
                content = f.read(plan.size)
            checksum = sha256_bytes(content)
            stored_on: list[str] = []
            for t in plan.targets:

                def gen() -> Iterator["dfs_pb2.ChunkData"]:
                    offset = 0
                    total = len(content)
                    while offset < total:
                        data = content[offset : offset + FRAME]
                        yield dfs_pb2.ChunkData(
                            chunk_id=plan.chunk_id,
                            offset=offset,
                            data=data,
                            last=(offset + len(data) >= total),
                        )
                        offset += len(data)

                resp = with_retry(
                    lambda: dfs_pb2_grpc.DataNodeServiceStub(get_channel(t.address)).StoreChunk(gen(), timeout=60.0)
                )
                if resp.ok and resp.checksum == checksum:
                    stored_on.append(t.node_id)
                else:
                    logger.warning(
                        "STORE_FAILED chunk=%s node=%s err=%s",
                        plan.chunk_id,
                        t.node_id,
                        resp.error if not resp.ok else "CHECKSUM_MISMATCH",
                    )
            return checksum, stored_on

        def store_one(plan: "dfs_pb2.ChunkPlan", upload_id: str) -> "dfs_pb2.ChunkResult":
            checksum, stored_on = store_to(plan)
            failed = [t.node_id for t in plan.targets if t.node_id not in stored_on]
            if failed and len(stored_on) < len(plan.targets):
                logger.info("REALLOCATE chunk=%s exclude=%s", plan.chunk_id, failed)
                realloc = self.stub.ReallocateChunk(
                    dfs_pb2.ReallocateRequest(
                        upload_id=upload_id,
                        chunk_id=plan.chunk_id,
                        exclude_nodes=frozenset(failed),
                    ),
                    timeout=5.0,
                )
                if realloc.ok and realloc.chunk.targets:
                    new_checksum, new_stored = store_to(realloc.chunk)
                    if new_stored:
                        stored_on = list(dict.fromkeys(stored_on + new_stored))
                        checksum = new_checksum
            return dfs_pb2.ChunkResult(
                chunk_id=plan.chunk_id, checksum=checksum, stored_on=stored_on
            )

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            results = list(ex.map(lambda c: store_one(c, allocate.upload_id), allocate.chunks))

        failed = [r.chunk_id for r in results if not r.stored_on]
        if failed:
            logger.error("UPLOAD_FAILED file=%s chunks=%s", remote, failed)
            return False

        file_checksum = combine_checksums([r.checksum for r in results])
        commit = self.stub.CommitFile(
            dfs_pb2.CommitRequest(
                upload_id=allocate.upload_id,
                filename=remote,
                file_checksum=file_checksum,
                chunks=results,
            ),
            timeout=5.0,
        )
        if not commit.ok:
            logger.error("COMMIT_FAILED file=%s err=%s", remote, commit.error)
            return False
        logger.info(
            "UPLOAD_COMMIT file=%s chunks=%d under_replicated=%d",
            remote,
            len(results),
            commit.under_replicated,
        )
        return True
