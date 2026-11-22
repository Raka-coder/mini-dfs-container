from __future__ import annotations

import logging
import signal
import sys
import time
import uuid
from concurrent import futures
from functools import wraps
from typing import Any

import grpc
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from client.upload import chunk_bounds
from common.config import config
from common.grpc_util import OPTIONS, get_channel
from common.logging_setup import setup_logging
from master.failure import FailureDetector
from master.placement import pick_targets
from master.replication import ReplicationManager
from master.state import ChunkMeta, ClusterState, FileMeta, NodeInfo, UploadCtx
from master.store import MasterStore
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger("dfs.master")

_METRICS_REGISTRY = CollectorRegistry()
dfs_nodes_alive = Gauge(
    "dfs_nodes_alive", "Number of alive nodes", registry=_METRICS_REGISTRY
)
dfs_nodes_down = Gauge(
    "dfs_nodes_down", "Number of down nodes", registry=_METRICS_REGISTRY
)
dfs_under_replicated_chunks = Gauge(
    "dfs_under_replicated_chunks", "Under replicated chunks", registry=_METRICS_REGISTRY
)
dfs_lost_chunks = Gauge("dfs_lost_chunks", "Lost chunks", registry=_METRICS_REGISTRY)
dfs_rereplications_total = Counter(
    "dfs_rereplications_total", "Total replications", registry=_METRICS_REGISTRY
)
dfs_rpc_seconds = Histogram(
    "dfs_rpc_seconds",
    "gRPC call duration seconds",
    ["method"],
    registry=_METRICS_REGISTRY,
)


def rpc_metric(method_name: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(self, request, context):
            start = time.monotonic()
            result = fn(self, request, context)
            dfs_rpc_seconds.labels(method=method_name).observe(time.monotonic() - start)
            return result

        return wrapper

    return decorator


class MasterServiceImpl(dfs_pb2_grpc.MasterServiceServicer):
    def __init__(self, state: ClusterState, store: MasterStore | None = None) -> None:
        self.state = state
        self.store = store

    @rpc_metric("Heartbeat")
    def Heartbeat(
        self, request: dfs_pb2.HeartbeatRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.HeartbeatResponse:
        now = time.monotonic()
        with self.state.lock:
            node = self.state.nodes.get(request.node_id)
            is_new = node is None
            was_down = node is not None and node.state == "DOWN"

            if is_new:
                node = NodeInfo(
                    node_id=request.node_id,
                    address=request.address,
                    state="ALIVE",
                    last_seen=now,
                )
                self.state.nodes[request.node_id] = node
                logger.info(
                    "NODE_REGISTERED node=%s addr=%s chunks=%d",
                    request.node_id,
                    request.address,
                    request.chunk_count,
                )
            else:
                node.address = request.address
                node.last_seen = now
                if was_down:
                    downtime = now - node.down_since
                    node.state = "ALIVE"
                    node.down_since = 0.0
                    logger.info(
                        "NODE_UP node=%s downtime=%.1fs", request.node_id, downtime
                    )

            node.used_bytes = request.used_bytes
            node.physical_bytes = request.physical_bytes
            node.free_bytes = request.free_bytes
            node.chunk_count = request.chunk_count

            req_full = is_new or was_down

            if request.full_report:
                reported_ids = {c.chunk_id for c in request.chunks}
                self.state.node_chunks[request.node_id] = reported_ids
                logger.info(
                    "BLOCK_REPORT node=%s reported_chunks=%d",
                    request.node_id,
                    len(reported_ids),
                )
                req_full = False

        return dfs_pb2.HeartbeatResponse(ok=True, request_full_report=req_full)

    @rpc_metric("ClusterStatus")
    def ClusterStatus(
        self, request: dfs_pb2.StatusRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.StatusResponse:
        now = time.monotonic()
        with self.state.lock:
            nodes_status: list[dfs_pb2.NodeStatus] = []
            for n in self.state.nodes.values():
                age = now - n.last_seen if n.last_seen > 0 else 999999.0
                nodes_status.append(
                    dfs_pb2.NodeStatus(
                        node_id=n.node_id,
                        address=n.address,
                        state=n.state,
                        used_bytes=n.used_bytes,
                        physical_bytes=n.physical_bytes,
                        free_bytes=n.free_bytes,
                        chunk_count=n.chunk_count,
                        last_seen_age_s=age,
                    )
                )

            under_rep = 0
            lost = 0
            for c in self.state.chunks.values():
                alive_replicas = [
                    nid
                    for nid in c.replicas
                    if nid in self.state.nodes
                    and self.state.nodes[nid].state == "ALIVE"
                ]
                if len(alive_replicas) == 0:
                    lost += 1
                elif len(alive_replicas) < config.RF:
                    under_rep += 1

            dfs_nodes_alive.set(
                sum(1 for n in self.state.nodes.values() if n.state == "ALIVE")
            )
            dfs_nodes_down.set(
                len(self.state.nodes)
                - sum(1 for n in self.state.nodes.values() if n.state == "ALIVE")
            )
            dfs_under_replicated_chunks.set(under_rep)
            dfs_lost_chunks.set(lost)
            dfs_rereplications_total.inc(self.state.rereplications_done)

            return dfs_pb2.StatusResponse(
                nodes=nodes_status,
                files=len(self.state.files),
                chunks=len(self.state.chunks),
                under_replicated=under_rep,
                lost=lost,
                rereplications_done=self.state.rereplications_done,
                replication=config.RF,
            )

    @rpc_metric("AllocateChunks")
    def AllocateChunks(
        self, request: dfs_pb2.AllocateRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.AllocateResponse:
        with self.state.lock:
            if request.filename in self.state.files and not request.overwrite:
                return dfs_pb2.AllocateResponse(ok=False, error="FILE_EXISTS")

            upload_id = uuid.uuid4().hex
            chunk_size = (
                request.chunk_size
                if request.chunk_size > 0
                else config.CHUNK_SIZE_MB * (1 << 20)
            )
            file_size = request.file_size
            bounds = chunk_bounds(file_size, chunk_size)

            chunks: list[dict] = []
            chunk_plans: list[dfs_pb2.ChunkPlan] = []
            for idx, (start, size) in enumerate(bounds):
                holders: set[str] = set()
                chunk_id = f"{upload_id}_c{idx:04d}"
                targets = pick_targets(
                    list(self.state.nodes.values()),
                    config.RF,
                    holders=frozenset(holders),
                )
                target_ids = [t.node_id for t in targets]
                holders.update(target_ids)
                chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "index": idx,
                        "size": size,
                        "checksum": "",
                        "stored_on": target_ids,
                    }
                )
                chunk_plans.append(
                    dfs_pb2.ChunkPlan(
                        chunk_id=chunk_id,
                        index=idx,
                        size=size,
                        targets=[
                            dfs_pb2.NodeAddr(node_id=t.node_id, address=t.address)
                            for t in targets
                        ],
                    )
                )

            self.state.uploads[upload_id] = UploadCtx(
                upload_id=upload_id,
                filename=request.filename,
                file_size=file_size,
                chunk_size=chunk_size,
                created_at=time.monotonic(),
                chunks=chunks,
            )
            return dfs_pb2.AllocateResponse(
                ok=True,
                upload_id=upload_id,
                replication=config.RF,
                chunks=chunk_plans,
            )

    @rpc_metric("ReallocateChunk")
    def ReallocateChunk(
        self, request: dfs_pb2.ReallocateRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.ReallocateResponse:
        with self.state.lock:
            ctx = self.state.uploads.get(request.upload_id)
            plan = None
            if ctx is not None:
                plan = next(
                    (c for c in ctx.chunks if c["chunk_id"] == request.chunk_id), None
                )
            if plan is None:
                return dfs_pb2.ReallocateResponse(ok=False, error="UPLOAD_NOT_FOUND")

            new_targets = pick_targets(
                list(self.state.nodes.values()),
                config.RF,
                exclude=frozenset(request.exclude_nodes),
                holders=frozenset(
                    nid for nid in plan["stored_on"] if nid not in request.exclude_nodes
                ),
            )
            plan["stored_on"] = [t.node_id for t in new_targets]
            return dfs_pb2.ReallocateResponse(
                ok=True,
                chunk=dfs_pb2.ChunkPlan(
                    chunk_id=request.chunk_id,
                    index=plan["index"],
                    size=plan["size"],
                    targets=[
                        dfs_pb2.NodeAddr(node_id=t.node_id, address=t.address)
                        for t in new_targets
                    ],
                ),
            )

    @rpc_metric("CommitFile")
    def CommitFile(
        self, request: dfs_pb2.CommitRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.CommitResponse:
        with self.state.lock:
            ctx = self.state.uploads.get(request.upload_id)
            if ctx is None or ctx.filename != request.filename:
                return dfs_pb2.CommitResponse(ok=False, error="UPLOAD_NOT_FOUND")

            under_replicated = 0
            for cr in request.chunks:
                plan = next(
                    (c for c in ctx.chunks if c["chunk_id"] == cr.chunk_id), None
                )
                if len(cr.stored_on) < config.RF:
                    under_replicated += 1
                self.state.chunks[cr.chunk_id] = ChunkMeta(
                    chunk_id=cr.chunk_id,
                    file=ctx.filename,
                    index=plan["index"] if plan else 0,
                    size=plan["size"] if plan else 0,
                    checksum=cr.checksum,
                    replicas=set(cr.stored_on),
                    state="COMMITTED" if cr.stored_on else "LOST",
                )
                for nid in cr.stored_on:
                    self.state.node_chunks.setdefault(nid, set()).add(cr.chunk_id)

            fm = FileMeta(
                name=request.filename,
                size=ctx.file_size,
                chunk_size=ctx.chunk_size,
                file_checksum=request.file_checksum,
                chunk_ids=[c["chunk_id"] for c in ctx.chunks],
                status="COMMITTED",
                created_at=ctx.created_at,
            )
            self.state.files[request.filename] = fm

            if self.store is not None:
                self.store.save_file(fm)
                for c in ctx.chunks:
                    cm = self.state.chunks.get(c["chunk_id"])
                    if cm is not None:
                        self.store.save_chunk(cm)
                for cr in request.chunks:
                    for nid in cr.stored_on:
                        self.store.save_replica(cr.chunk_id, nid)

            return dfs_pb2.CommitResponse(ok=True, under_replicated=under_replicated)

    @rpc_metric("GetReadLocations")
    def GetReadLocations(
        self, request: dfs_pb2.LocationsRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.LocationsResponse:
        with self.state.lock:
            fm = self.state.files.get(request.filename)
            if fm is None:
                return dfs_pb2.LocationsResponse(ok=False, error="FILE_NOT_FOUND")
            chunk_locations: list[dfs_pb2.ChunkLocation] = []
            for cid in fm.chunk_ids:
                cm = self.state.chunks.get(cid)
                if cm is None:
                    continue
                alive = sorted(
                    nid
                    for nid in cm.replicas
                    if nid in self.state.nodes
                    and self.state.nodes[nid].state == "ALIVE"
                )
                chunk_locations.append(
                    dfs_pb2.ChunkLocation(
                        chunk_id=cid,
                        index=cm.index,
                        size=cm.size,
                        checksum=cm.checksum,
                        replicas=[
                            dfs_pb2.NodeAddr(
                                node_id=nid, address=self.state.nodes[nid].address
                            )
                            for nid in alive
                        ],
                    )
                )
            return dfs_pb2.LocationsResponse(
                ok=True,
                file_size=fm.size,
                file_checksum=fm.file_checksum,
                chunks=chunk_locations,
            )

    @rpc_metric("ListFiles")
    def ListFiles(
        self, request: dfs_pb2.ListRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.ListResponse:
        with self.state.lock:
            files = [
                dfs_pb2.FileInfo(
                    name=fm.name,
                    size=fm.size,
                    chunks=len(fm.chunk_ids),
                    status=fm.status,
                    min_replicas=config.RF,
                    created_at=int(fm.created_at),
                )
                for fm in self.state.files.values()
            ]
            return dfs_pb2.ListResponse(files=files)

    @rpc_metric("DeleteFile")
    def DeleteFile(
        self, request: dfs_pb2.DeleteRequest, context: grpc.ServicerContext
    ) -> dfs_pb2.DeleteResponse:
        replicas: list[tuple[str, str]] = []  # (chunk_id, node_address)
        with self.state.lock:
            fm = self.state.files.pop(request.filename, None)
            deleted_chunks = 0
            if fm is not None:
                for cid in fm.chunk_ids:
                    cm = self.state.chunks.pop(cid, None)
                    if cm is None:
                        continue
                    deleted_chunks += 1
                    for nid in cm.replicas:
                        self.state.node_chunks.get(nid, set()).discard(cid)
                        node = self.state.nodes.get(nid)
                        if node is not None:
                            replicas.append((cid, node.address))
                if self.store is not None:
                    self.store.delete_file(request.filename)

        # Drop the bytes too, otherwise `rm --all` leaks chunk files on data nodes.
        for cid, address in replicas:
            try:
                stub = dfs_pb2_grpc.DataNodeServiceStub(get_channel(address))
                stub.DeleteChunk(dfs_pb2.DeleteChunkRequest(chunk_id=cid), timeout=5.0)
            except grpc.RpcError as e:
                logger.warning(
                    "DELETE_CHUNK_FAILED chunk=%s node=%s err=%s",
                    cid,
                    address,
                    e.code(),
                )
        return dfs_pb2.DeleteResponse(ok=True, chunks_deleted=deleted_chunks)


def serve() -> None:
    setup_logging("dfs.master")
    state = ClusterState()
    store = MasterStore(config.DB_PATH)
    files, chunks, replicas = store.load_all()
    state.files = files
    state.chunks = chunks
    for cid, nids in replicas.items():
        if cid in state.chunks:
            state.chunks[cid].replicas = set(nids)
        for nid in nids:
            state.node_chunks.setdefault(nid, set()).add(cid)
    master_service = MasterServiceImpl(state, store)
    failure = FailureDetector(state)
    failure.start()
    repl = ReplicationManager(state, store)
    repl.start()

    start_http_server(config.METRICS_PORT, registry=_METRICS_REGISTRY)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=config.GRPC_WORKERS),
        options=OPTIONS,
    )
    dfs_pb2_grpc.add_MasterServiceServicer_to_server(master_service, server)
    bind_addr = f"0.0.0.0:{config.MASTER_PORT}"
    server.add_insecure_port(bind_addr)
    server.start()
    logger.info("MASTER_STARTED bind=%s rf=%d", bind_addr, config.RF)

    def handle_sigterm(*_: Any) -> None:
        logger.info("MASTER_STOPPING")
        failure.stop()
        repl.stop()
        server.stop(grace=2.0)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
