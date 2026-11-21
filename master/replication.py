from __future__ import annotations

import logging
import queue
import threading
import time

import grpc

from common.config import config
from common.grpc_util import get_channel
from master.placement import pick_targets
from master.state import ClusterState
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger("dfs.master.repl")


class ReplicationManager:
    def __init__(self, state: ClusterState, store=None) -> None:
        self.state = state
        self.store = store
        self.running = True
        self.scan_interval = config.SCAN_INTERVAL_S
        self._queue: queue.Queue = queue.Queue()
        self._in_flight: set[str] = set()
        self._up_since: dict[str, float] = {}
        self._thread = threading.Thread(target=self._run, name="master.repl", daemon=True)
        self._worker = threading.Thread(target=self._worker_loop, name="master.repl.worker", daemon=True)

    def start(self) -> None:
        self._thread.start()
        self._worker.start()

    def stop(self) -> None:
        self.running = False
        self._thread.join(timeout=5.0)
        self._worker.join(timeout=5.0)

    def _run(self) -> None:
        while self.running:
            self.scan()
            time.sleep(self.scan_interval)

    def scan(self) -> None:
        with self.state.lock:
            now = time.monotonic()
            for node_id, node in self.state.nodes.items():
                if node.state == "ALIVE" and node_id not in self._up_since:
                    self._up_since[node_id] = now

            under: list[tuple[str, object]] = []
            for cid, cm in self.state.chunks.items():
                if cm.state != "COMMITTED":
                    continue
                alive = [
                    n
                    for n in cm.replicas
                    if n in self.state.nodes and self.state.nodes[n].state == "ALIVE"
                ]
                if len(alive) == 0:
                    cm.state = "LOST"
                    logger.error("CHUNK_LOST chunk=%s file=%s", cid, cm.file)
                elif len(alive) < config.RF:
                    under.append((cid, cm))

            for cid, cm in under:
                task_id = f"repl:{cid}"
                if task_id in self._in_flight:
                    continue
                source_id = min(
                    (n.node_id for n in self.state.nodes.values() if n.state == "ALIVE"),
                    key=lambda n: self.state.nodes[n].used_bytes,
                )
                source = self.state.nodes.get(source_id)
                if source is None:
                    continue
                target = pick_targets(
                    list(self.state.nodes.values()),
                    1,
                    holders=frozenset(cm.replicas),
                )
                if not target:
                    continue
                self._in_flight.add(task_id)
                self._queue.put((task_id, cid, source_id, target[0].node_id, cm.checksum))

            self._trim_over_replication()

    def _worker_loop(self) -> None:
        while True:
            task_id, cid, src_id, dst_id, expected = self._queue.get()
            try:
                source = self.state.nodes.get(src_id)
                if source is None or source.state != "ALIVE":
                    raise RuntimeError("source not alive")
                chan = get_channel(source.address)
                stub = dfs_pb2_grpc.DataNodeServiceStub(chan)
                resp = stub.ReplicateChunk(
                    dfs_pb2.ReplicateRequest(
                        chunk_id=cid,
                        target_address=self.state.nodes[dst_id].address,
                        expected_checksum=expected,
                    ),
                    timeout=60.0,
                )
                if resp.ok:
                    with self.state.lock:
                        cm = self.state.chunks.get(cid)
                        if cm is not None and cm.state == "COMMITTED":
                            cm.replicas.add(dst_id)
                            if self.store is not None:
                                self.store.save_replica(cid, dst_id)
                            self.state.rereplications_done += 1
                    logger.info(
                        "REREPLICATE_DONE chunk=%s src=%s dst=%s bytes=%d took=%.2fs",
                        cid,
                        src_id,
                        dst_id,
                        resp.bytes,
                        resp.seconds,
                    )
                else:
                    logger.warning(
                        "REREPLICATE_FAILED chunk=%s src=%s dst=%s err=%s",
                        cid,
                        src_id,
                        dst_id,
                        resp.error,
                    )
                    self._queue.put((task_id, cid, src_id, dst_id, expected))
            except Exception as e:  # noqa: BLE001 - requeue on transient failure
                logger.warning("REREPLICATE_ERROR chunk=%s err=%s", cid, e)
                self._queue.put((task_id, cid, src_id, dst_id, expected))
            finally:
                self._in_flight.discard(task_id)
                self._queue.task_done()

    def _trim_over_replication(self) -> None:
        with self.state.lock:
            now = time.monotonic()
            for cid, cm in list(self.state.chunks.items()):
                if cm.state != "COMMITTED" or len(cm.replicas) <= config.RF:
                    continue
                alive_ids = {
                    n.node_id for n in self.state.nodes.values() if n.state == "ALIVE"
                }
                if len(alive_ids) < config.RF:
                    continue
                for rid in sorted(set(cm.replicas)):
                    if len(cm.replicas) <= config.RF:
                        break
                    up_since = self._up_since.get(rid)
                    if up_since is None or (now - up_since) <= config.TRIM_GRACE_S:
                        continue
                    logger.info(
                        "TRIM_START chunk=%s victim=%s replicas=%d",
                        cid,
                        rid,
                        len(cm.replicas),
                    )
                    cm.replicas.discard(rid)
                    if self.store is not None:
                        self.store.delete_replica(cid, rid)
                    logger.info("TRIM_DONE chunk=%s victim=%s", cid, rid)

    def get_queue_depth(self) -> int:
        return self._queue.qsize()
