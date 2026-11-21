from __future__ import annotations

import logging
import threading
import time

from common.config import config
from master.state import ClusterState

logger = logging.getLogger("dfs.master.failure")


class FailureDetector:
    """Scans master state for heartbeats stale beyond HEARTBEAT_TIMEOUT_S."""

    def __init__(self, state: ClusterState) -> None:
        self.state = state
        self.running = True
        self._thread = threading.Thread(target=self._run, name="master.failure", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.running = False
        self._thread.join(timeout=5.0)

    def _run(self) -> None:
        while self.running:
            self.scan()
            time.sleep(config.SCAN_INTERVAL_S)

    def scan(self) -> None:
        now = time.monotonic()
        with self.state.lock:
            for node_id, node in list(self.state.nodes.items()):
                if node.state == "ALIVE" and (now - node.last_seen) > config.HEARTBEAT_TIMEOUT_S:
                    node.state = "DOWN"
                    node.down_since = now
                    affected = len(self.state.node_chunks.get(node_id, ()))
                    logger.warning(
                        "NODE_DOWN node=%s silent_for=%.1fs chunks_affected=%d",
                        node_id,
                        now - node.last_seen,
                        affected,
                    )
