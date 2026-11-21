from __future__ import annotations

from common.config import config

from master.state import NodeInfo


def pick_targets(
    nodes: list[NodeInfo] | tuple[NodeInfo, ...],
    k: int,
    exclude: frozenset[str] = frozenset(),
    holders: frozenset[str] = frozenset(),
) -> list[NodeInfo]:
    """Pick k ALIVE nodes with distinct ids; avoid holders/exclude; load-balanced by planned_bytes."""
    cands = [
        n
        for n in nodes
        if n.state == "ALIVE"
        and n.node_id not in exclude
        and n.node_id not in holders
        and n.free_bytes > config.MIN_FREE_BYTES
    ]
    cands.sort(key=lambda n: (n.used_bytes + n.planned_bytes, n.chunk_count, n.node_id))
    return cands[:k]
