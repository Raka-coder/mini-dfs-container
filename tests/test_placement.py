from __future__ import annotations

from master.placement import pick_targets
from master.state import NodeInfo


def _node(
    nid: str,
    state: str = "ALIVE",
    used: int = 0,
    planned: int = 0,
    free: int = 10**9,
    chunks: int = 0,
) -> NodeInfo:
    return NodeInfo(
        node_id=nid,
        address=f"{nid}:50052",
        state=state,
        last_seen=1.0,
        used_bytes=used,
        physical_bytes=used,
        free_bytes=free,
        chunk_count=chunks,
        planned_bytes=planned,
    )


def test_pick_targets_two_alive_nodes_anti_affinity() -> None:
    nodes = [_node("dn-1"), _node("dn-2"), _node("dn-3")]
    targets = pick_targets(nodes, 2)
    assert len(targets) == 2
    assert len({n.node_id for n in targets}) == 2


def test_pick_targets_skips_down_and_excluded_nodes() -> None:
    nodes = [_node("dn-1", state="DOWN"), _node("dn-2"), _node("dn-3")]
    targets = pick_targets(nodes, 2, exclude=frozenset({"dn-3"}))
    assert [n.node_id for n in targets] == ["dn-2"]


def test_pick_targets_respects_holders() -> None:
    nodes = [_node("dn-1"), _node("dn-2"), _node("dn-3")]
    targets = pick_targets(nodes, 2, holders=frozenset({"dn-1"}))
    assert "dn-1" not in {n.node_id for n in targets}
    assert len(targets) == 2
