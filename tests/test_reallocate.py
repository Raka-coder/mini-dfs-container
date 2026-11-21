from __future__ import annotations

from master.replication import ReplicationManager
from master.state import ChunkMeta, ClusterState, NodeInfo


def _node(nid: str, state: str = "ALIVE", used: int = 0) -> NodeInfo:
    return NodeInfo(
        node_id=nid,
        address=f"{nid}:50052",
        state=state,
        last_seen=1.0,
        used_bytes=used,
        physical_bytes=used,
        free_bytes=10**9,
        chunk_count=0,
        planned_bytes=0,
    )


def test_reallocate_uses_alive_holder_as_source() -> None:
    """Re-replication must copy from an ALIVE holder, never from an empty ALIVE node."""
    state = ClusterState()
    # dn-1 holds the chunk and has bytes on disk; dn-2 held it but is now DOWN.
    state.nodes["dn-1"] = _node("dn-1", used=4096)
    state.nodes["dn-2"] = _node("dn-2", state="DOWN")
    # dn-3 is ALIVE but holds nothing: the old bug picked it as source.
    state.nodes["dn-3"] = _node("dn-3")
    state.chunks["c1"] = ChunkMeta(
        chunk_id="c1",
        file="f.bin",
        index=0,
        size=4096,
        checksum="abc",
        replicas={"dn-1", "dn-2"},
    )

    mgr = ReplicationManager(state)
    mgr.scan()

    assert mgr.get_queue_depth() == 1
    _task_id, _cid, source_id, target_id, _checksum = mgr._queue.get_nowait()
    assert source_id == "dn-1"
    assert target_id not in {"dn-1", "dn-2"}
