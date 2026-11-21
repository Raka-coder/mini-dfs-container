from __future__ import annotations

from master.server import MasterServiceImpl
from master.state import ClusterState, NodeInfo
from proto import dfs_pb2

CHUNK = 1 << 20


def _node(nid: str) -> NodeInfo:
    return NodeInfo(
        node_id=nid,
        address=f"{nid}:50052",
        state="ALIVE",
        last_seen=1.0,
        used_bytes=0,
        physical_bytes=0,
        free_bytes=10**9,
        chunk_count=0,
        planned_bytes=0,
    )


def test_allocate_multichunk_each_chunk_gets_rf_targets() -> None:
    """Anti-affinity is per-chunk: every chunk of a 3-chunk file must get RF=2 targets."""
    state = ClusterState()
    for nid in ("dn-1", "dn-2", "dn-3"):
        state.nodes[nid] = _node(nid)
    svc = MasterServiceImpl(state)

    resp = svc.AllocateChunks(
        dfs_pb2.AllocateRequest(
            filename="big.bin", file_size=3 * CHUNK, chunk_size=CHUNK
        ),
        None,
    )

    assert resp.ok
    assert len(resp.chunks) == 3
    for plan in resp.chunks:
        ids = [t.node_id for t in plan.targets]
        assert len(ids) == 2, f"chunk {plan.index} got {ids}"
        assert len(set(ids)) == 2, f"chunk {plan.index} has duplicate targets {ids}"
