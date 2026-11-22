from __future__ import annotations

import master.server as server_module
from master.server import MasterServiceImpl
from master.state import ChunkMeta, ClusterState, FileMeta, NodeInfo
from proto import dfs_pb2


def _node(nid: str) -> NodeInfo:
    return NodeInfo(node_id=nid, address=f"{nid}:50052", state="ALIVE")


def test_delete_file_frees_chunks_on_data_nodes(monkeypatch) -> None:
    """`rm --all` must delete chunk bytes, not only master metadata."""
    state = ClusterState()
    state.nodes["dn-1"] = _node("dn-1")
    state.nodes["dn-2"] = _node("dn-2")
    state.files["f.bin"] = FileMeta(
        name="f.bin", size=20, chunk_size=10, file_checksum="x", chunk_ids=["c1", "c2"]
    )
    state.chunks["c1"] = ChunkMeta(
        chunk_id="c1",
        file="f.bin",
        index=0,
        size=10,
        checksum="a",
        replicas={"dn-1", "dn-2"},
    )
    state.chunks["c2"] = ChunkMeta(
        chunk_id="c2", file="f.bin", index=1, size=10, checksum="b", replicas={"dn-1"}
    )
    state.node_chunks = {"dn-1": {"c1", "c2"}, "dn-2": {"c1"}}

    deleted: list[tuple[str, str]] = []

    class FakeStub:
        def __init__(self, address: str) -> None:
            self.address = address

        def DeleteChunk(self, request, timeout=None):
            deleted.append((self.address, request.chunk_id))
            return dfs_pb2.DeleteChunkResponse(ok=True)

    monkeypatch.setattr(server_module, "get_channel", lambda address: address)
    monkeypatch.setattr(server_module.dfs_pb2_grpc, "DataNodeServiceStub", FakeStub)

    svc = MasterServiceImpl(state)
    resp = svc.DeleteFile(dfs_pb2.DeleteRequest(filename="f.bin"), None)

    assert resp.ok and resp.chunks_deleted == 2
    assert sorted(deleted) == [
        ("dn-1:50052", "c1"),
        ("dn-1:50052", "c2"),
        ("dn-2:50052", "c1"),
    ]
    assert state.files == {}
    assert state.chunks == {}
    assert state.node_chunks["dn-1"] == set()
