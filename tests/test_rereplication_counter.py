from __future__ import annotations

import master.server as server_module
from master.server import MasterServiceImpl
from master.state import ClusterState
from proto import dfs_pb2


class _FakeCounter:
    def __init__(self) -> None:
        self.value = 0.0

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount


def test_rereplication_counter_uses_delta_not_cumulative(monkeypatch) -> None:
    """ClusterStatus must not re-add the cumulative rereplications_done each call."""
    counter = _FakeCounter()
    monkeypatch.setattr(server_module, "dfs_rereplications_total", counter)

    state = ClusterState()
    state.rereplications_done = 0
    svc = MasterServiceImpl(state)

    state.rereplications_done = 3
    svc.ClusterStatus(dfs_pb2.StatusRequest(), None)
    assert counter.value == 3.0, f"expected 3 after first report, got {counter.value}"

    # No new re-replications: counter must NOT grow (this is the bug).
    svc.ClusterStatus(dfs_pb2.StatusRequest(), None)
    assert counter.value == 3.0, f"counter inflated to {counter.value} on idle call"

    state.rereplications_done = 5
    svc.ClusterStatus(dfs_pb2.StatusRequest(), None)
    assert counter.value == 5.0, f"expected 5 after delta, got {counter.value}"