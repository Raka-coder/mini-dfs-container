from __future__ import annotations

from dataclasses import dataclass, field
import threading


@dataclass
class NodeInfo:
    node_id: str
    address: str
    state: str = "ALIVE"  # ALIVE | DOWN
    last_seen: float = 0.0  # time.monotonic() on master receipt
    used_bytes: int = 0
    physical_bytes: int = 0
    free_bytes: int = 0
    chunk_count: int = 0
    planned_bytes: int = 0  # uncommitted allocated bytes
    down_since: float = 0.0


@dataclass
class ChunkMeta:
    chunk_id: str
    file: str
    index: int
    size: int
    checksum: str
    replicas: set[str] = field(default_factory=set)  # set of node_ids
    state: str = "COMMITTED"  # UPLOADING | COMMITTED | LOST
    under_since: float = 0.0


@dataclass
class FileMeta:
    name: str
    size: int
    chunk_size: int
    file_checksum: str
    chunk_ids: list[str] = field(default_factory=list)
    status: str = "COMMITTED"
    created_at: float = 0.0


@dataclass
class UploadCtx:
    upload_id: str
    filename: str
    file_size: int
    chunk_size: int
    created_at: float
    chunks: list[dict] = field(default_factory=list)


class ClusterState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.nodes: dict[str, NodeInfo] = {}  # node_id -> NodeInfo
        self.files: dict[str, FileMeta] = {}  # filename -> FileMeta
        self.chunks: dict[str, ChunkMeta] = {}  # chunk_id -> ChunkMeta
        self.node_chunks: dict[str, set[str]] = {}  # node_id -> set of chunk_ids
        self.uploads: dict[str, UploadCtx] = {}  # upload_id -> UploadCtx
        self.rereplications_done: int = 0
