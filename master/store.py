from __future__ import annotations

import sqlite3
from pathlib import Path

from master.state import ChunkMeta, FileMeta


SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    name TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    file_checksum TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    idx INTEGER NOT NULL,
    size INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS replicas (
    chunk_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (chunk_id, node_id)
);
"""


class MasterStore:
    """SQLite WAL persistence for master metadata (files, chunks, replicas)."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self.db.commit()

    def load_all(
        self,
    ) -> tuple[dict[str, FileMeta], dict[str, ChunkMeta], dict[str, set[str]]]:
        files: dict[str, FileMeta] = {}
        for row in self.db.execute(
            "SELECT name, size, chunk_size, file_checksum, status, created_at FROM files"
        ):
            name, size, chunk_size, file_checksum, status, created_at = row
            files[name] = FileMeta(
                name=name,
                size=size,
                chunk_size=chunk_size,
                file_checksum=file_checksum,
                status=status,
                created_at=created_at,
            )
        chunks: dict[str, ChunkMeta] = {}
        for row in self.db.execute(
            "SELECT chunk_id, file_name, idx, size, checksum, state FROM chunks"
        ):
            chunk_id, file_name, idx, size, checksum, state = row
            chunks[chunk_id] = ChunkMeta(
                chunk_id=chunk_id,
                file=file_name,
                index=idx,
                size=size,
                checksum=checksum,
                state=state,
            )
        replicas: dict[str, set[str]] = {}
        for chunk_id, node_id in self.db.execute(
            "SELECT chunk_id, node_id FROM replicas"
        ):
            replicas.setdefault(chunk_id, set()).add(node_id)
        return files, chunks, replicas

    def save_file(self, fm: FileMeta) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO files (name, size, chunk_size, file_checksum, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (fm.name, fm.size, fm.chunk_size, fm.file_checksum, fm.status, fm.created_at),
        )
        self.db.commit()

    def save_chunk(self, cm: ChunkMeta) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO chunks (chunk_id, file_name, idx, size, checksum, state) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cm.chunk_id, cm.file, cm.index, cm.size, cm.checksum, cm.state),
        )
        self.db.commit()

    def save_replica(self, chunk_id: str, node_id: str) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO replicas (chunk_id, node_id) VALUES (?, ?)",
            (chunk_id, node_id),
        )
        self.db.commit()

    def delete_file(self, name: str) -> None:
        self.db.execute("DELETE FROM replicas WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE file_name = ?)", (name,))
        self.db.execute("DELETE FROM chunks WHERE file_name = ?", (name,))
        self.db.execute("DELETE FROM files WHERE name = ?", (name,))
        self.db.commit()

    def delete_replica(self, chunk_id: str, node_id: str) -> None:
        self.db.execute(
            "DELETE FROM replicas WHERE chunk_id = ? AND node_id = ?",
            (chunk_id, node_id),
        )
        self.db.commit()
