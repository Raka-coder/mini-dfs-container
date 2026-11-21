from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_bytes(data: bytes) -> str:
    """SHA-256 hex digest of in-memory bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path | str) -> str:
    """SHA-256 hex digest of file contents, computed incrementally (1 MiB frames)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while frame := f.read(1 << 20):
            h.update(frame)
    return h.hexdigest()


def combine_checksums(chunk_checksums: list[str]) -> str:
    """SHA-256 over the concatenation of raw chunk digests, in chunk order."""
    h = hashlib.sha256()
    for c in chunk_checksums:
        h.update(bytes.fromhex(c))
    return h.hexdigest()
