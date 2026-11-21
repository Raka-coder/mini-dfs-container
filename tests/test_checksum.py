from __future__ import annotations

import hashlib
from pathlib import Path

from common.checksum import combine_checksums, sha256_bytes, sha256_file


def test_sha256_bytes_returns_hex_digest() -> None:
    assert sha256_bytes(b"abc") == hashlib.sha256(b"abc").hexdigest()


def test_sha256_file_matches_bytes(tmp_path: Path) -> None:
    p = tmp_path / "data.bin"
    p.write_bytes(b"hello world")
    assert sha256_file(p) == hashlib.sha256(b"hello world").hexdigest()


def test_combine_checksums_hashes_concatenated_chunk_digests() -> None:
    c1 = hashlib.sha256(b"one").digest()
    c2 = hashlib.sha256(b"two").digest()
    assert combine_checksums([c1.hex(), c2.hex()]) == hashlib.sha256(c1 + c2).hexdigest()
