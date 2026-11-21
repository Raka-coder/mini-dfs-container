from __future__ import annotations

from client.upload import chunk_bounds


def test_chunk_bounds_50mb_file_produces_seven_chunks() -> None:
    bounds = chunk_bounds(50 * (1 << 20), 8 * (1 << 20))
    assert len(bounds) == 7
    assert bounds[-1] == (48 * (1 << 20), 2 * (1 << 20))
    assert sum(size for _, size in bounds) == 50 * (1 << 20)


def test_chunk_bounds_empty_file_has_no_chunks() -> None:
    assert chunk_bounds(0, 8 * (1 << 20)) == []


def test_chunk_bounds_small_file_is_single_chunk() -> None:
    assert chunk_bounds(1024, 8 * (1 << 20)) == [(0, 1024)]
