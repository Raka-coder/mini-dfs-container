from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
from pathlib import Path

from common.logging_setup import setup_logging

logger = logging.getLogger("dfs.gendata")

FRAME = 1 << 20  # 1 MiB
PROFILES: dict[str, tuple[int, int, bool]] = {
    # profile: (count, size_mb, sparse)
    "lite": (100, 5, True),
    "full": (100, 50, False),
    "integrity": (5, 20, False),
}


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while frame := f.read(FRAME):
            h.update(frame)
    return h.hexdigest()


def make_sparse(path: Path, size: int) -> None:
    """File logis `size` byte, fisik ~0 (lubang filesystem)."""
    with path.open("wb") as f:
        f.truncate(size)


def make_random(path: Path, size: int) -> None:
    """File berisi data acak asli, ditulis per frame 1 MiB."""
    remaining = size
    with path.open("wb") as f:
        while remaining > 0:
            n = min(FRAME, remaining)
            f.write(os.urandom(n))
            remaining -= n


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def generate(
    out_dir: Path,
    profile: str,
    count: int,
    size_mb: int,
    batch: int,
    min_free_mb: int,
) -> dict:
    """Bangkitkan dataset + manifest. Kembalikan dict manifest."""
    sparse = PROFILES[profile][2]
    size = size_mb * (1 << 20)
    out_dir.mkdir(parents=True, exist_ok=True)

    need = count * size
    if not sparse:
        avail = free_bytes(out_dir)
        target = need if batch == 0 else min(need, batch * size)
        if avail < target + min_free_mb * (1 << 20):
            logger.error(
                "DISK_LOW profile=%s need=%.1fGiB free=%.1fGiB batch=%d",
                profile,
                target / 2**30,
                avail / 2**30,
                batch,
            )
            raise SystemExit(2)

    logger.info(
        "GEN_START profile=%s count=%d size_mb=%d sparse=%s batch=%d logical=%.2fGiB",
        profile,
        count,
        size_mb,
        sparse,
        batch,
        need / 2**30,
    )

    entries: list[dict] = []
    for i in range(1, count + 1):
        name = f"f{i:03d}.bin"
        path = out_dir / name
        if sparse:
            make_sparse(path, size)
        else:
            make_random(path, size)
        digest = _sha256_path(path)
        entries.append({"name": name, "size": size, "sha256": digest})
        logger.info("GEN_FILE name=%s bytes=%d sha256=%s", name, size, digest[:16])

        if batch and i % batch == 0:
            logger.info("GEN_BATCH_DONE generated=%d/%d", i, count)

    manifest = {
        "profile": profile,
        "count": count,
        "size_mb": size_mb,
        "sparse": sparse,
        "total_bytes": need,
        "files": entries,
    }
    mpath = out_dir / "manifest.json"
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("GEN_DONE profile=%s files=%d manifest=%s", profile, count, mpath)
    return manifest


def main() -> None:
    setup_logging("dfs.gendata")
    p = argparse.ArgumentParser(description="Generate dataset DFS-X")
    p.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    p.add_argument("--count", type=int, default=0, help="0 = default profil")
    p.add_argument("--size", type=int, default=0, help="MB; 0 = default profil")
    p.add_argument("--dir", default="/work/testdata")
    p.add_argument("--batch", type=int, default=0,
                   help="generate bertahap K file (hemat disk); 0 = sekaligus")
    p.add_argument("--min-free-mb", type=int, default=1024)
    a = p.parse_args()

    d_count, d_size, _ = PROFILES[a.profile]
    generate(Path(a.dir), a.profile, a.count or d_count, a.size or d_size,
             a.batch, a.min_free_mb)


if __name__ == "__main__":
    main()
