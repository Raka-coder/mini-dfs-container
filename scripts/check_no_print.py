from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path

# Add project root to sys.path so scripts can import common
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.logging_setup import setup_logging

logger = logging.getLogger("dfs.check")
TARGETS = ("master", "datanode", "client", "common")
PATTERN = re.compile(r"(?<![\w.])print\s*\(")


def main() -> int:
    setup_logging("dfs.check")
    hits: list[str] = []
    for folder in TARGETS:
        p = Path(folder)
        if not p.exists():
            continue
        for path in p.rglob("*.py"):
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                if PATTERN.search(line) and not line.lstrip().startswith("#"):
                    hits.append(f"{path}:{lineno}: {line.strip()}")
    if hits:
        for h in hits:
            logger.error("PRINT_FOUND %s", h)
        logger.error("CHECK_FAILED count=%d", len(hits))
        return 1
    logger.info("CHECK_OK no print() found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
