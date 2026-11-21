from __future__ import annotations

import logging
import os
import sys


def setup_logging(name: str = "dfs") -> logging.Logger:
    """Setup standard logging across all components with strict no-print rule."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        root.addHandler(handler)
        root.setLevel(level)

    # Dedicated CLI human-readable output logger without timestamps
    cli_out = logging.getLogger("dfs.out")
    cli_out.propagate = False
    if not cli_out.handlers:
        out_handler = logging.StreamHandler(sys.stdout)
        out_handler.setFormatter(logging.Formatter("%(message)s"))
        cli_out.addHandler(out_handler)
        cli_out.setLevel(logging.INFO)

    return logging.getLogger(name)
