from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict

import grpc

logger = logging.getLogger("dfs.grpc")

FRAME: int = 1 << 20  # 1 MiB chunk frame size

OPTIONS = [
    ("grpc.max_send_message_length", 16 * 1024 * 1024),
    ("grpc.max_receive_message_length", 16 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 10_000),
    ("grpc.keepalive_timeout_ms", 3_000),
    ("grpc.http2.max_pings_without_data", 0),
]

_channel_cache: Dict[str, grpc.Channel] = {}


def get_channel(target: str) -> grpc.Channel:
    """Return cached gRPC channel for target host:port."""
    if target not in _channel_cache:
        _channel_cache[target] = grpc.insecure_channel(target, options=OPTIONS)
    return _channel_cache[target]


def close_channels() -> None:
    for ch in _channel_cache.values():
        try:
            ch.close()
        except Exception:
            pass
    _channel_cache.clear()


def with_retry(
    rpc_func: Callable[[], Any],
    max_attempts: int = 3,
    backoff_factors: tuple[float, ...] = (0.2, 0.5, 1.0),
) -> Any:
    """Retry unary RPC on transient network failure."""
    last_err: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return rpc_func()
        except grpc.RpcError as e:
            code = e.code() if hasattr(e, "code") else None
            if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                last_err = e
                delay = backoff_factors[min(attempt, len(backoff_factors) - 1)]
                logger.warning(
                    "RPC transient error %s on attempt %d/%d; retrying in %.2fs",
                    code,
                    attempt + 1,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)
            else:
                raise
    if last_err:
        raise last_err
