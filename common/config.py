from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8000"))
    MASTER_PORT: int = int(os.getenv("MASTER_PORT", "50051"))
    RF: int = int(os.getenv("RF", "2"))
    CHUNK_SIZE_MB: int = int(os.getenv("CHUNK_SIZE_MB", "8"))
    HEARTBEAT_TIMEOUT_S: float = float(os.getenv("HEARTBEAT_TIMEOUT_S", "10.0"))
    SCAN_INTERVAL_S: float = float(os.getenv("SCAN_INTERVAL_S", "2.0"))
    REPLICATION_CONCURRENCY: int = int(os.getenv("REPLICATION_CONCURRENCY", "2"))
    TRIM_GRACE_S: float = float(os.getenv("TRIM_GRACE_S", "30.0"))
    UPLOAD_TTL_S: float = float(os.getenv("UPLOAD_TTL_S", "600.0"))
    DB_PATH: str = os.getenv("DB_PATH", "/meta/master.db")
    MIN_FREE_BYTES: int = int(os.getenv("MIN_FREE_BYTES", "536870912"))  # 512 MiB

    NODE_ID: str = os.getenv("NODE_ID", "")
    NODE_ADDR: str = os.getenv("NODE_ADDR", "")
    MASTER_ADDR: str = os.getenv("MASTER_ADDR", "master:50051")
    DN_PORT: int = int(os.getenv("DN_PORT", "50052"))
    DATA_DIR: str = os.getenv("DATA_DIR", "/data")
    HEARTBEAT_INTERVAL_S: float = float(os.getenv("HEARTBEAT_INTERVAL_S", "2.0"))
    SPARSE_WRITE: int = int(os.getenv("SPARSE_WRITE", "0"))
    FSYNC: int = int(os.getenv("FSYNC", "0"))
    MIN_FREE_MB: int = int(os.getenv("MIN_FREE_MB", "512"))

    GRPC_WORKERS: int = int(os.getenv("GRPC_WORKERS", "8"))
    CLIENT_WORKERS: int = int(os.getenv("CLIENT_WORKERS", "8"))
    GRPC_COMPRESSION: str = os.getenv("GRPC_COMPRESSION", "none")
    WORK_DIR: str = os.getenv("WORK_DIR", "/work")


config = Config()
