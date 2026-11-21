# AGENTS.md

Agent guide for this repo. Full specs live in `docs/` — keep this file terse and pointer-heavy.

## Project

DFS-X: mini distributed file system (master + data nodes + client), Python 3.12, gRPC, Podman/WSL2 on Windows host.

- Specs: `docs/IMPEMENTATION_GUIDE.md`; decisions: `docs/DECISIONS.md`; phases: `docs/tasks/00_OVERVIEW.md`; evidence: `docs/evidence/`.

## Commands

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dfs.proto   # stubs are generated + gitignored; run before importing proto
python scripts/check_no_print.py                                                        # must pass
ruff format && ruff check
podman build -t localhost/dfsx:latest .
podman-compose -p dfsx up -d            # fallback: python scripts/cluster.py up --nodes N
podman-compose -p dfsx down -v
```

## Architecture

- One image `localhost/dfsx:latest`; entrypoints: `python -m master.server`, `python -m datanode.server`, `python -m client.cli`.
- Master: in-memory state under one RLock; authoritative clock is `time.monotonic()` on master; metadata in SQLite WAL (`DB_PATH`, default `/meta/master.db`); failure-detector + replication-queue threads.
- Datanode: chunks at `<DATA_DIR>/chunks/<xx>/<chunk_id>`; atomic write via `.tmp` + `os.replace`; stream frames of `FRAME = 1 << 20`; incremental SHA-256; refuse store if free space < `MIN_FREE_MB`.
- Client: gRPC channel cached per node; RPC deadlines (control 5 s, transfer 60 s); retry 3x on `UNAVAILABLE`/`DEADLINE_EXCEEDED`. Upload = AllocateChunks -> stream -> CommitFile; download = GetReadLocations -> stream via `os.pwrite` -> verify -> delete partial on failure.
- RF=2 with anti-affinity; chunk size 8 MiB; heartbeat 2 s / timeout 10 s; scan 2 s; master GRPC_WORKERS=16, datanode=8; client workers 8.
- Compose has no `depends_on:` conditions; start order via retry/backoff — use `python -m client.cli wait-ready --min-nodes 3` before testing.

## Hard rules (locked — changes go through `docs/DECISIONS.md`)

- No `print()`; use `logging`; CLI output via logger `dfs.out`. Enforced by `python scripts/check_no_print.py`.
- Podman, not Docker; image ref by full name `docker.io/library/python:3.12-slim`.
- Streaming 1 MiB frames; never buffer a whole file in memory.
- All RPCs have deadlines (control 5 s, transfer 60 s); channel cached per node, not per chunk.
- Config via env vars with defaults in `common/config.py`; no magic numbers in logic.
- LF line endings; type hints + short docstring for public functions; `ruff format` / `ruff check`.

## Signals

- Required log tags: `NODE_DOWN`/`NODE_UP`, `REREPLICATE_START`/`REREPLICATE_DONE`, `CLUSTER_HEALTHY`, `UPLOAD_COMMIT`, `CHUNK_STORED`, `CHECKSUM_MISMATCH`, `DISK_LOW`, `GEN_*`, `BENCH_*` (parsed by `scripts/fault_test.py`).
- Prometheus metrics prefixed `dfs_` on port 8000.

## Workflow

- One phase per session; run phase verification commands for real, save output to `docs/evidence/phaseN.md`; don't mark done without real output.

## Status

- Phase 1 DONE · Phase 2 IN PROGRESS · Phases 3–5 not started.
