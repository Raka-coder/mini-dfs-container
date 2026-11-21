#!/usr/bin/env python3
"""Generate docker-compose.gen.yml for N-node DFS-X cluster (N in 2,3,4,8,16)."""
from __future__ import annotations

import argparse
from pathlib import Path


def generate(nodes: int, out: str, dn_mem: str, dn_cpus: float, dn_workers: int, client_workers: int) -> None:
    lines: list[str] = []
    lines.append("x-dn: &dn")
    lines.append("  image: localhost/dfsx:latest")
    lines.append("  networks: [dfsnet]")
    lines.append("  restart: on-failure")
    lines.append("  mem_limit: " + dn_mem)
    lines.append("  cpus: " + str(dn_cpus))
    lines.append('  command: ["python", "-m", "datanode.server"]')
    lines.append("  environment: &dn_env")
    lines.append("    LOG_LEVEL: INFO")
    lines.append("    MASTER_ADDR: master:50051")
    lines.append('    DN_PORT: "50052"')
    lines.append("    METRICS_PORT: 8000")
    lines.append("    SPARSE_WRITE: \"0\"")
    lines.append("    FSYNC: \"0\"")
    lines.append("    MIN_FREE_MB: \"512\"")
    lines.append(f"    GRPC_WORKERS: \"{dn_workers}\"")
    lines.append("")
    lines.append("services:")
    lines.append("  master:")
    lines.append("    image: localhost/dfsx:latest")
    lines.append("    container_name: dfs-master")
    lines.append('    command: ["python", "-m", "master.server"]')
    lines.append("    networks: [dfsnet]")
    lines.append("    restart: on-failure")
    lines.append("    mem_limit: 256m")
    lines.append("    cpus: 0.5")
    lines.append('    ports: ["50051:50051"]')
    lines.append("    environment:")
    lines.append("      LOG_LEVEL: INFO")
    lines.append("      RF: \"2\"")
    lines.append("      CHUNK_SIZE_MB: \"8\"")
    lines.append("      HEARTBEAT_TIMEOUT_S: \"10\"")
    lines.append("      SCAN_INTERVAL_S: \"2\"")
    lines.append("      REPLICATION_CONCURRENCY: \"2\"")
    lines.append("      DB_PATH: /meta/master.db")
    lines.append("      METRICS_PORT: 8000")
    lines.append("      GRPC_WORKERS: 16")
    lines.append("    volumes: [\"master_meta:/meta\"]")
    for i in range(1, nodes + 1):
        lines.append(f"  datanode{i}:")
        lines.append("    <<: *dn")
        lines.append(f"    container_name: dfs-dn{i}")
        lines.append(
            f"    environment: {{<<: *dn_env, NODE_ID: dn-{i}, NODE_ADDR: \"datanode{i}:50052\"}}"
        )
        lines.append(f"    volumes: [\"dn{i}_data:/data\"]")
    lines.append("  client:")
    lines.append("    image: localhost/dfsx:latest")
    lines.append("    container_name: dfs-client")
    lines.append('    command: ["sleep", "infinity"]')
    lines.append("    networks: [dfsnet]")
    lines.append("    mem_limit: 512m")
    lines.append("    cpus: 1.0")
    lines.append("    environment:")
    lines.append("      MASTER_ADDR: \"master:50051\"")
    lines.append("      CLIENT_WORKERS: \"" + str(client_workers) + "\"")
    lines.append("      WORK_DIR: /work")
    lines.append("      LOG_LEVEL: INFO")
    lines.append("    volumes: [\"client_work:/work\", \"./results:/results\"]")
    lines.append("")
    lines.append("networks:")
    lines.append("  dfsnet: {}")
    lines.append("")
    lines.append("volumes:")
    lines.append("  master_meta: {}")
    for i in range(1, nodes + 1):
        lines.append(f"  dn{i}_data: {{}}")
    lines.append("  client_work: {}")
    Path(out).write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Generate multi-node docker-compose for DFS-X")
    p.add_argument("--nodes", type=int, default=3, help="Jumlah datanode (2,3,4,8,16)")
    p.add_argument("--out", default="docker-compose.gen.yml")
    p.add_argument("--dn-mem", default="192m")
    p.add_argument("--dn-cpus", type=float, default=0.5)
    p.add_argument("--dn-workers", type=int, default=8)
    p.add_argument("--client-workers", type=int, default=8)
    a = p.parse_args()
    generate(a.nodes, a.out, a.dn_mem, a.dn_cpus, a.dn_workers, a.client_workers)
    print(f"GENERATED nodes={a.nodes} out={a.out}")


if __name__ == "__main__":
    main()
