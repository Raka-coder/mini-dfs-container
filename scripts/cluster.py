#!/usr/bin/env python3
"""Cluster orchestrator for DFS-X across platforms / WSL2.

Directly manages containers via podman commands inside podman-machine-default.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.logging_setup import setup_logging
import logging

logger = logging.getLogger("dfs.cluster")


def run_podman(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["wsl", "-d", "podman-machine-default", "-u", "root", "podman"] + args
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def cmd_build(args: argparse.Namespace) -> int:
    logger.info("BUILD_START image=localhost/dfsx:latest")
    # Tar source to pipe into podman build in WSL
    import io
    import tarfile

    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as tar:
        for folder in ["proto", "common", "master", "datanode", "client"]:
            p = Path(folder)
            if p.exists():
                for f in p.rglob("*"):
                    if not f.name.endswith(".pyc") and "__pycache__" not in str(f):
                        tar.add(f, arcname=str(f).replace("\\", "/"))
        for top_f in ["requirements.txt", "requirements-build.txt", "Dockerfile"]:
            if Path(top_f).exists():
                tar.add(top_f, arcname=top_f)

    tar_bytes = tar_buf.getvalue()
    cmd = ["wsl", "-d", "podman-machine-default", "-u", "root", "podman", "build", "-t", "localhost/dfsx:latest", "-"]
    proc = subprocess.run(cmd, input=tar_bytes, text=False, capture_output=True)
    if proc.returncode == 0:
        logger.info("BUILD_SUCCESS image=localhost/dfsx:latest")
        return 0
    else:
        logger.error("BUILD_FAILED err=%s", proc.stderr.decode(errors="replace"))
        return proc.returncode


def cmd_up(args: argparse.Namespace) -> int:
    nodes = args.nodes
    logger.info("CLUSTER_UP nodes=%d", nodes)
    # 1. Network
    run_podman(["network", "create", "dfsnet"], check=False)

    # 2. Volumes
    run_podman(["volume", "create", "master_meta"], check=False)
    run_podman(["volume", "create", "client_work"], check=False)
    for i in range(1, nodes + 1):
        run_podman(["volume", "create", f"dn{i}_data"], check=False)

    # 3. Master container
    run_podman(["rm", "-f", "dfs-master"], check=False)
    run_podman([
        "run", "-d",
        "--name", "dfs-master",
        "--network", "dfsnet",
        "--memory", "256m",
        "--cpus", "0.5",
        "-p", "50051:50051",
        "-e", "LOG_LEVEL=INFO",
        "-e", "RF=2",
        "-e", "CHUNK_SIZE_MB=8",
        "-e", "HEARTBEAT_TIMEOUT_S=10",
        "-e", "SCAN_INTERVAL_S=2",
        "-e", "REPLICATION_CONCURRENCY=2",
        "-e", "DB_PATH=/meta/master.db",
        "-e", "METRICS_PORT=8000",
        "-e", "GRPC_WORKERS=16",
        "-v", "master_meta:/meta",
        "localhost/dfsx:latest",
        "python", "-m", "master.server"
    ])

    # 4. DataNodes
    for i in range(1, nodes + 1):
        dn_name = f"dfs-dn{i}"
        run_podman(["rm", "-f", dn_name], check=False)
        run_podman([
            "run", "-d",
            "--name", dn_name,
            "--network", "dfsnet",
            "--memory", args.dn_mem,
            "--cpus", str(args.dn_cpus),
            "-e", "LOG_LEVEL=INFO",
            "-e", "MASTER_ADDR=dfs-master:50051",
            "-e", "DN_PORT=50052",
            "-e", "METRICS_PORT=8000",
            "-e", "SPARSE_WRITE=0",
            "-e", "FSYNC=0",
            "-e", "MIN_FREE_MB=512",
            "-e", f"GRPC_WORKERS={args.dn_workers}",
            "-e", f"NODE_ID=dn-{i}",
            "-e", f"NODE_ADDR={dn_name}:50052",
            "-v", f"dn{i}_data:/data",
            "localhost/dfsx:latest",
            "python", "-m", "datanode.server"
        ])

    # 5. Client
    run_podman(["rm", "-f", "dfs-client"], check=False)
    run_podman([
        "run", "-d",
        "--name", "dfs-client",
        "--network", "dfsnet",
        "--memory", "512m",
        "--cpus", "1.0",
        "-e", "MASTER_ADDR=dfs-master:50051",
        "-e", f"CLIENT_WORKERS={args.client_workers}",
        "-e", "WORK_DIR=/work",
        "-e", "LOG_LEVEL=INFO",
        "-v", "client_work:/work",
        "localhost/dfsx:latest",
        "sleep", "infinity"
    ])

    logger.info("CLUSTER_UP_DONE nodes=%d", nodes)
    return 0


def cmd_down(args: argparse.Namespace) -> int:
    logger.info("CLUSTER_DOWN delete_volumes=%s", args.volumes)
    # Stop & rm containers
    proc = run_podman(["ps", "-a", "--filter", "name=dfs-", "--format", "{{.Names}}"], check=False)
    names = proc.stdout.strip().split()
    for name in names:
        if name:
            run_podman(["rm", "-f", name], check=False)

    if args.volumes:
        vproc = run_podman(["volume", "ls", "--format", "{{.Name}}"], check=False)
        for v in vproc.stdout.strip().split():
            if v.startswith("dn") or v in ("master_meta", "client_work"):
                run_podman(["volume", "rm", "-f", v], check=False)
    return 0


def cmd_ps(args: argparse.Namespace) -> int:
    res = run_podman(["ps", "--filter", "name=dfs-"])
    sys.stdout.write(res.stdout)
    return res.returncode


def cmd_exec(args: argparse.Namespace) -> int:
    cmd = ["wsl", "-d", "podman-machine-default", "-u", "root", "podman", "exec", args.container] + args.cmd
    res = subprocess.run(cmd)
    return res.returncode


def main() -> None:
    setup_logging("dfs.cluster")
    parser = argparse.ArgumentParser(description="DFS-X Cluster Manager")
    sub = parser.add_subparsers(dest="action", required=True)

    # build
    sub.add_parser("build")

    # up
    p_up = sub.add_parser("up")
    p_up.add_argument("--nodes", type=int, default=3)
    p_up.add_argument("--dn-mem", default="192m")
    p_up.add_argument("--dn-cpus", type=float, default=0.5)
    p_up.add_argument("--dn-workers", type=int, default=8)
    p_up.add_argument("--client-workers", type=int, default=8)

    # down
    p_down = sub.add_parser("down")
    p_down.add_argument("-v", "--volumes", action="store_true")

    # ps
    sub.add_parser("ps")

    # exec
    p_ex = sub.add_parser("exec")
    p_ex.add_argument("container")
    p_ex.add_argument("cmd", nargs=argparse.REMAINDER)

    args = parser.parse_args()
    if args.action == "build":
        sys.exit(cmd_build(args))
    elif args.action == "up":
        sys.exit(cmd_up(args))
    elif args.action == "down":
        sys.exit(cmd_down(args))
    elif args.action == "ps":
        sys.exit(cmd_ps(args))
    elif args.action == "exec":
        sys.exit(cmd_exec(args))


if __name__ == "__main__":
    main()
