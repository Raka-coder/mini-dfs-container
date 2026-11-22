from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import grpc

from client.bench import Bench
from client.download import Downloader
from client.gendata import PROFILES
from client.gendata import generate as generate_dataset
from client.upload import Uploader
from common.config import config
from common.grpc_util import get_channel
from common.logging_setup import setup_logging
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger("dfs.cli")
out = logging.getLogger("dfs.out")


def cmd_wait_ready(args: argparse.Namespace) -> int:
    t0 = time.monotonic()
    min_nodes = args.min_nodes
    timeout = args.timeout
    channel = get_channel(args.master)
    stub = dfs_pb2_grpc.MasterServiceStub(channel)

    out.info(
        f"Waiting for cluster ready (min {min_nodes} nodes, timeout {timeout}s)..."
    )
    while time.monotonic() - t0 < timeout:
        try:
            resp = stub.ClusterStatus(dfs_pb2.StatusRequest(), timeout=3.0)
            alive = sum(1 for n in resp.nodes if n.state == "ALIVE")
            if alive >= min_nodes:
                out.info(f"Cluster READY! {alive}/{len(resp.nodes)} nodes ALIVE.")
                return 0
        except grpc.RpcError:
            pass
        time.sleep(1.0)

    out.info(f"TIMEOUT waiting for {min_nodes} nodes.")
    return 1


def cmd_status(args: argparse.Namespace) -> int:
    channel = get_channel(args.master)
    stub = dfs_pb2_grpc.MasterServiceStub(channel)
    try:
        resp = stub.ClusterStatus(dfs_pb2.StatusRequest(), timeout=5.0)
        out.info("=" * 65)
        out.info(f"DFS-X CLUSTER STATUS (RF={resp.replication})")
        out.info(
            f"Files: {resp.files} | Chunks: {resp.chunks} | Under-Replicated: {resp.under_replicated} | Lost: {resp.lost}"
        )
        out.info(f"Re-replications Completed: {resp.rereplications_done}")
        out.info("-" * 65)
        out.info(
            f"{'NODE ID':<10} {'ADDRESS':<20} {'STATE':<8} {'CHUNKS':<8} {'USED (MB)':<10} {'FREE (GB)':<10}"
        )
        out.info("-" * 65)
        for n in resp.nodes:
            used_mb = n.used_bytes / (1 << 20)
            free_gb = n.free_bytes / (1 << 30)
            out.info(
                f"{n.node_id:<10} {n.address:<20} {n.state:<8} {n.chunk_count:<8} {used_mb:<10.1f} {free_gb:<10.2f}"
            )
        out.info("=" * 65)
        return 0
    except grpc.RpcError as e:
        out.info(f"Error querying cluster status: {e}")
        return 1


def cmd_upload(args: argparse.Namespace) -> int:
    uploader = Uploader(args.master, workers=args.workers)
    ok = uploader.upload(
        local_path=args.local,
        remote=args.remote,
        chunk_size=args.chunk_mb * (1 << 20),
        force=args.force,
    )
    return 0 if ok else 1


def cmd_download(args: argparse.Namespace) -> int:
    downloader = Downloader(args.master, workers=args.workers)
    ok = downloader.download(remote=args.remote, local_path=args.local, sink=args.sink)
    return 0 if ok else 1


def cmd_ls(args: argparse.Namespace) -> int:
    stub = dfs_pb2_grpc.MasterServiceStub(get_channel(args.master))
    resp = stub.ListFiles(dfs_pb2.ListRequest(), timeout=5.0)
    for f in resp.files:
        out.info(f"{f.name} size={f.size} chunks={f.chunks} status={f.status}")
    return 0


def cmd_rm(args: argparse.Namespace) -> int:
    stub = dfs_pb2_grpc.MasterServiceStub(get_channel(args.master))
    if args.all:
        resp = stub.ListFiles(dfs_pb2.ListRequest(), timeout=5.0)
        for f in resp.files:
            d = stub.DeleteFile(dfs_pb2.DeleteRequest(filename=f.name), timeout=5.0)
            out.info(f"Deleted {f.name}: ok={d.ok}")
        return 0
    d = stub.DeleteFile(dfs_pb2.DeleteRequest(filename=args.name), timeout=5.0)
    out.info(f"Deleted {args.name}: ok={d.ok}")
    return 0 if d.ok else 1


def cmd_info(args: argparse.Namespace) -> int:
    stub = dfs_pb2_grpc.MasterServiceStub(get_channel(args.master))
    resp = stub.GetReadLocations(
        dfs_pb2.LocationsRequest(filename=args.remote), timeout=5.0
    )
    if not resp.ok:
        out.info(f"Info failed: {resp.error}")
        return 1
    for c in resp.chunks:
        out.info(
            f"{c.chunk_id} idx={c.index} size={c.size} sha256={c.checksum} replicas={[n.node_id for n in c.replicas]}"
        )
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    downloader = Downloader(args.master, workers=args.workers)
    ok = downloader.download(
        remote=args.remote, local_path=args.remote + ".verify", sink="null"
    )
    return 0 if ok else 1


def cmd_gen_data(args: argparse.Namespace) -> int:
    generate_dataset(
        Path(args.dir),
        args.profile,
        args.count or PROFILES[args.profile][0],
        args.size or PROFILES[args.profile][1],
        args.batch,
        args.min_free_mb,
    )
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    bench = Bench(args.master, workers=args.workers)
    if args.mode == "upload-all":
        res = bench.bench_upload_all(
            args.dir, args.remote_prefix, args.chunk_mb * (1 << 20), args.force
        )
    else:
        res = bench.bench_download_all(args.dir, args.remote_prefix, args.sink)
    res["timings"] = bench.latency.raw()
    res["latency"] = bench.latency.summary()
    out.info("BENCH_JSON " + json.dumps(res))
    return 0 if res["ok"] else 1


def main() -> None:
    setup_logging("dfs.cli")
    parser = argparse.ArgumentParser(prog="dfsx", description="DFS-X CLI Client")
    parser.add_argument(
        "--master", default=config.MASTER_ADDR, help="Master address (host:port)"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_wait = subparsers.add_parser(
        "wait-ready", help="Wait until cluster has min alive nodes"
    )
    p_wait.add_argument(
        "--min-nodes", type=int, default=3, help="Minimum alive nodes required"
    )
    p_wait.add_argument(
        "--timeout", type=float, default=60.0, help="Timeout in seconds"
    )
    p_wait.set_defaults(func=cmd_wait_ready)

    p_stat = subparsers.add_parser("status", help="Show cluster status")
    p_stat.set_defaults(func=cmd_status)

    p_up = subparsers.add_parser("upload", help="Upload local file to DFS")
    p_up.add_argument("local")
    p_up.add_argument("remote")
    p_up.add_argument("--workers", type=int, default=8)
    p_up.add_argument("--chunk-mb", type=int, default=8)
    p_up.add_argument("--force", action="store_true")
    p_up.set_defaults(func=cmd_upload)

    p_down = subparsers.add_parser("download", help="Download file from DFS")
    p_down.add_argument("remote")
    p_down.add_argument("local")
    p_down.add_argument("--workers", type=int, default=8)
    p_down.add_argument("--sink", default="disk", choices=["disk", "null"])
    p_down.set_defaults(func=cmd_download)

    p_ls = subparsers.add_parser("ls", help="List stored files")
    p_ls.set_defaults(func=cmd_ls)

    p_rm = subparsers.add_parser("rm", help="Delete stored file")
    p_rm.add_argument("remote", nargs="?")
    p_rm.add_argument("--all", action="store_true")
    p_rm.set_defaults(func=cmd_rm)

    p_info = subparsers.add_parser("info", help="Show chunk locations")
    p_info.add_argument("remote")
    p_info.set_defaults(func=cmd_info)

    p_verify = subparsers.add_parser("verify", help="Verify file via download-to-null")
    p_verify.add_argument("remote")
    p_verify.add_argument("--workers", type=int, default=8)
    p_verify.set_defaults(func=cmd_verify)

    p_gen = subparsers.add_parser("gen-data", help="Generate dataset")
    p_gen.add_argument("--profile", default="lite", choices=sorted(PROFILES))
    p_gen.add_argument("--count", type=int, default=0)
    p_gen.add_argument("--size", type=int, default=0)
    p_gen.add_argument("--dir", default="/work/testdata")
    p_gen.add_argument("--batch", type=int, default=0)
    p_gen.add_argument("--min-free-mb", type=int, default=1024)
    p_gen.set_defaults(func=cmd_gen_data)

    p_bench = subparsers.add_parser(
        "bench", help="Benchmark upload/download of all *.bin files"
    )
    p_bench.add_argument(
        "--mode", choices=["upload-all", "download-all"], required=True
    )
    p_bench.add_argument("--dir", default="/work/testdata")
    p_bench.add_argument("--remote-prefix", default="")
    p_bench.add_argument("--workers", type=int, default=4)
    p_bench.add_argument("--chunk-mb", type=int, default=50)
    p_bench.add_argument("--sink", default="null", choices=["disk", "null"])
    p_bench.add_argument("--force", action="store_true")
    p_bench.set_defaults(func=cmd_bench)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
