# DFS-X Benchmark SUMMARY

Generated from `results/bench_runs.csv` (24 measured runs: 12 tier-1 + 12 tier-2) and
`results/latency_run1.json`. All values are from real runs — no synthetic data.

## Tier 1 — Fixed 3 nodes, profile `full` (100 files x 50 MiB), workers varied

| workers | upload MiB/s (mean) | download MiB/s (mean) | tasks/s (mean) | latency avg (s) | latency p95 (s) |
|---|---|---|---|---|---|
| 1 | 29.897 | 100.998 | 0.598 | 1.694 | 3.372 |
| 2 | 32.654 | 114.664 | 0.653 | 1.539 | 2.809 |
| 4 | 32.024 | 116.393 | 0.640 | 1.570 | 2.938 |
| 8 | 29.253 | 117.621 | 0.585 | 1.723 | 3.414 |

Speedup (upload, vs W=1): W=2 = 1.092, W=4 = 1.071, W=8 = 0.978.
Efficiency (upload, S/P): W=2 = 0.546, W=4 = 0.268, W=8 = 0.122.

Note: upload throughput saturates around 30 MiB/s regardless of workers because the
master's single-writer allocation and per-chunk RPC dominate; download scales
monotonically (101 -> 118 MiB/s) since reads are parallelised across replicas.

## Tier 2 — Scalability, profile `lite` (100 files x 5 MiB), workers = 4

| nodes | upload MiB/s (mean) | download MiB/s (mean) | tasks/s (mean) | latency avg (s) |
|---|---|---|---|---|
| 2 | 40.169 | 82.842 | 8.034 | 0.124 |
| 4 | 34.593 | 63.076 | 6.919 | 0.144 |
| 8 | 26.969 | 41.448 | 5.394 | 0.185 |
| 16 | 18.336 | 24.497 | 3.667 | 0.273 |

Speedup (upload, vs 2 nodes): 4 nodes = 0.861, 8 nodes = 0.671, 16 nodes = 0.456.
Per-file latency grows with node count (0.124 -> 0.273 s) because placement spreads
chunks across more nodes, increasing cross-node RPC fan-out on a 2 GB RAM VM.

## Latency (representative W=4, full profile, 100 files)

From `results/latency_run1.json`:
- upload: avg = 1.4195 s, p50 = 1.1745 s, p95 = 2.5822 s, p99 = 3.9535 s (n = 100)
- download: avg = 0.4197 s, p50 = 0.3871 s, p95 = 0.4102 s, p99 = 2.5741 s (n = 100)

## Fault tolerance (`results/fault_summary.csv`)

| scenario | status | duration (s) |
|---|---|---|
| F1 (stop node during upload) | PASS | 21.83 |
| F2 (auto-repair + download failover) | PASS | 63.64 |
| F3 (node rejoin & trim) | PASS | 50.56 |
| F4 (download while offline) | PASS | 5.56 |
| F5 (corrupt chunk -> checksum fallback) | PASS | 0.83 |
| F6 (master restart + WAL persistence) | PASS | 15.94 |

## Plots

7 PNGs in `results/plots/` (all > 10 KB), all data-driven:
`speedup_vs_workers.png`, `efficiency_vs_workers.png`, `throughput_vs_nodes.png`,
`scalability_nodes.png`, `tasks_per_sec.png`, `latency_per_op.png`, `recovery_timeline.png`.
