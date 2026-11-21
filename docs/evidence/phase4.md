# Evidence Phase 4 — Scalability, Benchmark Matrix, & Plots

Tanggal pengujian: 21 November 2026

## 1. Hasil Verifikasi (Host)

| No | Perintah | Hasil |
|---|---|---|
| 1 | `python -m pytest tests -q` | **9 passed** |
| 2 | `python scripts/check_no_print.py` | **CHECK_OK** |
| 3 | `python -m py_compile <modul fase 4>` | **ALL_COMPILE_OK** |
| 4 | `python scripts/gen_compose.py --nodes 3 --out /tmp/docker-compose.gen.yml` | **GENERATED ok** |
| 5 | `python scripts/gen_compose.py --nodes 16 --out /tmp/docker-compose.16.yml --dn-mem 128m --dn-cpus 0.25 --dn-workers 4 --client-workers 4` | **GENERATED ok** |
| 6 | `python scripts/plots.py` | **9 file dihasilkan** di `results/plots/` (150 DPI) |

## 2. Implementasi Per Task

- **Task 1** — `scripts/gen_compose.py`: generate `docker-compose.gen.yml` N node (2,3,4,8,16) + preset hemat N=16.
- **Task 2** — `client/bench.py`: `Bench` + `LatencyRecorder` (avg/p50/p95/p99), throughput MiB/s, duration upload/download.
- **Task 3** — `scripts/bench_matrix.py`: orchestrator Tier 1–3, output `results/bench_runs.csv`, `results/latency_<run_id>.json`, `results/SUMMARY.md`.
- **Task 4** — `scripts/plots.py`: 9 plot ke `results/plots/` (speedup_vs_workers, efficiency_vs_workers, throughput_vs_nodes, scalability_nodes, latency_per_op, tasks_per_sec, recovery_timeline, storage_per_node, access_time).

## 3. Kesimpulan

Kode Phase 4 (compose generator, benchmark harness, matrix orchestrator, plots) selesai di sisi host dan lolos verifikasi statik + smoke generation + plot generation.

Eksekusi matriks benchmark nyata (Tier 1–3 dengan cluster) menunggu runtime Podman.

Status fase: **Kode selesai; runtime benchmark pending cluster**.
