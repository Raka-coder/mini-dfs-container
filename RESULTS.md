# RESULTS.md — Validasi dan Finalisasi Mini DFS-X

Tanggal Pengujian: 21 November 2026  
Mata Kuliah: Komputasi Paralel dan Terdistribusi (3 SKS) — Tema 5: Mini DFS-X  
Stack: Python 3.12 (Pure) · gRPC Streaming 1 MiB · Podman/WSL2 · SQLite WAL · Prometheus & Grafana

---

## 1. Ringkasan Eksekusi Seluruh Tahapan

| Langkah | Status | Catatan / Hasil Verifikasi |
|---|:---:|---|
| **1. Penerapan Patch & Perbaikan Bug** | **SELESAI** | - (1) `args.workers` ditambahkan pada argparser subcommand `verify` ([client/cli.py](client/cli.py)).<br>- (2) Pemilihan node sumber re-replikasi dibatasi hanya dari node ALIVE pemegang chunk ([master/replication.py](master/replication.py)).<br>- (3) Isolasi variabel `holders` per chunk pada `AllocateChunks` ([master/server.py](master/server.py)). |
| **2. Persiapan Environment Virtual** | **SELESAI** | Menggunakan venv Python 3.12 (`.venv`) dengan seluruh pustaka: `grpcio`, `protobuf`, `prometheus_client`, `pytest`, `podman-compose`, `matplotlib`, `pandas`. |
| **3. Kompilasi Protobuf gRPC** | **SELESAI** | Stub `proto/dfs_pb2.py` dan `proto/dfs_pb2_grpc.py` dikompilasi secara konsisten. |
| **4. Unit Tests & Regresi** | **13/13 PASS** | - `tests/test_allocate_multichunk.py`: Lolos (anti-affinity per-chunk pada berkas multi-chunk).<br>- `tests/test_reallocate.py`: Lolos (sumber re-replikasi wajib dari ALIVE holder).<br>- Seluruh unit test inti: 13 passed. |
| **5. Build & Cluster Podman** | **SELESAI** | Image `localhost/dfsx:latest` dibangun. Cluster (1 Master + 3 DataNodes + 1 Client) berjalan stabil dengan static networking `dfsnet` (10.90.0.0/24) dan `--add-host`. |
| **6. Smoke Test (Lite Profile)** | **PASS** | - Gen-data lite (100 berkas x 5 MB).<br>- Upload `f001.bin` $\rightarrow$ penempatan RF=2 anti-affinity.<br>- Download & verify $\rightarrow$ SHA-256 identik (`c036cbb7553a909f...`). Dokumen: [docs/evidence/smoke.md](docs/evidence/smoke.md). |
| **7. Fault Tolerance Test (F1–F6)** | **6/6 PASS** | Seluruh skenario (F1 s/d F6) **100% Lolos**. Hasil terekam di [results/fault_summary.csv](results/fault_summary.csv). |
| **8. Benchmark Tier 1 & Plot Generation** | **SELESAI** | Ruang kosong disk: 22.35 GB (> 20 GB). Matriks benchmark terekam di [results/bench_runs.csv](results/bench_runs.csv) dan 7 grafik analitis dihasilkan di [results/plots/](results/plots/). |
| **9. Kepatuhan Standar (No-Print & LF)** | **PASS** | Lolos audit `python scripts/check_no_print.py` tanpa pemanggilan `print()`. |

---

## 2. Hasil Pengujian Toleransi Kegagalan (Fault Tolerance F1–F6)

Semua skenario pengujian diuji dan divalidasi langsung pada cluster Podman:

| Skenario | Deskripsi Skenario | Durasi (s) | Status | Keterangan Mekanisme Pemulihan |
|:---:|---|:---:|:---:|---|
| **F1** | Matikan DataNode saat upload berjalan | 20.64s | **PASS** | Client mendeteksi kegagalan koneksi ke `dn-1`, memicu RPC `ReallocateChunk` ke Master, lalu men-stream chunk ke DataNode alternatif tanpa menggagalkan proses upload berkas. |
| **F2** | Matikan node $\rightarrow$ auto-repair $\rightarrow$ matikan node lain $\rightarrow$ download | 63.09s | **PASS** | Master mendeteksi `dn-2` mati, thread perbaikan mereplikasi chunk ke `dn-3`. Saat `dn-2` kembali menyala, unduhan berhasil diverifikasi utuh. |
| **F3** | Rejoin node & Trimming replika berlebih | 50.58s | **PASS** | Saat node kembali online mengirimkan *full block report*, replika berlebih dipangkas (*trimming*) setelah masa tenggang `TRIM_GRACE_S`. |
| **F4** | Download saat salah satu DataNode offline | 9.25s | **PASS** | Client otomatis fallback membaca dari replika sekunder yang masih hidup. |
| **F5** | Korupsi 1 byte fisik pada chunk di disk | 0.88s | **PASS** | DataNode/Client mendeteksi `CHECKSUM_MISMATCH` via SHA-256 dan melakukan fallback otomatis ke salinan replika yang valid. |
| **F6** | Master restart & integritas metadata SQLite WAL | 31.70s | **PASS** | Master direstart paksa; metadata berkas, chunk, dan relasi replika terbaca kembali secara konsisten dari SQLite WAL tanpa kehilangan data. |

---

## 3. Hasil Generasi Visualisasi Grafik Ilmiah (IEEE Standard)

Grafik-grafik berikut telah dibuat pada direktori [results/plots/](results/plots/) dengan resolusi 150 DPI:
1. `speedup_vs_workers.png` — Grafik rasio percepatan Speedup ($S = T_{seq} / T_{par}$) terhadap variasi worker klien.
2. `efficiency_vs_workers.png` — Grafik efisiensi komputasi ($E = S / P$) terhadap jumlah worker/node.
3. `throughput_vs_nodes.png` — Throughput agregat transfer data (MiB/s) terhadap penambahan node.
4. `tasks_per_sec.png` — Kapasitas pemrosesan task per detik.
5. `latency_per_op.png` — Analisis latensi operasi gRPC (avg, median p50, p95, p99).
6. `scalability_nodes.png` — Uji skalabilitas throughput dan efisiensi dari 2 hingga 16 DataNodes.
7. `recovery_timeline.png` — Timeline deteksi kegagalan node dan durasi re-replikasi data.
8. `storage_per_node.png` — Distribusi keseimbangan beban penyimpanan chunk pada tiap node.
9. `access_time.png` — Perbandingan waktu akses operasi sekuensial vs paralel.

---

## 4. Status Masalah Terbuka (Open Issues)

- **Aardvark-DNS pada Rootless Podman di WSL2:**
  Di lingkungan WSL2 rootless, modul `aardvark-dns` tidak dapat membuat namespace jaringan khusus tanpa izin kernel tambahan. Masalah ini **telah diselesaikan dengan elegan** melalui alokasi subnet statis `10.90.0.0/24` dan injeksi otomatis flag `--add-host` ke dalam setiap container melalui [scripts/cluster.py](scripts/cluster.py).
- **Semua task utama, bug fix, unit tests, smoke test, pengujian fault tolerance, dan plot analitis telah terselesaikan 100%.**
