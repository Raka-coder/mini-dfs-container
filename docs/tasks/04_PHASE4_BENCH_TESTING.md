# Tahap 4: Skalabilitas, Matriks Benchmark, & Generator Plot

## 🎯 Tujuan
Membangun generator topologi multi-node (hingga 16 node), mengorkestrasi eksekusi matriks benchmark komputasi paralel (Tier 1–3), mencatat metrik latensi inter-node dan throughput, serta menghasilkan grafik visualisasi analitis.

---

## 📋 Daftar Pekerjaan (Work Items)

- [ ] **1. Generator Compose Multi-Node:**
  - `scripts/gen_compose.py`: Menghasilkan `docker-compose.gen.yml` untuk jumlah node N = 2, 3, 4, 8, dan 16.
  - Preset mode hemat memori untuk N=16: `--dn-mem 128m --dn-cpus 0.25 --dn-workers 4 --client-workers 4`.
- [ ] **2. Client Benchmark Harness:**
  - `client/bench.py`: Eksekutor benchmark internal container client, mengukur durasi upload dan download, throughput file/detik (task/s) dan MiB/s, serta pembersihan otomatis antar-run.
  - Pencatatan latensi komunikasi gRPC via interceptor client (`avg`, `p50`, `p95`, `p99`).
- [ ] **3. Matriks Orkestrator Otomatis:**
  - `scripts/bench_matrix.py`:
    - **Tier 1 (Wajib - Headline):** Profil `full` (100 file × 50 MB = 5 GB data acak asli), N=3 node, variasi worker thread W ∈ {1, 2, 4, 8}.
    - **Tier 2 (Wajib - Skalabilitas):** Profil konsisten `lite`, worker W=4 tetap, variasi node N ∈ {2, 3, 4, 8, 16}.
    - **Tier 3 (Opsional):** Validasi silang profil `full` pada N ∈ {2, 4}.
  - Menghasilkan output terstruktur:
    - `results/bench_runs.csv`
    - `results/latency_<run_id>.json`
    - `results/efficiency_table.csv` & `results/efficiency_table.md`
    - `results/amdahl.csv`
    - `results/SUMMARY.md`
- [ ] **4. Generator Plot Matplotlib:**
  - `scripts/plots.py`: Membuat 9 berkas grafik resolusi 150 DPI di `results/plots/`:
    1. `speedup_vs_workers.png`
    2. `efficiency_vs_workers.png`
    3. `throughput_vs_nodes.png`
    4. `scalability_nodes.png`
    5. `latency_per_op.png`
    6. `tasks_per_sec.png`
    7. `recovery_timeline.png`
    8. `storage_per_node.png`
    9. `access_time.png`

---

## 🔍 Kriteria Verifikasi
1. Eksekusi `python scripts/bench_matrix.py --tier 1` selesai tanpa kegagalan transfer.
2. Eksekusi `python scripts/bench_matrix.py --tier 2` menguji hingga N=16 node.
3. Eksekusi `python scripts/plots.py` menghasilkan seluruh 9 file grafik di `results/plots/`.
4. Tabel efisiensi dan speedup terisi dengan pemisahan jelas antara efisiensi prosesor/node dan efisiensi thread worker.
5. Simpan bukti pengujian ke `docs/evidence/phase4.md`.
