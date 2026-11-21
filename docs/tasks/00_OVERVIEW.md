# DFS-X Task Overview & Status Tracker

Dokumen ini memetakan perkembangan implementasi teknis untuk proyek **DFS-X Terminal** (Mini Distributed File System berbasis CLI, gRPC, dan Podman).

---

## 📌 Status Tahapan

| Tahap | Dokumen Modul | Deskripsi | Status |
|---|---|---|:---:|
| **Tahap 1** | [01_PHASE1_FOUNDATION.md](file:///C:/dfs-x/docs/tasks/01_PHASE1_FOUNDATION.md) | Fondasi, Proto gRPC, Common Config/Logging, Dockerfile, Compose, Heartbeat | ✅ DONE |
| **Tahap 2** | [02_PHASE2_CORE_STORAGE.md](file:///C:/dfs-x/docs/tasks/02_PHASE2_CORE_STORAGE.md) | Chunk Storage Atomik, 2PC Placement (RF=2), Upload/Download, SHA-256, SQLite WAL | ⏳ In Progress |
| **Tahap 3** | [03_PHASE3_FAULT_RECOVERY.md](file:///C:/dfs-x/docs/tasks/03_PHASE3_FAULT_RECOVERY.md) | Failure Detector, Queue-based Re-replication, Scenarios F1–F6 | ⬜ Not Started |
| **Tahap 4** | [04_PHASE4_BENCH_TESTING.md](file:///C:/dfs-x/docs/tasks/04_PHASE4_BENCH_TESTING.md) | Skalabilitas 2–16 Node, Bench Matrix (Tier 1–3), Plot Visualisasi Matplotlib | ⬜ Not Started |
| **Tahap 5** | [05_PHASE5_MONITORING.md](file:///C:/dfs-x/docs/tasks/05_PHASE5_MONITORING.md) | Prometheus Exporter & Grafana Auto-provisioning Dashboard (Bonus) | ⬜ Not Started |

---

## 🛡️ Aturan Keras Pengembangan
1. **Logging, bukan `print()`**: Seluruh kode memakai modul `logging`. Keluaran CLI memakai logger `dfs.out`. Lolos `python scripts/check_no_print.py`.
2. **Podman Native**: Semua perintah cluster memakai `podman` dan `podman-compose`.
3. **gRPC Streaming**: Chunk ditransmisikan dalam frame 1 MiB (`FRAME = 1 << 20`). Jangan buffer seluruh file ke memori.
4. **Data Nyata**: Volume penyimpanan data chunk menggunakan named volume (`dnX_data`), bukan bind-mount Windows.
5. **LF Line Endings**: Gunakan LF untuk seluruh file `.py`, `.proto`, `.yml`, `.dockerfile`.
