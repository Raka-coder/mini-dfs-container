# Tahap 3: Fault Tolerance & Pemulihan Replikasi

## 🎯 Tujuan
Membangun mekanisme deteksi kegagalan node otomatis, antrean perbaikan replikasi mandiri (*queue-based replication manager*), sinkronisasi block report, penanganan node mati saat transfer aktif, dan pengujian skenario F1–F6.

---

## 📋 Daftar Pekerjaan (Work Items)

- [ ] **1. Master Failure Detector & Auto Recovery:**
  - `master/failure.py`: Background thread mendeteksi node `DOWN` jika selisih `monotonic() - last_seen > HEARTBEAT_TIMEOUT_S (10s)`.
  - Menandai node kembali `ALIVE` saat heartbeat diterima lagi, dengan instruksi `request_full_report=true`.
  - `master/replication.py`:
    - Repair queue berbasis `queue.Queue` untuk chunk under-replicated.
    - Worker pool eksekusi perintah `ReplicateChunk` dari datanode sumber ke target baru.
    - Mekanisme trimming over-replication setelah masa tenggang `TRIM_GRACE_S` saat node kembali online.
    - Garbage collection (GC) untuk chunk orphan.
- [ ] **2. DataNode Replikasi Antar-Node & Block Report:**
  - Implementasi RPC `ReplicateChunk` pada `datanode/server.py`: membaca chunk lokal dan men-stream ke target `StoreChunk`.
  - Peningkatan `datanode/heartbeat.py`: melampirkan daftar seluruh chunk saat start atau ketika diminta master (`full_report=true`).
- [ ] **3. Toleransi Kegagalan Jalur Upload/Download:**
  - Endpoint `ReallocateChunk` pada master: penggantian target on-the-fly jika ada replika gagal saat proses upload berlangsung.
  - Failover download: jika membaca dari replika primer gagal atau checksum mismatch, otomatis fallback membaca dari replika sekunder.
- [ ] **4. Test Suite Fault Injection:**
  - `scripts/fault_test.py`: Pengujian otomatis skenario F1 s/d F6:
    - **F1:** Matikan salah satu node saat upload file besar sedang berlangsung.
    - **F2:** Matikan node -> tunggu recovery -> matikan node lain -> download tetap sukses dari node tersisa.
    - **F3:** Nyalakan kembali node yang mati -> block report & trimming chunk berlebih.
    - **F4:** Download file saat salah satu node mati.
    - **F5:** Korupsi satu byte pada chunk di disk fisik -> verifikasi checksum error & fallback replika.
    - **F6:** Restart master node saat cluster sedang beroperasi.

---

## 🔍 Kriteria Verifikasi
1. Menjalankan `python scripts/fault_test.py --profile lite --scenarios F1,F2,F3,F4,F6` -> Semua skenario Lolos.
2. Menjalankan `python scripts/fault_test.py --profile integrity --scenarios F5` -> Lolos.
3. Log master memuat tag `NODE_DOWN`, `REREPLICATE_START`, `REREPLICATE_DONE`, dan `CLUSTER_HEALTHY`.
4. File `results/fault_summary.csv` terisi rekapitulasi waktu deteksi dan pemulihan.
5. Simpan bukti pengujian ke `docs/evidence/phase3.md`.
