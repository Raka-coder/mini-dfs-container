# Tahap 2: Mesin Penyimpanan Inti & Operasi I/O

## 🎯 Tujuan
Mengimplementasikan penyimpanan chunk atomik, chunking 8 MiB, replikasi RF=2 anti-affinity, upload/download paralel dengan SHA-256, persistensi metadata SQLite WAL, dan generator dataset.

---

## 📋 Daftar Pekerjaan (Work Items)

- [ ] **1. SHA-256 & Data Generator:**
  - `common/checksum.py`: Helper SHA-256 frame, stream, file, dan kombinasi chunk checksums.
  - `client/gendata.py`: Dataset generator multi-profil (`lite` sparse, `full` data acak asli `os.urandom`, `integrity`) beserta pembuatan `manifest.json`.
- [ ] **2. DataNode Chunk Storage Engine:**
  - `datanode/chunkstore.py`:
    - Simpan chunk per frame 1 MiB atomik (`.tmp` -> `os.replace()`).
    - Cek batas minimum ruang bebas disk (`MIN_FREE_MB`).
    - Baca chunk secara streaming 1 MiB per frame (`RetrieveChunk`).
    - Scan awal direktori `/data/chunks` saat boot untuk inisialisasi counter bytes & chunks.
- [ ] **3. Master Metadata & Placement Engine:**
  - `master/store.py`: Persistensi metadata SQLite WAL (`files`, `chunks`, `replicas`).
  - `master/placement.py`: Algoritma pemilihan target replika chunk (`pick_targets`) dengan anti-affinity (RF=2) dan load balancing via `planned_bytes`.
  - Endpoint `AllocateChunks`, `CommitFile`, `GetReadLocations`, `ListFiles`, `DeleteFile`.
- [ ] **4. Client Upload & Download Pipelines:**
  - `client/upload.py`: 2-phase commit pipeline via multi-threaded worker pool (chunking 8 MiB, SHA-256 lokal, transfer gRPC streaming, verifikasi checksum kembalian, commit).
  - `client/download.py`: Multi-threaded chunk retrieval via `os.pwrite` langsung ke offset tujuan, validasi SHA-256 per chunk dan per file, failover ke replika sekunder jika primer gagal.
  - CLI subcommand: `upload`, `download`, `ls`, `rm`, `info`, `verify`, `gen-data`.
- [ ] **5. Sanity Unit Tests (Host):**
  - `tests/test_placement.py`: Verifikasi RF=2 anti-affinity dan distribusi beban.
  - `tests/test_chunking.py`: Verifikasi pembagian chunk 50 MiB -> 7 chunk (terakhir 2 MiB).
  - `tests/test_checksum.py`: Determinisme kalkulasi SHA-256 gabungan.

---

## 🔍 Kriteria Verifikasi
1. Menjalankan `python -m pytest tests -q` di host -> Lolos.
2. Generate file profil `integrity`, upload, verifikasi info replika (tiap chunk memiliki 2 replika di node berbeda).
3. Download file dan verifikasi SHA-256 file output identik dengan file sumber.
4. Restart container master (`podman restart dfs-master`), verifikasi daftar file (`cli ls`) tidak hilang (persisten).
5. Generate file 50 MB, upload, dan verifikasi info chunk berjumlah 7 chunk (terakhir 2 MiB).
6. Simpan bukti pengujian ke `docs/evidence/phase2.md`.
