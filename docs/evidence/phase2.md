# Result Tahap 2 — Core Storage Engine

**Tanggal:** 21 November 2026
**Status Kesimpulan:** Kode storage engine Phase 2 selesai di host; verifikasi runtime penuh memerlukan cluster Podman.

## 1. Ringkasan Implementasi

1. **SHA-256 & Data Generator**
   - `common/checksum.py`: `sha256_bytes`, `sha256_file`, `combine_checksums` (SHA-256 atas concat digest chunk per urutan).
   - `client/gendata.py`: generator multi-profil (`lite` sparse, `full` acak asli, `integrity`) + `manifest.json`.

2. **DataNode Chunk Storage**
   - `datanode/server.py`: `StoreChunk` (frame 1 MiB, atomic `.tmp` → `os.replace`, cek `MIN_FREE_MB`, `FSYNC`), `RetrieveChunk`, `ReplicateChunk`, `DeleteChunk`, `GetStats`.
   - `client/upload.py`: helper `chunk_bounds` (chunking 8 MiB, 50 MiB → 7 chunk).

3. **Master Metadata & Placement**
   - `master/placement.py`: `pick_targets` (anti-affinity RF=2, load balancing via `planned_bytes`).
   - `master/store.py`: `MasterStore` SQLite WAL (`files`, `chunks`, `replicas`).
   - `master/server.py`: endpoint `AllocateChunks`, `ReallocateChunk`, `CommitFile`, `GetReadLocations`, `ListFiles`, `DeleteFile` (dari placeholder `NOT_IMPLEMENTED` → implementasi penuh).

4. **Client Upload & Download**
   - `client/upload.py`: `Uploader` pipeline 2PC (Allocate → thread pool store → Commit).
   - `client/download.py`: `Downloader` (retrieve paralel, validasi checksum, failover replika, `--sink`).
   - `client/cli.py`: subcommand `upload`, `download`, `ls`, `rm`, `info`, `verify`, `gen-data`.

## 2. Hasil Verifikasi (Host)

| No | Perintah | Hasil |
|---|---|---|
| 1 | `python -m pytest tests -q` | **9 passed** (test_checksum 3, test_chunking 3, test_placement 3) |
| 2 | `python scripts/check_no_print.py` | **CHECK_OK** (tanpa `print()`) |
| 3 | `python -m py_compile <modul fase 2>` | **ALL_COMPILE_OK** |
| 4 | `python -m client.gendata --profile lite --count 2 --size 1` | **GEN_START/GEN_FILE/GEN_DONE OK** |

## 3. Unit Test Detail

- `tests/test_checksum.py`: SHA-256 bytes/file + kombinasi digest chunk — **GREEN**.
- `tests/test_chunking.py`: pembagian chunk 50 MiB → 7 chunk (terakhir 2 MiB), file kosong, file kecil — **GREEN**.
- `tests/test_placement.py`: anti-affinity RF=2, skip node down/exclude, respect holders — **GREEN**.

## 4. Catatan / Kendala

- Stub gRPC (`proto/dfs_pb2*.py`) belum bisa digenerate di sesi host ini (`grpcio-tools` gagal build di sandbox); wajib sebelum runtime master/datanode.
- Verifikasi integrasi end-to-end (upload/download real via gRPC, replika RF=2, restart persistensi) menunggu runtime Podman/WSL2.

## 5. Kesimpulan

- Ringkasan implementasi Phase 2: **SELESAI** di sisi host (kode + unit test + smoke).
- Full acceptance criteria fase 2 (upload 50 MB → 7 chunk, verifikasi replika, download + SHA-256, restart persistence) menunggu cluster live.
