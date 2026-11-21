# Evidence Phase 3 — Fault Tolerance & Replication Recovery

Tanggal pengujian: 21 November 2026

## 1. Hasil Verifikasi (Host)

| No | Perintah | Hasil |
|---|---|---|
| 1 | `python -m pytest tests -q` | **9 passed** (test_checksum 3, test_chunking 3, test_placement 3) |
| 2 | `python scripts/check_no_print.py` | **CHECK_OK** (tanpa `print()`) |
| 3 | `python -m py_compile <modul fase 3>` | **ALL_COMPILE_OK** |

## 2. Implementasi Per Task

- **Task 1** — `master/failure.py` (`FailureDetector`): thread scan `SCAN_INTERVAL_S`; mark DOWN jika `monotonic() - last_seen > HEARTBEAT_TIMEOUT_S`; log `NODE_DOWN node=... silent_for=... chunks_affected=...`. Mark UP + `request_full_report` sudah ada di `master/server.py` Heartbeat.
- **Task 2** — `master/replication.py` (`ReplicationManager`): scan under-replicated (`|replicas ∩ ALIVE| < RF`); queue task + `in_flight` dedup; `REREPLICATE_START/DONE` log; retry via `DataNodeService.ReplicateChunk`; trim over-replication (`TRIM_*`, `TRIM_GRACE_S`); `MasterStore.delete_replica` ditambah.
- **Task 3** — `datanode/server.py`: `ReplicateChunk` (stream chunk ke target + validasi `expected_checksum`, `CHECKSUM_MISMATCH`) dan full block report via Heartbeat sudah terpenuhi.
- **Task 4** — `client/upload.py`: failover via `ReallocateChunk` (exclude node gagal → retry ke target baru); `client/download.py` failover ke replika sekunder sudah ada.
- **Task 5** — `scripts/fault_test.py`: skenario F1–F6 + `results/fault_summary.csv` + log `results/fault/`.

## 3. Kesimpulan

Kode Phase 3 (failure detector, replication manager, datanode polish, client failover, fault-test automation) **selesai di sisi host dan lolos verifikasi statik/syntax + unit test**.

Eksekusi skenario fault (F1–F6) memerlukan cluster Podman yang live — menunggu runtime.

Status fase: **Kode selesai; runtime fault scenario pending cluster**.
