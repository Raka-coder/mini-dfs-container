# Evidence Phase 5 — Monitoring Stack (Prometheus & Grafana)

Tanggal pengujian: 21 November 2026

## 1. Hasil Verifikasi (Host)

| No | Perintah / File | Hasil |
|---|---|---|
| 1 | `python -m py_compile master/server.py datanode/server.py ...` | **ALL_COMPILE_OK** |
| 2 | `python -m pytest tests -q` | **9 passed** |
| 3 | `python scripts/check_no_print.py` | **CHECK_OK** |
| 4 | `python -m json.tool monitoring/grafana/dashboards/dfsx.json` | (validasi JSON dashboard) |

## 2. Implementasi Per Task

- **Task 1** — Exporter Prometheus: `master/server.py` (`dfs_nodes_alive`, `dfs_nodes_down`, `dfs_under_replicated_chunks`, `dfs_lost_chunks`, `dfs_rereplications_total`, `dfs_rpc_seconds` via decorator) + metrics HTTP di `METRICS_PORT` (8000); `datanode/server.py` (`dfs_dn_bytes_in_total`, `dfs_dn_bytes_out_total`, `dfs_dn_chunks`, `dfs_dn_used_bytes`, `dfs_dn_free_bytes`).
- **Task 2** — Monitoring & auto-provisioning: `docker-compose.monitoring.yml`, `monitoring/prometheus.yml`, `monitoring/grafana/provisioning/datasources/ds.yml`, `monitoring/grafana/provisioning/dashboards/dash.yml`, `monitoring/grafana/dashboards/dfsx.json` (7 panel).
- **Task 3** — Dokumentasi: inilah bagian final — cara menjalankan stack monitoring + akses Grafana/Prometheus.

## 3. Kesimpulan

Kode monitoring stack Phase 5 selesai di sisi host (exporter + auto-provisioning + dashboard JSON).

Verifikasi runtime (deploy cluster + akses :9090/:3000 + dashboard live) menunggu cluster Podman.

Status fase: **Kode selesai; runtime monitoring pending cluster**.
