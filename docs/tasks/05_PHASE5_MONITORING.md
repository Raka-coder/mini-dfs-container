# Tahap 5: Monitoring Stack (Prometheus & Grafana)

## 🎯 Tujuan
Menyediakan stack observabilitas real-time cluster untuk keperluan demonstrasi visual (panel node status, under-replicated counter, I/O rates, RPC latency) dengan konfigurasi otomatis (*auto-provisioning*).

---

## 📋 Daftar Pekerjaan (Work Items)

- [ ] **1. Integrasi Exporter Prometheus:**
  - Exporter prometheus pada `master/server.py` dan `datanode/server.py` pada port `METRICS_PORT` (default 8000).
  - Metrik master: `dfs_nodes_alive`, `dfs_nodes_down`, `dfs_under_replicated_chunks`, `dfs_lost_chunks`, `dfs_rereplications_total`, `dfs_rpc_seconds`.
  - Metrik datanode: `dfs_dn_bytes_in_total`, `dfs_dn_bytes_out_total`, `dfs_dn_chunks`, `dfs_dn_used_bytes`, `dfs_dn_free_bytes`.
- [ ] **2. Konfigurasi Monitoring & Auto-provisioning:**
  - `docker-compose.monitoring.yml`: Layanan `prometheus` dan `grafana` di jaringan `dfsnet`.
  - `monitoring/prometheus.yml`: Target scrape master dan datanode.
  - `monitoring/grafana/provisioning/datasources/ds.yml`: Setup otomatis datasource Prometheus.
  - `monitoring/grafana/provisioning/dashboards/dash.yml`: Provider dashboard otomatis.
  - `monitoring/grafana/dashboards/dfsx.json`: Template dashboard memuat 7 panel wajib (Node status, Under-replicated chunks, Throughput, Storage per node, Latency p95, Re-replications, File & chunk count).
- [ ] **3. Dokumentasi Cara Menjalankan:**
  - Instruksi menjalankan cluster dengan monitoring:
    `podman-compose -p dfsx -f docker-compose.yml -f docker-compose.monitoring.yml up -d`
  - Akses Grafana tanpa login pada `http://localhost:3000`.

---

## 🔍 Kriteria Verifikasi
1. Cluster dengan monitoring berhasil di-deploy.
2. Endpoint `http://localhost:9090` (Prometheus) dan `http://localhost:3000` (Grafana) dapat diakses.
3. Dashboard DFS-X langsung tampil otomatis menampilkan data real-time dari cluster.
4. Simpan bukti tangkapan layar/log ke `docs/evidence/phase5.md`.
