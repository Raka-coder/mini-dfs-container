# Panduan Demo DFS-X (Cepat: Stop → Start → Monitor)

Dokumen ini berisi urutan demo singkat tanpa `down -v` (volume & config tetap), sehingga cluster langsung ready setelah `start`.

## Prasyarat

- VM Podman sudah berjalan: `podman machine start`
- Container sudah pernah dibuat (minimal sekali `scripts/cluster.py up --nodes 3` + monitoring).
- Terminal default: **PowerShell** (bukan bash).

---

## 1. Stop (setelah demo selesai, ~5 detik)

```powershell
podman stop dfs-grafana dfs-prometheus dfs-client dfs-dn1 dfs-dn2 dfs-dn3 dfs-master
```

## 2. Start (saat demo mulai, ~20 detik)

```powershell
podman start dfs-master dfs-dn1 dfs-dn2 dfs-dn3 dfs-client dfs-prometheus dfs-grafana
Start-Sleep -Seconds 20
```

## 3. Verifikasi cluster (5 detik)

```powershell
podman ps --format "table {{.Names}}\t{{.Status}}"
podman exec dfs-client python -m client.cli wait-ready --min-nodes 3 --timeout 60
podman exec dfs-client python -m client.cli status
```

**Output yang diharapkan:**

```
dfs-master   Up
dfs-dn1      Up
dfs-dn2      Up
dfs-dn3      Up
dfs-client   Up
Cluster READY! 3/3 nodes ALIVE.
```

## 4. Verifikasi monitoring (5 detik)

```powershell
podman exec dfs-prometheus wget -qO- http://localhost:9090/-/healthy
podman exec dfs-grafana wget -qO- http://localhost:3000/api/health
```

## 5. Buka akses dari host (SSH tunnel)

Terminal terpisah (biarkan terbuka selama demo):

```powershell
ssh -i "$env:USERPROFILE\.local\share\containers\podman\machine\machine" -p 54345 `
  -o StrictHostKeyChecking=no -o UserKnownHostsFile=NUL `
  -L 3000:10.90.0.31:3000 -L 9090:10.90.0.30:9090 -N user@localhost
```

Lalu buka di browser:
- **Grafana:** http://localhost:3000 (anonymous Admin, dashboard `DFS-X`)
- **Prometheus:** http://localhost:9090 (targets 4/4 UP)

## 6. Bukti cepat (opsional, untuk video)

```powershell
# Counter realistis (bukan miliaran)
podman exec dfs-prometheus wget -qO- "http://localhost:9090/api/v1/query?query=dfs_nodes_alive"

# Status node
podman exec dfs-client python -m client.cli status

# Hasil benchmark (sudah ada, tidak perlu run ulang)
Get-Content results\bench_runs.csv
Get-Content results\fault_summary.csv
```

## Catatan

- **Jangan pakai `down -v`** untuk demo — itu hapus volume (data & config), butuh rebuild + gen-data lagi (~2 menit).
- `stop`/`start` mempertahankan volume, jadi cluster langsung ready setelah start (heartbeat ~10–15 detik).
- Kalau container sudah di-`rm` (terhapus), pakai cold start: `scripts/cluster.py up --nodes 3`.
- Tunnel SSH harus tetap terbuka selama akses browser.
- `podman machine start` harus dijalankan dulu jika VM mati.

## Urutan demo yang disarankan (total ~30 detik)

```powershell
# Terminal 1
podman start dfs-master dfs-dn1 dfs-dn2 dfs-dn3 dfs-client dfs-prometheus dfs-grafana
Start-Sleep 20
podman ps
podman exec dfs-client python -m client.cli status

# Terminal 2 (biarkan terbuka)
ssh -i "$env:USERPROFILE\.local\share\containers\podman\machine\machine" -p 54345 -o StrictHostKeyChecking=no -o UserKnownHostsFile=NUL -L 3000:10.90.0.31:3000 -L 9090:10.90.0.30:9090 -N user@localhost

# Browser: http://localhost:3000 dan http://localhost:9090
```
