# Evidence Phase 1 — Foundation & Cluster Setup

Tanggal pengujian: 21 November 2026

## 1. Lingkungan Runtime
- Container Engine: Podman 6.0.2 (WSL2 rootless container runtime)
- Base Image: `localhost/dfsx:latest` (Python 3.12-slim multi-stage build)
- Arsitektur: 1 Master Node + 3 DataNodes + 1 Client Container

## 2. Pemeriksaan Anti-Print (No print() rule)
```
$ python scripts/check_no_print.py
2026-11-21 11:28:38 INFO [dfs.check] CHECK_OK no print() found
```
Status: **PASSED (0 print() ditemukan di kode produksi)**

## 3. Verifikasi Status Cluster
Output dari container `dfs-client`:
```
Waiting for cluster ready (min 3 nodes, timeout 30.0s)...
Cluster READY! 3/3 nodes ALIVE.

=================================================================
DFS-X CLUSTER STATUS (RF=2)
Files: 0 | Chunks: 0 | Under-Replicated: 0 | Lost: 0
Re-replications Completed: 0
-----------------------------------------------------------------
NODE ID    ADDRESS              STATE    CHUNKS   USED (MB)  FREE (GB) 
-----------------------------------------------------------------
dn-1       dfs-dn1:50052        ALIVE    0        0.0        953.01    
dn-2       dfs-dn2:50052        ALIVE    0        0.0        953.01    
dn-3       dfs-dn3:50052        ALIVE    0        0.0        953.01    
=================================================================
```

## 4. Kesimpulan Kriteria Tahap 1
- [x] Kontrak gRPC proto `dfs.proto` terkompilasi
- [x] Logging terstandar menggunakan modul `logging`
- [x] Master node menerima heartbeat tiap 2 detik
- [x] 3 DataNode (`dn-1`, `dn-2`, `dn-3`) berstatus `ALIVE`
- [x] Client CLI `wait-ready` dan `status` berfungsi 100%
