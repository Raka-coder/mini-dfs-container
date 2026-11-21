# Smoke Test Evidence — DFS-X Cluster

Tanggal Pengujian: 21 November 2026

## 1. Setup & Inisialisasi
- Cluster 1 Master + 3 DataNodes (dn-1, dn-2, dn-3) + 1 Client berjalan dalam jaringan Podman `dfsnet` (10.90.0.0/24).
- Static hostname mapping (`--add-host`) terverifikasi aktif pada seluruh node.

## 2. Pemeriksaan Kesiapan Cluster
```text
DFS-X CLUSTER STATUS (RF=2)
Files: 0 | Chunks: 0 | Under-Replicated: 0 | Lost: 0
Re-replications Completed: 0
-----------------------------------------------------------------
NODE ID    ADDRESS              STATE    CHUNKS   USED (MB)  FREE (GB) 
-----------------------------------------------------------------
dn-1       dfs-dn1:50052        ALIVE    0        0.0        952.89    
dn-2       dfs-dn2:50052        ALIVE    0        0.0        952.89    
dn-3       dfs-dn3:50052        ALIVE    0        0.0        952.89    
=================================================================
```

## 3. Eksekusi Smoke Test (Gen-Data, Upload, Download, Verify)
1. **Gen-data lite**: 100 berkas x 5 MB berhasil dibuat di `/work/testdata/`.
2. **Upload**: `podman exec dfs-client python -m client.cli upload /work/testdata/f001.bin f001.bin`
   - Hasil: `UPLOAD_COMMIT file=f001.bin chunks=1 under_replicated=0`
   - Replikasi: Ditempatkan dengan anti-affinity RF=2 di `['dn-1', 'dn-2']`.
3. **Download**: `podman exec dfs-client python -m client.cli download f001.bin /work/f001_dl.bin`
   - Hasil: `DOWNLOAD_OK file=f001.bin chunks=1`
4. **Verify**: `podman exec dfs-client python -m client.cli verify f001.bin`
   - Hasil: `DOWNLOAD_OK file=f001.bin chunks=1`
5. **SHA-256 Match**:
   - Sumber: `c036cbb7553a909f8b8877d4461924307f27ecb66cff928eeeafd569c3887e29  /work/testdata/f001.bin`
   - Unduhan: `c036cbb7553a909f8b8877d4461924307f27ecb66cff928eeeafd569c3887e29  /work/f001_dl.bin`
   - **Status: IDENTIK / PASS**
