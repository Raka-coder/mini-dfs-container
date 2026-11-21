# Tahap 1: Fondasi, Komunikasi gRPC, & Lingkungan Cluster

## 🎯 Tujuan
Menyiapkan dasar proyek, kontrak gRPC, konfigurasi terpusat via dataclass, logging terstandarisasi, linter anti-`print()`, multi-stage Dockerfile, definisi `docker-compose.yml` (3 datanode + master + client), serta mekanisme heartbeat awal.

---

## 📋 Daftar Pekerjaan (Work Items)

- [x] **1. File Pengaturan Proyek:**
  - `.gitattributes` (LF normalization)
  - `.gitignore` (ignore build artifacts, `.venv`, `results/`, `testdata/`, `*.bin`, stub gRPC)
  - `.dockerignore`
  - `.editorconfig`
  - `requirements.txt` (`grpcio==1.66.1`, `protobuf==5.28.2`, `prometheus_client==0.21.0`)
  - `requirements-build.txt` (`grpcio-tools==1.66.1`)
- [x] **2. Kontrak gRPC:**
  - `proto/dfs.proto` (MasterService & DataNodeService sesuai spesifikasi B3)
  - `proto/__init__.py`
  - Kompilasi stub proto ke `proto/dfs_pb2.py` dan `proto/dfs_pb2_grpc.py`
- [x] **3. Common Utilities:**
  - `common/config.py`: Konfigurasi berbasis dataclass dan membaca `os.environ` dengan nilai default.
  - `common/logging_setup.py`: Inisialisasi logger terstandar dan logger khusus `dfs.out`.
  - `common/grpc_util.py`: Setting frame 1 MiB, channel options, retry helper backoff, channel cache per address.
- [x] **4. Linter Anti-Print:**
  - `scripts/check_no_print.py`: Script pemeriksa agar tidak ada `print()` di modul produksi (`master`, `datanode`, `client`, `common`).
- [x] **5. Server Skeleton & Registrasi Heartbeat:**
  - `master/server.py`: gRPC server untuk `MasterService` dengan implementasi endpoint `Heartbeat` dan `ClusterStatus`.
  - `master/state.py`: Struktur memory state untuk melacak node hidup/mati dilindungi `threading.RLock`.
  - `datanode/server.py`: gRPC server untuk `DataNodeService` dengan background thread heartbeat ke master setiap 2 detik.
  - `client/cli.py`: Entrypoint CLI dengan subcommand `wait-ready` dan `status`.
- [x] **6. Docker & Compose Setup:**
  - `Dockerfile`: Multi-stage build menggunakan `docker.io/library/python:3.12-slim`.
  - `docker-compose.yml`: Master, 3 DataNode (`datanode1`, `datanode2`, `datanode3`), Client.
  - Named volumes: `master_meta`, `dn1_data`, `dn2_data`, `dn3_data`, `client_work`.

---

## 🔍 Kriteria Verifikasi
1. Menjalankan `python scripts/check_no_print.py` -> Lolos (0 print ditemukan).
2. `podman build -t localhost/dfsx:latest .` -> Build sukses.
3. `podman-compose -p dfsx up -d` -> Semua container aktif dan sehat.
4. `podman exec dfs-client python -m client.cli wait-ready --min-nodes 3 --timeout 60` -> Sukses.
5. `podman exec dfs-client python -m client.cli status` -> 3 node `ALIVE`.
6. Simpan hasil verifikasi ke `docs/evidence/phase1.md`.
