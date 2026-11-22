# Prompt Instruksi untuk Agent CLI — DFS-X Setup & Cluster Run

> **Status:** Final deliverable untuk task setup lingkungan & pemastikan cluster DFS-X berjalan di Podman (Windows/WSL2).
> **Cara pakai:** Copy-paste seluruh konten markdown ini langsung ke Agent CLI (Cursor Composer, Copilot Workspace, Claude Code, dll).

---

### 📋 Prompt untuk Agent CLI:

**Role:** Kamu adalah Senior DevOps Engineer & Python Architect. Tugasmu adalah menyelesaikan setup lingkungan, memperbaiki dependensi, dan memastikan cluster DFS-X berjalan di Podman Windows.

**Konteks Proyek:**
- **Judul:** DFS-X Terminal (Mini Distributed File System).
- **Stack:** Python 3.12, gRPC, Podman (WSL2), SQLite.
- **Masalah Saat Ini:** Instalasi `pip` gagal karena mencoba mengompilasi `grpcio` dari source di Python 3.14/terbaru tanpa Visual C++ Build Tools.
- **Target:** Sistem harus bisa di-build, dijalankan (`podman-compose up`), dan menerima upload file dummy.

**Instruksi Langkah-demi-Langkah (Wajib Diikuti):**

1.  **Perbaiki Lingkungan Python Host:**
    *   Pastikan kita menggunakan **Python 3.12**. Jika environment saat ini bukan 3.12, hapus `.venv` lama dan buat baru dengan `py -3.12 -m venv .venv`.
    *   Aktifkan environment dan update pip: `python -m pip install --upgrade pip`.
    *   Install dependensi host dengan versi yang stabil (menghindari compile dari source):
        ```powershell
        pip install grpcio==1.62.1 protobuf==5.26.1 prometheus_client==0.21.0
        pip install grpcio-tools==1.62.1 podman-compose matplotlib pandas ruff pytest
        ```

2.  **Generate Kode Protobuf:**
    *   Jalankan perintah ini di root folder untuk membuat stubs Python dari `proto/dfs.proto`:
        ```powershell
        python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dfs.proto
        ```
    *   Pastikan file `proto/dfs_pb2.py` dan `proto/dfs_pb2_grpc.py` berhasil dibuat.

3.  **Build Container Image:**
    *   Build image tunggal untuk semua node (Master, DataNode, Client) menggunakan `Dockerfile` yang ada:
        ```powershell
        podman build -t localhost/dfsx:latest .
        ```

4.  **Jalankan Cluster:**
    *   Start cluster menggunakan `docker-compose.yml` (kompatibel dengan podman-compose):
        ```powershell
        podman-compose -p dfsx up -d
        ```
    *   Tunggu hingga semua containers running. Verifikasi dengan `podman ps`.

5.  **Verifikasi Kesiapan Sistem (Health Check):**
    *   Jalankan perintah ini berulang kali sampai status menunjukkan 3 node ALIVE:
        ```powershell
        podman exec dfs-client python -m client.cli wait-ready --min-nodes 3 --timeout 60
        podman exec dfs-client python -m client.cli status
        ```

6.  **Uji Fungsi Dasar (Smoke Test):**
    *   Generate data dummy kecil (mode lite/sparse):
        ```powershell
        podman exec dfs-client python -m client.cli gen-data --profile lite
        ```
    *   Coba upload satu file:
        ```powershell
        podman exec dfs-client python -m client.cli upload /work/testdata/f001.bin test_file.bin
        ```
    *   Cek metadata untuk memastikan replikasi RF=2 berjalan:
        ```powershell
        podman exec dfs-client python -m client.cli info test_file.bin
        ```

**Aturan Penting:**
*   Gunakan **PowerShell** commands jika di Windows.
*   Jangan gunakan `print()` di kode Python; pastikan semua logging menggunakan modul `logging`.
*   Jika ada error pada `podman-compose`, coba fallback ke `podman run` manual atau periksa log dengan `podman logs dfs-master`.
*   Laporkan setiap langkah yang berhasil atau gagal secara rinci.

---

## Catatan untuk Agent yang Memproses Prompt Ini

- **Sumber kebenaran versi:** `requirements.txt` & `requirements-build.txt` di repo adalah kebenaran tertinggi (grpcio==1.66.1, protobuf==5.28.2, prometheus_client==0.21.0; grpcio-tools==1.66.1). Pin pada prompt (1.62.1 / 5.26.1) perlu disesuaikan bila memproses dari repo.
- **Struktur entrypoint:** master = `python -m master.server`, datanode = `python -m datanode.server`, client CLI = `python -m client.cli`.
- **Stubs protobuf:** harus digenerate dulu (`python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dfs.proto`) sebelum modul yang memakai `proto` diimpor.
- **CLI yang tersedia saat ini:** `wait-ready`, `status`, `upload`, `download`, `ls`, `rm`, `info`, `verify`, `gen-data` (`client/cli.py`).
- **Konteks fase pengembangan:** phase 2–5 sudah terimplementasi di host (unit test 9 passed, `check_no_print` OK) — instruksi di atas tetap relevan untuk setup lingkungan baru.
