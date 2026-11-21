Siap. Ini dokumen instruksi final — sudah dikunci sesuai semua keputusan kita (Podman, Full Python, 28 GB, dev `lite` → uji `full` data asli, tanpa PowerShell/shell script, skala sampai 16 node).

**Simpan sebagai `IMPLEMENTATION_GUIDE.md`** di root repo. Dokumen ini bisa langsung kamu tempel ke coding agent (Bagian A + B), atau kamu pakai sendiri sebagai checklist.

---

# DFS-X Terminal — Instruksi Implementasi Final

**Judul proyek**
> DFS-X Terminal: Implementasi Mini Distributed File System Berbasis CLI untuk Analisis Replikasi dan Fault Recovery pada Lingkungan Container

**Tema:** Tema 5 — Cloud-based Distributed File Storage (DFS-X)
**Mata kuliah:** Komputasi Paralel dan Terdistribusi (3 SKS) · UAS Ganjil 2026/2027
**Batas waktu:** 1 minggu · ujian Kamis, 3 Desember 2026

**Stack terkunci:** Windows 10/11 + Podman (rootless, WSL2) · Python 3.12 · gRPC · container volume · Prometheus + Grafana (bonus)

**Cakupan dokumen:** implementasi, pengujian, benchmark, fault test, monitoring, dan semua bukti untuk laporan. Laporan IEEE/PPT/video dikerjakan terpisah, tetapi **semua angka, grafik, dan log wajib dihasilkan oleh implementasi ini**.

---

## CARA PAKAI DOKUMEN INI

| Audiens | Bagian yang dibaca |
|---|---|
| **Coding agent (Claude/Copilot/dll)** | Tempel **Bagian A** sebagai system prompt, referensikan **Bagian B** sebagai spesifikasi. Kerjakan per fase (Bagian D), jangan sekaligus. |
| **Kamu (manusia)** | Bagian A2 (keputusan), A6 (setup), H (cheatsheet), E (Definition of Done), F (pemetaan rubrik). |
| **Saat menulis laporan** | Bagian C (definisi metrik), F (pemetaan soal), G (deliverables). |

**Aturan pemakaian untuk agent:** kerjakan **satu fase** per sesi. Setelah selesai, jalankan perintah verifikasi fase itu, simpan keluaran ke `docs/evidence/phaseN.md`, baru lanjut. Jangan mengklaim selesai tanpa keluaran perintah nyata.

---

# BAGIAN A — KONTEKS, KEPUTUSAN, DAN ATURAN KERJA

## A1. Sumber kebenaran

Naskah soal UAS adalah sumber kebenaran fungsional. Ringkasan ketentuan yang mengikat:

**Tema 5 — spesifikasi:**
- 1 master node + 3 data nodes
- Implementasi **replication, partitioning, fault recovery**
- Pengujian upload/download **100 file (50 MB)** → dibaca sebagai **100 file × 50 MB = 5 GB data logis**
- Output: hasil uji upload/download + grafik throughput dan waktu akses

**Bagian B — komponen minimal arsitektur (semua wajib):**
1. Master/Coordinator Node — task scheduling, fault detection, data distribution
2. Worker Node — melaksanakan komputasi paralel
3. Communication Layer — MPI / RPC / REST / Kafka
4. Shared/Distributed Storage — NFS / HDFS / container volume
5. Monitoring Module — status node, beban CPU, jaringan (opsional Prometheus/Grafana)

**Bagian F — 6 parameter uji (semua wajib):**
1. Speedup Ratio `S = T_sekuensial / T_paralel`
2. Efficiency `E = S / P`, **P = jumlah prosesor/node**
3. Throughput = **jumlah task yang selesai per detik**
4. Latency = waktu rata-rata komunikasi antar node
5. Fault Tolerance Test — simulasi 1 node gagal saat eksekusi
6. Scalability — uji performa dengan bertambahnya node **(2, 4, 8, 16)**

**Bagian C — struktur laporan:** Pendahuluan · Kajian Teori · Metodologi & Arsitektur · Implementasi · Pengujian dan Analisis Performansi · Kesimpulan & Saran

**Bagian D:** Source code + `README.md` lengkap (cara run, dependensi, topologi)
**Bagian E:** Presentasi, demo & video **maksimum 10 menit**

**Rubrik:** Tema & Desain 15% · Implementasi 25% · Kinerja & Pengujian 25% · Analisis Ilmiah 15% · Laporan & Presentasi 20% · Bonus monitoring/deployment +5%

> Jika ada konflik antara dokumen ini dan naskah soal, **ikuti naskah soal** dan catat konflik di `docs/DECISIONS.md`.

## A2. Keputusan final (TERKUNCI — jangan diubah tanpa mencatat di DECISIONS.md)

| # | Keputusan | Nilai | Alasan |
|---|---|---|---|
| 1 | Runtime | **Podman** (bukan Docker) | Hanya Podman yang tersedia di host. Sintaks kompatibel; file tetap `docker-compose.yml` agar sesuai ketentuan soal. |
| 2 | Bahasa | **Python 3.12 murni** (Master, DataNode, Client) | Deadline 1 minggu. Hybrid Go menambah 2–3 hari setup protoc ganda + debugging lintas bahasa. Bottleneck nyata ada di I/O WSL2, bukan bahasa. |
| 3 | RPC | **gRPC**, streaming frame 1 MiB | Hemat memori, sesuai anjuran Tema 5. |
| 4 | Storage | **Named volume** per node (`dn1_data`…`dnN_data`) | Memenuhi "container volume". Bind-mount Windows dilarang untuk data besar (lambat via 9P). |
| 5 | Replication Factor | **RF = 2**, anti-affinity wajib | 5 GB × 2 = 10 GB, muat di 28 GB. Toleran 1 node gagal. Consistency dijamin 2-phase commit + checksum, bukan oleh jumlah replika. |
| 6 | Chunk size | **8 MiB** (50 MiB → 7 chunk, chunk terakhir 2 MiB) | 700 chunk untuk 100 file (bukan 1300 seperti chunk 4 MiB) → hemat ~46% overhead RPC. |
| 7 | Heartbeat | interval **2 s**, timeout **10 s** | WSL2 jittery; timeout 6 s terlalu rawan false-positive. |
| 8 | Metadata | **SQLite WAL**, persisten di volume `master_meta` | Restart master tidak menghilangkan file (syarat F6). |
| 9 | **Profil data dev** | **`lite`** = 100 × 5 MB **sparse** | Iterasi cepat, disk ≈ 0. Dipakai Fase 1–3. |
| 10 | **Profil data uji** | **`full`** = 100 × 50 MB **data acak asli** (`os.urandom`) | **Benchmark & data laporan wajib data nyata.** Tidak ada penghematan disk di jalur uji. |
| 11 | `SPARSE_WRITE` datanode | **0 (default)** | Dengan data acak, sparse-write tidak berlaku. Dimatikan agar hasil uji jujur dan tidak ada memcmp sia-sia. |
| 12 | Kompresi gRPC | **none** | Data acak tidak terkompresi; kompresi hanya menambah bias CPU. |
| 13 | Skrip bantu | **Python saja** (host, cross-platform) | **Tidak ada `.ps1`, tidak ada `.sh`.** Semua operasi cluster memakai perintah bawaan `podman` / `podman-compose`. |
| 14 | Unit test | **Minimal sanity test saja** (placement, chunking, checksum) | Bukan suite pytest lengkap. Prioritas: integration test end-to-end. |
| 15 | Skalabilitas | **2, 3, 4, 8, 16 node** | Soal minta 16. Prosedur khusus & anggaran resource di C3. |
| 16 | Monitoring | Prometheus + Grafana, **dimatikan saat benchmark**, dinyalakan saat demo | Bonus +5%, tanpa mengganggu angka performa. |
| 17 | Image | **Satu image** `localhost/dfsx:latest`, peran dibedakan lewat `command` | Hemat disk & waktu build. |

## A3. Aturan keras (wajib, tanpa pengecualian)

1. **Logging, bukan `print()`.** Seluruh kode memakai modul `logging`. Keluaran CLI untuk pengguna memakai logger khusus `dfs.out` (handler stdout, format `%(message)s`). Verifikasi: `python scripts/check_no_print.py` harus lolos.
2. **Podman, bukan Docker.** Semua perintah `podman` / `podman-compose`. Rujukan image selalu nama penuh (`docker.io/library/python:3.12-slim`) agar tidak kena error short-name resolution.
3. **Streaming, jangan buffer.** Chunk dikirim/diterima dalam frame **1 MiB**. Dilarang membaca seluruh file atau seluruh chunk ke memori sekaligus di jalur transfer. Memori per worker ≤ ±1 chunk (8 MiB).
4. **Data besar hanya di named volume.** Chunk, dataset dummy, dan output unduhan berada di volume dalam VM Podman. Bind-mount ke Windows **hanya** untuk berkas kecil: `./results`, config monitoring, provisioning Grafana.
5. **Line ending LF.** `.gitattributes` berisi `* text=auto eol=lf`. Tambahkan `.editorconfig`.
6. **Konfigurasi lewat environment variable** (tabel B2) dengan default di `common/config.py`. **Tidak boleh ada keharusan mengisi `.env` manual** — semua default sudah ada di compose + dataclass. Tidak ada angka ajaib yang di-hardcode di logika.
7. **Type hints + docstring singkat** untuk fungsi publik. Format `ruff format`, lint `ruff check` (dev dependency di host saja).
8. **Dependensi minimal.** Runtime image: `grpcio`, `protobuf`, `prometheus_client`. Build stage: `grpcio-tools`. Host venv: `podman-compose`, `matplotlib`, `pandas`, `ruff`, `grpcio`, `grpcio-tools`, `protobuf`. **Tidak ada numpy/scipy/pandas di dalam image.**
9. **Bukti sebelum klaim.** Fase dianggap selesai hanya jika perintah verifikasinya benar-benar dijalankan dan keluarannya disimpan di `docs/evidence/phaseN.md`. Dilarang menulis "seharusnya berhasil".
10. **Kegagalan harus terlihat.** Setiap error gRPC dicatat WARNING/ERROR dengan `node_id`, `chunk_id`, dan penyebab. **Dilarang `except: pass`.** Gunakan `except grpc.RpcError as e:` lalu log.
11. **Jangan mengubah protokol diam-diam.** Jika `proto/dfs.proto` berubah: update file, regenerasi stub, catat di `docs/DECISIONS.md`.
12. **Tidak ada data besar di git.** `.gitignore` mencakup `*.bin`, `results/`, `testdata/`, `__pycache__`, `.venv`. Skrip tetap di-commit.
13. **Jam master otoritatif.** `last_seen` node memakai `time.monotonic()` **di sisi master**, bukan timestamp dari datanode (hindari clock skew).
14. **Semua RPC punya deadline.** Kontrol 5 s, transfer chunk 60 s. Channel di-cache per node, **jangan buat channel per chunk**.
15. **Angka benchmark apa adanya.** Dilarang memoles, menghaluskan, atau merekayasa hasil. Keterbatasan host ditulis di laporan sebagai analisis, bukan disembunyikan.

## A4. Yang sengaja TIDAK dikerjakan (eksplisit, agar agent tidak menambah scope)

| Item | Status | Alasan |
|---|---|---|
| `dfs.ps1` / helper PowerShell | ❌ Skip | Diganti perintah podman bawaan (Bagian H) |
| Skrip bash `.sh` | ❌ Skip | Host Windows; skrip bantu ditulis Python |
| Suite pytest lengkap | ❌ Skip | Hanya 3 sanity test minimal (B10) |
| Kubernetes / deployment K8s | ❌ Skip | Podman Compose sudah memenuhi; K8s hanya bonus alternatif |
| Hybrid Go / Rust | ❌ Skip | Keputusan A2 #2 |
| Consistent hashing | ❌ Skip | Overkill untuk ≤16 node; load-aware + anti-affinity cukup |
| Multi-master / Raft / leader election | ❌ Skip | Master tunggal; SPOF diakui sebagai keterbatasan di laporan |
| Enkripsi / TLS / auth | ❌ Skip | Di luar cakupan soal |
| `standard` profile (ukuran campuran) | ❌ Skip | Cukup `lite`, `full`, `integrity` |
| Centralized logging (Loki/ELK) | ❌ Skip | Rotating file + `podman logs` cukup |

## A5. Anggaran sumber daya

### A5.1 RAM & CPU per container

| Komponen | `mem_limit` | `cpus` | Catatan |
|---|---|---|---|
| master | 256m | 0.5 | SQLite + state in-memory |
| datanode (default, N ≤ 8) | 192m | 0.5 | tanpa buffer chunk penuh |
| datanode (N = 16, mode hemat) | 128m | 0.25 | `GRPC_WORKERS=4` |
| client | 512m | 1.0 | 8 worker × 8 MiB ≈ 64 MiB + overhead |
| prometheus | 256m | 0.5 | retensi 2 h, scrape 5 s |
| grafana | 256m | 0.5 | anon-viewer, tanpa plugin tambahan |

### A5.2 Ukuran `podman machine` berdasarkan RAM laptop

| RAM laptop | `--memory` | Skala maksimum aman | Monitoring |
|---|---|---|---|
| 8 GB | 3072 | 4 node penuh; 16 node **mode hemat + lite** (risiko, lihat C3) | mati saat benchmark |
| 16 GB | 5120–6144 | 16 node mode hemat + lite | boleh nyala saat demo |

> **Wajib:** agent memeriksa `podman info` dan RAM host lebih dulu, lalu menyesuaikan. Jangan asal pakai 4 CPU / 4 GB jika host cuma 8 GB.

### A5.3 Anggaran disk (host tersisa 28 GB) — skenario puncak `full`

| Item | Estimasi |
|---|---|
| Image `localhost/dfsx` (final stage) | ~250 MB |
| Builder stage + cache `grpcio-tools` | ~250 MB |
| Base `python:3.12-slim` (shared layer) | ~130 MB |
| `prom/prometheus` | ~110 MB |
| `grafana/grafana` | ~450 MB |
| Volume `client_work` — dataset `full` (100 × 50 MB, **data nyata**) | **5.0 GB** |
| Volume `dnX_data` — total chunk RF=2 | **10.0 GB** |
| Volume `master_meta` (SQLite) | < 20 MB |
| Log terotasi (semua container) | < 150 MB |
| `results/` (CSV, JSON, PNG) | < 100 MB |
| Overhead VM / slack VHDX WSL | ~1.5 GB |
| **PUNCAK** | **≈ 17.9 GB** |
| Tersisa | 28 GB |
| **Headroom** | **≈ 10 GB** ✅ |

**Catatan penting:** total chunk RF=2 selalu ~10 GB **terlepas dari jumlah node** (hanya menyebar lebih tipis). Jadi menaikkan N tidak menambah kebutuhan disk.

**Aturan penghematan disk (wajib diimplementasi):**
1. DataNode **menolak** menyimpan chunk jika `shutil.disk_usage(DATA_DIR).free < MIN_FREE_MB` (default 512) → `ok=false, error="DISK_LOW"`.
2. Pembersihan antar-run benchmark memakai **`DeleteFile`** (CLI `rm`), bukan menghapus volume — lebih cepat dan tidak membengkakkan VHDX.
3. **Mode batch** (`gendata --batch 20`): generate 20 file → upload → hapus file lokal → lanjut. Menurunkan puncak dari 15 GB jadi ~11 GB. Manifest checksum tetap lengkap.
4. Benchmark download **menghapus setiap file output setelah checksum diverifikasi**.
5. Pre-flight check sebelum run `full`: skrip benchmark menolak mulai jika free space VM < 18 GB.
6. Setelah proyek selesai: `podman-compose -p dfsx down -v` → `podman system prune -a` → `podman volume prune -f`.
7. VHDX WSL **tidak menyusut otomatis**. Jika disk C: masih penuh setelah prune: `wsl --shutdown` lalu `wsl --manage podman-machine-default --set-sparse true` (bila versi WSL mendukung). Catat di DECISIONS.md.

## A6. Persiapan lingkungan (Windows + Podman)

Jalankan berurutan. Simpan keluaran ke `docs/evidence/phase1.md`.

```powershell
# 0. Prasyarat: Windows 10 build 19041+ / Windows 11, virtualisasi aktif di BIOS
wsl --install          # restart bila diminta
wsl --update

# 1. Podman
winget install RedHat.Podman
podman --version

# 2. VM Podman — SESUAIKAN dengan RAM laptop (lihat A5.2)
#    Contoh untuk laptop 8 GB:
podman machine init --cpus 4 --memory 3072 --disk-size 40
podman machine start
podman info --format "{{.Host.Arch}} {{.Host.MemTotal}}"

# 3. Python host (untuk skrip orkestrasi, plot, sanity test)
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install podman-compose matplotlib pandas ruff grpcio grpcio-tools protobuf

# 4. Smoke test
podman run --rm docker.io/library/python:3.12-slim python -c "import sys; sys.stdout.write('ok')"
podman-compose --version
```

**Verifikasi wajib Phase 1:**
```powershell
podman info --format "{{.Host.MemTotal}}"      # harus >= ukuran yang diminta
podman volume ls                              # setelah up: 5 volume dfsx
```

> Jika `podman-compose` bermasalah (mis. `mem_limit`/`cpus` diabaikan), fallback ke `scripts/cluster.py` yang memanggil `podman run` langsung (B8.4). Catat di DECISIONS.md.

**Optimasi host (opsional, dampak besar):**
- Tambahkan folder proyek dan folder VM Podman ke exclusion Windows Defender real-time scanning.
- Pastikan sumber proyek berada di drive dengan ruang lega; data tetap di named volume, bukan di folder Windows.

## A7. Prioritas bila waktu mepet

**Urutan dikorbankan (dari yang pertama dikorbankan):**
1. Monitoring Prometheus + Grafana (bonus, bukan wajib)
2. Run `full` pada N = 2 dan 4 (Tier 3 di C3)
3. Replikasi percobaan ke-3 (cukup 2 recorded run)
4. Latency p99 (cukup avg + p50 + p95)
5. Trimming over-replication & GC orphan
6. Sanity test unit

**JANGAN PERNAH dikorbankan:**
- ✅ Upload/download benar, checksum cocok
- ✅ RF = 2 + anti-affinity
- ✅ Fault recovery otomatis (F1–F3)
- ✅ Benchmark `full` 100 × 50 MB pada 3 node (headline, sesuai soal)
- ✅ Skalabilitas sampai 16 node (walau dengan profil `lite`, didokumentasikan)
- ✅ Logging tanpa `print()`
- ✅ `README.md` + laporan + video

---

# BAGIAN B — SPESIFIKASI IMPLEMENTASI

## B1. Struktur repositori

```
dfsx/
├── proto/
│   ├── __init__.py
│   └── dfs.proto
├── common/
│   ├── __init__.py
│   ├── config.py            # env → dataclass, semua default di sini
│   ├── logging_setup.py     # setup_logging(name), logger "dfs.out"
│   ├── checksum.py          # sha256_file, sha256_bytes, combine_checksums
│   └── grpc_util.py         # OPTIONS, channel cache, retry, interceptor latency
├── master/
│   ├── __init__.py
│   ├── server.py            # entrypoint: python -m master.server
│   ├── state.py             # dataclass + indeks in-memory + RLock
│   ├── store.py             # SQLite WAL
│   ├── placement.py         # pick_targets + anti-affinity
│   ├── failure.py           # failure detector thread
│   └── replication.py       # re-replication queue + trim + GC orphan
├── datanode/
│   ├── __init__.py
│   ├── server.py            # entrypoint: python -m datanode.server
│   ├── chunkstore.py        # tulis atomik, baca streaming, scan awal
│   └── heartbeat.py         # thread heartbeat + block report
├── client/
│   ├── __init__.py
│   ├── cli.py               # argparse, entrypoint: python -m client.cli
│   ├── upload.py
│   ├── download.py
│   ├── gendata.py           # profil lite / full / integrity + manifest
│   └── bench.py             # runner single-run (di dalam container)
├── scripts/                 # SEMUA PYTHON, dijalankan di HOST
│   ├── gen_compose.py       # hasilkan compose + prometheus untuk N node
│   ├── cluster.py           # fallback tanpa podman-compose (opsional)
│   ├── bench_matrix.py      # orkestrasi matriks benchmark
│   ├── fault_test.py        # skenario F1–F6
│   ├── plots.py             # matplotlib → results/plots/
│   └── check_no_print.py    # penjaga aturan A3 #1
├── tests/                   # sanity test minimal (B10)
│   ├── test_placement.py
│   ├── test_chunking.py
│   └── test_checksum.py
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       ├── provisioning/datasources/ds.yml
│       ├── provisioning/dashboards/dash.yml
│       └── dashboards/dfsx.json
├── results/                 # bind-mount kecil, di-.gitignore
│   ├── plots/
│   └── fault/
├── docs/
│   ├── DECISIONS.md
│   └── evidence/            # phase1.md ... phase5.md
├── Dockerfile
├── docker-compose.yml               # 3 node default (deliverable)
├── docker-compose.monitoring.yml    # override prometheus + grafana
├── requirements.txt
├── requirements-build.txt
├── .gitattributes
├── .gitignore
├── .dockerignore
├── .editorconfig
└── README.md
```

**Generate stub proto** (dijalankan dari root repo, host maupun Dockerfile memakai perintah sama):
```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dfs.proto
```
Hasil: `proto/dfs_pb2.py`, `proto/dfs_pb2_grpc.py`. Impor **selalu** `from proto import dfs_pb2, dfs_pb2_grpc` (butuh `proto/__init__.py`).

**Berkas wajib kecil:**

`.gitattributes`
```
* text=auto eol=lf
*.png binary
*.bin binary
```

`.gitignore`
```
__pycache__/
*.py[cod]
.venv/
*.bin
testdata/
results/
proto/dfs_pb2.py
proto/dfs_pb2_grpc.py
docker-compose.gen.yml
monitoring/prometheus.gen.yml
.ruff_cache/
```

`.dockerignore`
```
.git
.venv
results
docs
tests
scripts
monitoring
*.bin
__pycache__
```

`requirements.txt` (runtime image — pin versi, `grpcio` dan `grpcio-tools` **harus sama**)
```
grpcio==1.66.1
protobuf==5.28.2
prometheus_client==0.21.0
```

`requirements-build.txt`
```
grpcio-tools==1.66.1
```

## B2. Konfigurasi (environment variable)

Semua default ada di `common/config.py` sebagai dataclass. **Tidak perlu `.env` manual.**

| Variabel | Default | Komponen | Keterangan |
|---|---|---|---|
| `LOG_LEVEL` | `INFO` | semua | |
| `METRICS_PORT` | `8000` | master, datanode | endpoint Prometheus |
| `MASTER_PORT` | `50051` | master | |
| `RF` | `2` | master | replication factor |
| `CHUNK_SIZE_MB` | `8` | master | default untuk client |
| `HEARTBEAT_TIMEOUT_S` | `10` | master | batas DOWN |
| `SCAN_INTERVAL_S` | `2` | master | siklus detector + replication |
| `REPLICATION_CONCURRENCY` | `2` | master | batasi beban laptop |
| `TRIM_GRACE_S` | `30` | master | tunda hapus replika berlebih |
| `UPLOAD_TTL_S` | `600` | master | kedaluwarsa upload tak ter-commit |
| `DB_PATH` | `/meta/master.db` | master | |
| `MIN_FREE_BYTES` | `536870912` | master | ambang kelayakan target alokasi |
| `NODE_ID` | — (wajib) | datanode | mis. `dn-1` |
| `NODE_ADDR` | — (wajib) | datanode | mis. `datanode1:50052` |
| `MASTER_ADDR` | `master:50051` | datanode, client | |
| `DN_PORT` | `50052` | datanode | |
| `DATA_DIR` | `/data` | datanode | |
| `HEARTBEAT_INTERVAL_S` | `2` | datanode | |
| `SPARSE_WRITE` | **`0`** | datanode | **0 untuk benchmark jujur**; 1 hanya eksperimen |
| `FSYNC` | `0` | datanode | 1 = fsync per chunk (lebih aman, lebih lambat) |
| `MIN_FREE_MB` | `512` | datanode | tolak tulis bila ruang bebas lebih kecil |
| `GRPC_WORKERS` | master `16`, datanode `8` | server | thread pool; **4 untuk N=16** |
| `CLIENT_WORKERS` | `8` | client | thread unggah/unduh |
| `GRPC_COMPRESSION` | `none` | client, datanode | **jangan gzip untuk benchmark** |
| `WORK_DIR` | `/work` | client | root dataset & output |

## B3. Kontrak gRPC — `proto/dfs.proto` (lengkap, copy-paste)

```protobuf
syntax = "proto3";
package dfs;

// ---------- umum ----------
message NodeAddr { string node_id = 1; string address = 2; }

// ---------- MasterService ----------
message ChunkPlan {
  string chunk_id = 1;
  int32  index    = 2;
  int64  size     = 3;
  repeated NodeAddr targets = 4;      // RF node berbeda (anti-affinity)
}
message AllocateRequest {
  string filename   = 1;
  int64  file_size  = 2;
  int64  chunk_size = 3;              // 0 = pakai default master
  bool   overwrite  = 4;
}
message AllocateResponse {
  bool   ok = 1; string error = 2;
  string upload_id = 3;
  int32  replication = 4;
  repeated ChunkPlan chunks = 5;
}
message ReallocateRequest {           // ganti target bila replika gagal saat upload
  string upload_id = 1; string chunk_id = 2;
  repeated string exclude_nodes = 3;
}
message ReallocateResponse { bool ok = 1; string error = 2; ChunkPlan chunk = 3; }

message ChunkResult {
  string chunk_id = 1;
  string checksum = 2;                // sha256 hex dihitung client
  repeated string stored_on = 3;      // node_id yang sukses menyimpan
}
message CommitRequest {
  string upload_id = 1; string filename = 2;
  string file_checksum = 3;           // sha256 dari gabungan checksum chunk berurutan
  repeated ChunkResult chunks = 4;
}
message CommitResponse { bool ok = 1; string error = 2; int32 under_replicated = 3; }

message LocationsRequest { string filename = 1; }
message ChunkLocation {
  string chunk_id = 1; int32 index = 2; int64 size = 3; string checksum = 4;
  repeated NodeAddr replicas = 5;     // hanya node ALIVE, urut preferensi
}
message LocationsResponse {
  bool ok = 1; string error = 2;
  int64 file_size = 3; string file_checksum = 4;
  repeated ChunkLocation chunks = 5;
}

message ChunkInfo { string chunk_id = 1; int64 size = 2; }
message HeartbeatRequest {
  string node_id = 1; string address = 2;
  int64 used_bytes = 3;               // logis (jumlah ukuran chunk)
  int64 physical_bytes = 4;           // fisik (st_blocks*512)
  int64 free_bytes = 5;
  int32 chunk_count = 6;
  bool  full_report = 7;
  repeated ChunkInfo chunks = 8;      // diisi hanya bila full_report
}
message HeartbeatResponse { bool ok = 1; bool request_full_report = 2; }

message ListRequest {}
message FileInfo {
  string name = 1; int64 size = 2; int32 chunks = 3;
  string status = 4; int32 min_replicas = 5; int64 created_at = 6;
}
message ListResponse { repeated FileInfo files = 1; }

message DeleteRequest { string filename = 1; }
message DeleteResponse { bool ok = 1; string error = 2; int32 chunks_deleted = 3; }

message StatusRequest {}
message NodeStatus {
  string node_id = 1; string address = 2; string state = 3;   // ALIVE | DOWN
  int64 used_bytes = 4; int64 physical_bytes = 5; int64 free_bytes = 6;
  int32 chunk_count = 7; double last_seen_age_s = 8;
}
message StatusResponse {
  repeated NodeStatus nodes = 1;
  int32 files = 2; int32 chunks = 3;
  int32 under_replicated = 4; int32 lost = 5; int32 rereplications_done = 6;
  int32 replication = 7;
}

service MasterService {
  rpc AllocateChunks(AllocateRequest)     returns (AllocateResponse);
  rpc ReallocateChunk(ReallocateRequest)  returns (ReallocateResponse);
  rpc CommitFile(CommitRequest)           returns (CommitResponse);
  rpc GetReadLocations(LocationsRequest)  returns (LocationsResponse);
  rpc Heartbeat(HeartbeatRequest)         returns (HeartbeatResponse);
  rpc ListFiles(ListRequest)              returns (ListResponse);
  rpc DeleteFile(DeleteRequest)           returns (DeleteResponse);
  rpc ClusterStatus(StatusRequest)        returns (StatusResponse);
}

// ---------- DataNodeService ----------
message ChunkData {
  string chunk_id = 1;                // wajib di pesan pertama
  int64  offset   = 2;
  bytes  data     = 3;                // frame <= 1 MiB
  bool   last     = 4;
}
message StoreResponse { bool ok = 1; string error = 2; string checksum = 3; int64 bytes_written = 4; }
message RetrieveRequest { string chunk_id = 1; }
message ReplicateRequest {            // perintah master: kirim chunk ke target
  string chunk_id = 1; string target_address = 2; string expected_checksum = 3;
}
message ReplicateResponse { bool ok = 1; string error = 2; int64 bytes = 3; double seconds = 4; }
message DeleteChunkRequest { string chunk_id = 1; }
message DeleteChunkResponse { bool ok = 1; }
message StatsRequest {}
message StatsResponse {
  string node_id = 1; int64 used_bytes = 2; int64 physical_bytes = 3;
  int64 free_bytes = 4; int32 chunk_count = 5; int64 bytes_in = 6; int64 bytes_out = 7;
}

service DataNodeService {
  rpc StoreChunk(stream ChunkData)      returns (StoreResponse);
  rpc RetrieveChunk(RetrieveRequest)    returns (stream ChunkData);
  rpc ReplicateChunk(ReplicateRequest)  returns (ReplicateResponse);
  rpc DeleteChunk(DeleteChunkRequest)   returns (DeleteChunkResponse);
  rpc GetStats(StatsRequest)            returns (StatsResponse);
}
```

**Opsi channel/server (`common/grpc_util.py`) — wajib:**
```python
OPTIONS = [
    ("grpc.max_send_message_length", 16 * 1024 * 1024),
    ("grpc.max_receive_message_length", 16 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 10_000),
    ("grpc.keepalive_timeout_ms", 3_000),
    ("grpc.http2.max_pings_without_data", 0),
]
FRAME = 1 << 20   # 1 MiB
```
- Client **meng-cache satu channel per node** (`dict[address, grpc.Channel]`), jangan buat channel per chunk.
- Semua RPC punya deadline: kontrol **5 s**, transfer chunk **60 s**.
- Retry helper: 3 percobaan, backoff 0.2 / 0.5 / 1.0 s, hanya untuk `UNAVAILABLE` / `DEADLINE_EXCEEDED`.

## B4. Master Node

### B4.1 State in-memory (dilindungi satu `threading.RLock`)

```python
@dataclass
class NodeInfo:
    node_id: str; address: str
    state: str = "ALIVE"            # ALIVE | DOWN
    last_seen: float = 0.0          # time.monotonic() saat heartbeat DITERIMA master
    used_bytes: int = 0; physical_bytes: int = 0; free_bytes: int = 0
    chunk_count: int = 0
    planned_bytes: int = 0          # alokasi belum ter-commit (hindari numpuk di 1 node)
    down_since: float = 0.0

@dataclass
class ChunkMeta:
    chunk_id: str; file: str; index: int; size: int; checksum: str
    replicas: set[str]              # node_id
    state: str = "COMMITTED"        # UPLOADING | COMMITTED | LOST
    under_since: float = 0.0

@dataclass
class FileMeta:
    name: str; size: int; chunk_size: int; file_checksum: str
    chunk_ids: list[str]; status: str; created_at: float
```

Indeks tambahan: `node_chunks: dict[str, set[str]]`, `uploads: dict[upload_id, UploadCtx]` (plan + waktu dibuat, untuk TTL).

**`last_seen` wajib `time.monotonic()` sisi master.** Jangan percaya jam datanode.

### B4.2 Penempatan & anti-affinity (`placement.py`)

```python
def pick_targets(nodes, k, exclude=frozenset(), holders=frozenset()):
    """Pilih k node ALIVE berbeda; hindari 'holders' (sudah punya replika) & 'exclude'."""
    cands = [n for n in nodes
             if n.state == "ALIVE"
             and n.node_id not in exclude
             and n.node_id not in holders
             and n.free_bytes > MIN_FREE_BYTES]
    cands.sort(key=lambda n: (n.used_bytes + n.planned_bytes, n.chunk_count, n.node_id))
    return cands[:k]
```

- Tiap chunk mendapat **RF node berbeda** (anti-affinity wajib).
- Setelah memilih, tambahkan `size` ke `planned_bytes` tiap target → chunk berikutnya menyebar. Inilah yang membuat Chunk 0 → `dn-1,dn-3`, Chunk 1 → `dn-2,dn-1`, dst. **Ini adalah implementasi *partitioning* yang diminta soal.**
- Jika node ALIVE < RF: alokasi tetap diizinkan dengan replika sebanyak tersedia (≥1), chunk ditandai *under-replicated*, log WARNING. Jika 0 node: `ok=false`.
- `CommitFile`: validasi `upload_id`; setiap chunk punya ≥1 `stored_on` yang ALIVE; tulis metadata ke SQLite **dalam satu transaksi**; status `COMMITTED`; kembalikan jumlah under-replicated; kurangi `planned_bytes`.
- `chunk_id` deterministik: `f"{upload_id}_c{index:04d}"` (mudah dilacak di log).

### B4.3 Failure detector (`failure.py`)

Thread tiap `SCAN_INTERVAL_S`: node `ALIVE` dengan `monotonic() - last_seen > HEARTBEAT_TIMEOUT_S` → `DOWN`.

**Log persis (dipakai sebagai bukti screenshot & di-parse `fault_test.py`):**
```
WARNING [master.failure] NODE_DOWN node=dn-2 silent_for=10.4s chunks_affected=23
INFO    [master.failure] NODE_UP node=dn-2 downtime=48.2s
```
- Node DOWN yang mengirim heartbeat → `ALIVE`, balas `request_full_report=true`.
- Node belum dikenal (habis restart master) juga diminta `full_report`.

### B4.4 Replication manager (`replication.py`) — **queue-based, bukan lewat heartbeat**

Thread tiap `SCAN_INTERVAL_S`:
1. Cari under-replicated: chunk `COMMITTED` dengan `|replicas ∩ ALIVE| < RF`.
2. 0 replika ALIVE → state `LOST`, log ERROR, metrik `dfs_lost_chunks`.
3. Urutkan naik berdasarkan jumlah replika hidup (paling berisiko dulu).
4. Masukkan ke **`queue.Queue` task repair** dengan set `in_flight` untuk cegah duplikat. Worker poolconsume maks `REPLICATION_CONCURRENCY` in-flight.
5. Untuk tiap task:
   - `source` = replika ALIVE dengan beban terkecil
   - `target` = `pick_targets(..., k=1, holders=replicas)`
   - Panggil `DataNodeService.ReplicateChunk` di source dengan `target_address` + `expected_checksum`. Source men-stream chunk ke `StoreChunk` target; target memverifikasi checksum.
   - Sukses → tambah replika ke metadata + SQLite, log DONE. Gagal → log WARNING, **re-queue dengan backoff**, coba lagi siklus berikutnya.
6. Tidak ada target (mis. N=2, 1 mati) → tetap under-replicated, log WARNING **sekali per chunk-set** (jangan spam).
7. **Trim over-replication:** replika ALIVE > RF yang stabil ≥ `TRIM_GRACE_S` → `DeleteChunk` di node paling penuh (terjadi saat node kembali).
8. **GC orphan:** pada `full_report`, chunk yang tidak ada di metadata dan bukan upload aktif (belum kedaluwarsa `UPLOAD_TTL_S`) → `DeleteChunk`.

**Log persis:**
```
INFO [master.repl] REREPLICATE_START chunk=ab12_c0004 src=dn-1 dst=dn-3 alive_replicas=1
INFO [master.repl] REREPLICATE_DONE  chunk=ab12_c0004 src=dn-1 dst=dn-3 bytes=8388608 took=0.41s
INFO [master.repl] CLUSTER_HEALTHY under_replicated=0 recovery_time=17.8s
```

> **Catatan desain:** repair task **tidak** dikirim lewat heartbeat response. Heartbeat tetap ringan (ping + block report). Ini menghindari task hilang saat node restart dan memberi jalur retry yang jelas.

### B4.5 Persistensi (`store.py`)

SQLite: `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`

Tabel:
- `files(name PK, size, chunk_size, file_checksum, status, created_at)`
- `chunks(chunk_id PK, file_name, idx, size, checksum, state)`
- `replicas(chunk_id, node_id, PRIMARY KEY(chunk_id, node_id))`

Tulis **hanya** saat commit / delete / replica-change (bukan saat alokasi). Saat start: muat semua ke memori; semua node dianggap `DOWN` sampai heartbeat pertama.

### B4.6 Metrik master (Prometheus, port `METRICS_PORT`)

`dfs_nodes_alive`, `dfs_nodes_down`, `dfs_files_total`, `dfs_chunks_total`, `dfs_under_replicated_chunks`, `dfs_lost_chunks`, `dfs_rereplications_total`, `dfs_rereplication_seconds` (histogram), `dfs_rpc_seconds{method}` (histogram via server interceptor), `dfs_node_used_bytes{node}`, `dfs_node_physical_bytes{node}`.

## B5. Data Node

### B5.1 Chunk store (`chunkstore.py`)

- Path: `/data/chunks/<2 huruf pertama id>/<chunk_id>`
- Tulis ke `.tmp` lalu `os.replace()` → **atomik**, tidak ada chunk setengah jadi.
- Hitung SHA-256 **inkremental** saat frame masuk (satu pass, tanpa buffer penuh).
- `FRAME = 1 << 20`. Baca/tulis per frame.
- **`SPARSE_WRITE=0` (default).** Jika diaktifkan (`1`, hanya eksperimen): frame yang seluruhnya nol di-skip dengan `f.seek(len(frame), os.SEEK_CUR)`, lalu `f.truncate(total)`. Checksum tetap dihitung dari data logis.
- Tolak simpan bila `shutil.disk_usage(DATA_DIR).free < MIN_FREE_MB` → `ok=false, error="DISK_LOW"`, log ERROR.
- `FSYNC=1` → `f.flush(); os.fsync()` sebelum `os.replace`.
- `RetrieveChunk`: baca streaming 1 MiB per frame.
- Saat start: scan `/data/chunks` sekali → `chunk_count`, `used_bytes` (logis), `physical_bytes` (`st_blocks*512`). Selanjutnya counter diperbarui in-memory.
- `ReplicateChunk`: source membaca chunk, stream ke `StoreChunk` target, target bandingkan dengan `expected_checksum`. mismatch → `ok=false, error="CHECKSUM_MISMATCH"`.

### B5.2 Heartbeat (`heartbeat.py`)

Thread daemon tiap `HEARTBEAT_INTERVAL_S` (2 s) mengirim `HeartbeatRequest`. Saat start dan ketika `request_full_report=true`, sertakan daftar seluruh chunk (`full_report=true`). Tangani `grpc.RpcError` dengan log WARNING **rate-limited** lalu lanjut loop (master mungkin sedang restart).

### B5.3 Metrik datanode

`dfs_dn_bytes_in_total`, `dfs_dn_bytes_out_total`, `dfs_dn_chunks`, `dfs_dn_used_bytes`, `dfs_dn_physical_bytes`, `dfs_dn_free_bytes`, `dfs_rpc_seconds{method}`.

## B6. Client / CLI

### B6.1 Perintah lengkap

```
python -m client.cli upload   <lokal> [remote] [--workers 8] [--chunk-mb 8] [--force]
python -m client.cli download <remote> <lokal> [--workers 8] [--sink disk|null]
python -m client.cli ls
python -m client.cli rm       <remote> | --all
python -m client.cli info     <remote>        # lokasi tiap chunk + replika
python -m client.cli status                   # node, penggunaan, under-replicated
python -m client.cli verify   <remote>        # unduh ke null + cek checksum
python -m client.cli wait-ready --min-nodes 3 --timeout 60
python -m client.cli gen-data --profile lite|full|integrity [--count N] [--size MB] [--batch K] [--dir /work/testdata]
python -m client.cli bench    upload|download|mixed [--profile full] [--workers 8] [--run-id ID] [--out /results]
```

### B6.2 Upload (`upload.py`)

1. `AllocateChunks` → plan per chunk.
2. `ThreadPoolExecutor(max_workers=W)`; tiap task: baca chunk dari file via `seek/read` (≤ 8 MiB di memori), hitung SHA-256, kirim ke setiap target **berurutan** dalam stream 1 MiB (`StoreChunk`), bandingkan `StoreResponse.checksum` dengan checksum lokal.
3. Retry per replika: 3 percobaan, backoff 0.2/0.5/1 s. Node tetap gagal → `ReallocateChunk(exclude_nodes=[gagal])` untuk pengganti. Tidak ada pengganti → lanjut dengan replika yang berhasil (≥1), laporkan di `CommitRequest.stored_on` (master tandai under-replicated; replication manager memperbaiki). Nol replika sukses → upload gagal, log ERROR.
4. `file_checksum = sha256(concat(chunk_checksums berurutan))` — murah, tidak membaca file dua kali.
5. `CommitFile`. Jika gagal, **jangan commit** (chunk yatim dibersihkan GC).

**Log:** `UPLOAD_COMMIT file=f001.bin chunks=7 bytes=52428800 under_replicated=0 took=1.83s`

### B6.3 Download (`download.py`)

1. `GetReadLocations` → daftar chunk + replika ALIVE.
2. Pra-alokasi file lokal (`truncate(file_size)`), thread pool W. Tiap task: coba replika berurutan (acak/urut beban), terima stream, hitung SHA-256, bandingkan dengan `checksum`; tulis di offset `index*chunk_size` dengan **`os.pwrite`** (client jalan di Linux container).
3. Replika gagal / checksum salah → log WARNING `CHECKSUM_MISMATCH` → replika berikutnya. Semua gagal → error.
4. Verifikasi `file_checksum` dari checksum chunk; hapus file parsial bila gagal.
5. `--sink null`: stream + verifikasi checksum tanpa menulis disk (untuk mengukur read cluster murni). **Default `disk`** agar angka end-to-end jujur.

### B6.4 Pengukuran latency (wajib untuk soal F.4)

Pasang **client interceptor** yang mencatat `time.perf_counter()` per RPC (metode, durasi, status). Agregasi `avg`, `p50`, `p95`, `p99`, `count` per metode: `AllocateChunks`, `StoreChunk`, `RetrieveChunk`, `CommitFile`, `GetReadLocations`, `ReallocateChunk`, `Heartbeat`.

**Tambahan (komunikasi antar-node, bukan hanya client→node):** master mencatat durasi `ReplicateChunk` (node→node) pada histogram `dfs_rereplication_seconds` dan di log `REREPLICATE_DONE ... took=`. Laporkan ini sebagai **latency antar worker node**.

Simpan ke `results/latency_<run_id>.json`.

## B7. Generator data — `client/gendata.py` (kode lengkap)

**Ini kunci strategi "develop lite, uji full".** Profil `full` memakai **data acak asli**, bukan sparse.

| Profil | Isi | Ukuran logis | Isi fisik | Dipakai untuk |
|---|---|---|---|---|
| `lite` | 100 × 5 MB | 0.5 GB | **sparse (≈0)** | Fase 1–3: dev, debug, fault test cepat |
| `full` | 100 × 50 MB | **5 GB** | **acak asli (5 GB)** | **Benchmark & data laporan (sesuai soal)** |
| `integrity` | 5 × 20 MB | 100 MB | acak asli | Uji checksum & skenario F5 (korupsi chunk) |

Manifest `manifest.json` selalu dibuat: `{profile, count, size_mb, files:[{name, size, sha256}]}`. Ini memungkinkan verifikasi download **tanpa menyimpan file asli** (mendukung mode batch).

```python
"""Generator dataset dummy DFS-X.

Profil:
  lite      -> sparse, cepat, disk ~0   (development)
  full      -> data acak asli, 5 GB     (benchmark & laporan)
  integrity -> data acak asli, 100 MB   (uji checksum / korupsi)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
from pathlib import Path

from common.logging_setup import setup_logging

logger = logging.getLogger("dfs.gendata")

FRAME = 1 << 20  # 1 MiB
PROFILES: dict[str, tuple[int, int, bool]] = {
    # profile: (count, size_mb, sparse)
    "lite": (100, 5, True),
    "full": (100, 50, False),
    "integrity": (5, 20, False),
}


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while frame := f.read(FRAME):
            h.update(frame)
    return h.hexdigest()


def make_sparse(path: Path, size: int) -> None:
    """File logis `size` byte, fisik ~0 (lubang filesystem)."""
    with path.open("wb") as f:
        f.truncate(size)


def make_random(path: Path, size: int) -> None:
    """File berisi data acak asli, ditulis per frame 1 MiB."""
    remaining = size
    with path.open("wb") as f:
        while remaining > 0:
            n = min(FRAME, remaining)
            f.write(os.urandom(n))
            remaining -= n


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def generate(out_dir: Path, profile: str, count: int, size_mb: int,
             batch: int, min_free_mb: int) -> dict:
    """Bangkitkan dataset + manifest. Kembalikan dict manifest."""
    sparse = PROFILES[profile][2]
    size = size_mb * (1 << 20)
    out_dir.mkdir(parents=True, exist_ok=True)

    need = count * size
    if not sparse:
        avail = free_bytes(out_dir)
        target = need if batch == 0 else min(need, batch * size)
        if avail < target + min_free_mb * (1 << 20):
            logger.error(
                "DISK_LOW profile=%s need=%.1fGiB free=%.1fGiB batch=%d",
                profile, target / 2**30, avail / 2**30, batch,
            )
            raise SystemExit(2)

    logger.info(
        "GEN_START profile=%s count=%d size_mb=%d sparse=%s batch=%d logical=%.2fGiB",
        profile, count, size_mb, sparse, batch, need / 2**30,
    )

    entries: list[dict] = []
    for i in range(1, count + 1):
        name = f"f{i:03d}.bin"
        path = out_dir / name
        if sparse:
            make_sparse(path, size)
        else:
            make_random(path, size)
        digest = _sha256_path(path)
        entries.append({"name": name, "size": size, "sha256": digest})
        logger.info("GEN_FILE name=%s bytes=%d sha256=%s", name, size, digest[:16])

        if batch and i % batch == 0:
            logger.info("GEN_BATCH_DONE generated=%d/%d", i, count)

    manifest = {
        "profile": profile, "count": count, "size_mb": size_mb,
        "sparse": sparse, "total_bytes": need, "files": entries,
    }
    mpath = out_dir / "manifest.json"
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("GEN_DONE profile=%s files=%d manifest=%s", profile, count, mpath)
    return manifest


def main() -> None:
    setup_logging("dfs.gendata")
    p = argparse.ArgumentParser(description="Generate dataset DFS-X")
    p.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    p.add_argument("--count", type=int, default=0, help="0 = default profil")
    p.add_argument("--size", type=int, default=0, help="MB; 0 = default profil")
    p.add_argument("--dir", default="/work/testdata")
    p.add_argument("--batch", type=int, default=0,
                   help="generate bertahap K file (hemat disk); 0 = sekaligus")
    p.add_argument("--min-free-mb", type=int, default=1024)
    a = p.parse_args()

    d_count, d_size, _ = PROFILES[a.profile]
    generate(Path(a.dir), a.profile, a.count or d_count, a.size or d_size,
             a.batch, a.min_free_mb)


if __name__ == "__main__":
    main()
```

> **Penting untuk laporan:** profil `lite` sparse hanya untuk development. **Semua angka benchmark yang masuk laporan berasal dari profil `full` (data acak asli) atau dari matriks skalabilitas yang konsisten memakai profil yang sama di semua N.** Jangan mencampur angka `lite` dan `full` dalam satu grafik.

## B8. Container

### B8.1 `Dockerfile` (satu image, multi-stage)

```dockerfile
FROM docker.io/library/python:3.12-slim AS builder
WORKDIR /src
COPY requirements.txt requirements-build.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt \
 && pip install --no-cache-dir -r requirements-build.txt
COPY proto ./proto
RUN touch proto/__init__.py \
 && python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dfs.proto

FROM docker.io/library/python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY --from=builder /install /usr/local
WORKDIR /app
COPY --from=builder /src/proto ./proto
COPY common ./common
COPY master ./master
COPY datanode ./datanode
COPY client ./client
CMD ["python", "-c", "import sys; sys.exit('specify a command')"]
```

Build: `podman build -t localhost/dfsx:latest .`
Jalankan sebagai root di dalam container (di Podman rootless dipetakan ke user biasa → izin named volume aman).

### B8.2 `docker-compose.yml` (3 node — deliverable default)

```yaml
x-dn: &dn
  image: localhost/dfsx:latest
  networks: [dfsnet]
  restart: on-failure
  mem_limit: 192m
  cpus: 0.5
  command: ["python", "-m", "datanode.server"]
  environment: &dn_env
    LOG_LEVEL: INFO
    MASTER_ADDR: master:50051
    DN_PORT: "50052"
    METRICS_PORT: "8000"
    SPARSE_WRITE: "0"
    FSYNC: "0"
    MIN_FREE_MB: "512"
    GRPC_WORKERS: "8"

services:
  master:
    image: localhost/dfsx:latest
    container_name: dfs-master
    command: ["python", "-m", "master.server"]
    networks: [dfsnet]
    restart: on-failure
    mem_limit: 256m
    cpus: 0.5
    ports: ["50051:50051"]
    environment:
      LOG_LEVEL: INFO
      RF: "2"
      CHUNK_SIZE_MB: "8"
      HEARTBEAT_TIMEOUT_S: "10"
      SCAN_INTERVAL_S: "2"
      REPLICATION_CONCURRENCY: "2"
      DB_PATH: /meta/master.db
      METRICS_PORT: "8000"
      GRPC_WORKERS: "16"
    volumes: ["master_meta:/meta"]

  datanode1:
    <<: *dn
    container_name: dfs-dn1
    environment: {<<: *dn_env, NODE_ID: dn-1, NODE_ADDR: "datanode1:50052"}
    volumes: ["dn1_data:/data"]

  datanode2:
    <<: *dn
    container_name: dfs-dn2
    environment: {<<: *dn_env, NODE_ID: dn-2, NODE_ADDR: "datanode2:50052"}
    volumes: ["dn2_data:/data"]

  datanode3:
    <<: *dn
    container_name: dfs-dn3
    environment: {<<: *dn_env, NODE_ID: dn-3, NODE_ADDR: "datanode3:50052"}
    volumes: ["dn3_data:/data"]

  client:
    image: localhost/dfsx:latest
    container_name: dfs-client
    command: ["sleep", "infinity"]
    networks: [dfsnet]
    mem_limit: 512m
    cpus: 1.0
    environment:
      MASTER_ADDR: "master:50051"
      CLIENT_WORKERS: "8"
      WORK_DIR: "/work"
      LOG_LEVEL: INFO
    volumes: ["client_work:/work", "./results:/results"]

networks:
  dfsnet: {}

volumes:
  master_meta: {}
  dn1_data: {}
  dn2_data: {}
  dn3_data: {}
  client_work: {}
```

**Catatan kompatibilitas `podman-compose`:**
- **Tidak ada `depends_on: condition`.** Urutan start ditangani retry: datanode/client mencoba ulang ke master dengan backoff; skrip memakai `wait-ready`.
- Jika `mem_limit`/`cpus` tidak dihormati versi terpasang → fallback `scripts/cluster.py` (`podman run --memory 192m --cpus 0.5 ...`), catat di DECISIONS.md.
- Jangan pakai key `name:` top-level; gunakan `-p dfsx`.
- **Data besar hanya di named volume.** `./results` bind-mount kecil, boleh.

### B8.3 `scripts/gen_compose.py` (wajib untuk skalabilitas 2/4/8/16)

```
python scripts/gen_compose.py --nodes N --out docker-compose.gen.yml \
       [--dn-mem 192m] [--dn-cpus 0.5] [--dn-workers 8] [--client-workers 8]
```

Menghasilkan:
- compose dengan N datanode (`datanode1..N`, `NODE_ID=dn-1..dn-N`, volume `dn1_data..dnN_data`)
- `monitoring/prometheus.gen.yml` berisi target N node
- master + client sama seperti default

**Preset mode hemat untuk N = 16:** `--dn-mem 128m --dn-cpus 0.25 --dn-workers 4 --client-workers 4`

`docker-compose.yml` statis (3 node) **tetap menjadi deliverable utama**.

### B8.4 `scripts/cluster.py` (fallback, opsional)

Jika `podman-compose` bermasalah. Subperintah: `up --nodes N`, `down [--volumes]`, `stop-node X`, `start-node X`, `ps`.
Implementasi: `podman network create dfsnet` → `podman volume create ...` → `podman run -d --name dfs-dnX --network dfsnet --memory 192m --cpus 0.5 -v dnX_data:/data -e NODE_ID=... localhost/dfsx:latest python -m datanode.server`.

### B8.5 Monitoring (bonus +5%) — `docker-compose.monitoring.yml`

Dijalankan sebagai override:
```powershell
podman-compose -p dfsx -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

```yaml
services:
  prometheus:
    image: docker.io/prom/prometheus:v2.55.1
    container_name: dfs-prometheus
    networks: [dfsnet]
    mem_limit: 256m
    cpus: 0.5
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.retention.time=2h
    volumes: ["./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]

  grafana:
    image: docker.io/grafana/grafana:11.3.0
    container_name: dfs-grafana
    networks: [dfsnet]
    mem_limit: 256m
    cpus: 0.5
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_ANALYTICS_REPORTING_ENABLED: "false"
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["3000:3000"]

networks:
  dfsnet: {}
```

`monitoring/prometheus.yml`
```yaml
global: {scrape_interval: 5s}
scrape_configs:
  - job_name: master
    static_configs: [{targets: ["master:8000"]}]
  - job_name: datanodes
    static_configs: [{targets: ["datanode1:8000","datanode2:8000","datanode3:8000"]}]
```

**Dashboard `dfsx.json` (ter-provision otomatis) wajib memuat panel:**
1. Node ALIVE / DOWN (stat)
2. Under-replicated & lost chunks (time series)
3. Storage per node — logis vs fisik (bar)
4. Throughput in/out per node — `rate(dfs_dn_bytes_in_total[15s])`, `rate(dfs_dn_bytes_out_total[15s])`
5. Latency RPC p95 per metode — `histogram_quantile(0.95, sum(rate(dfs_rpc_seconds_bucket[1m])) by (le, method))`
6. Jumlah re-replication (counter)
7. Jumlah file & chunk (stat)

**Aturan:** monitoring **dimatikan saat benchmark utama** (mengurangi gangguan CPU), **dinyalakan saat demo fault recovery**.

## B9. Kontrak logging & metrik bersama

`common/logging_setup.py`:
- Format: `%(asctime)s %(levelname)s [%(name)s] %(message)s`
- Level dari `LOG_LEVEL`
- Logger `dfs.out` untuk keluaran CLI: format `%(message)s`, handler stdout
- `RotatingFileHandler` opsional: 10 MB × 3 backup per proses

**Tag peristiwa log WAJIB konsisten** (dipakai `fault_test.py` untuk mengukur waktu deteksi/pemulihan dan untuk screenshot bukti):

| Tag | Level | Sumber |
|---|---|---|
| `NODE_DOWN` / `NODE_UP` | WARNING / INFO | master.failure |
| `REREPLICATE_START` / `REREPLICATE_DONE` | INFO | master.repl |
| `CLUSTER_HEALTHY` | INFO | master.repl |
| `UPLOAD_COMMIT` | INFO | master / client |
| `CHUNK_STORED` | INFO | datanode |
| `CHECKSUM_MISMATCH` | WARNING | datanode / client |
| `DISK_LOW` | ERROR | datanode |
| `GEN_START` / `GEN_FILE` / `GEN_DONE` | INFO | client.gendata |
| `BENCH_START` / `BENCH_DONE` | INFO | client.bench |

## B10. Sanity test minimal (`tests/`, host, tanpa container)

Hanya 3 berkas, total ~80 baris. Bukan suite lengkap.

1. `test_placement.py` — RF node berbeda; `exclude`/`holders` dihormati; node DOWN/penuh tidak dipilih; distribusi merata setelah `planned_bytes` diperbarui.
2. `test_chunking.py` — 50 MiB / 8 MiB → 7 chunk, chunk terakhir 2 MiB; file 0 byte; file < 1 chunk.
3. `test_checksum.py` — `combine_checksums` deterministik dan urut.

Jalankan: `python -m pytest tests -q` (pytest di host venv, **bukan di image**).

## B11. Penjaga aturan — `scripts/check_no_print.py`

```python
"""Pastikan tidak ada print() di kode produksi (aturan A3 #1)."""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

from common.logging_setup import setup_logging

logger = logging.getLogger("dfs.check")
TARGETS = ("master", "datanode", "client", "common")
PATTERN = re.compile(r"(?<![\w.])print\s*\(")


def main() -> int:
    setup_logging("dfs.check")
    hits: list[str] = []
    for folder in TARGETS:
        for path in Path(folder).rglob("*.py"):
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                if PATTERN.search(line) and not line.lstrip().startswith("#"):
                    hits.append(f"{path}:{lineno}: {line.strip()}")
    if hits:
        for h in hits:
            logger.error("PRINT_FOUND %s", h)
        logger.error("CHECK_FAILED count=%d", len(hits))
        return 1
    logger.info("CHECK_OK no print() found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

# BAGIAN C — PENGUJIAN & BENCHMARK (pemetaan ke soal Bagian F)

## C1. Definisi metrik (WAJIB dibaca — ada jebakan rubrik)

| Parameter soal | Definisi yang dipakai | Catatan |
|---|---|---|
| **Throughput** | **(a)** `files_completed / total_seconds` → **task/detik** (definisi literal soal) · **(b)** `total_bytes / 2^20 / total_seconds` → **MiB/s** | **Laporkan KEDUANYA.** Soal menulis "jumlah task yang selesai per detik"; grafik MiB/s tetap dibuat karena Tema 5 minta grafik throughput. |
| **Speedup (paralelisme worker)** | `S_w(W) = T(1) / T(W)` pada **N node tetap**; `T(1)` = upload sekuensial (1 worker, 1 file sekaligus) | Untuk grafik speedup vs workers |
| **Speedup (skala node)** | `S_n(N) = T(N_ref) / T(N)`, `N_ref = 2`, beban & worker tetap | Untuk grafik scalability |
| **Efficiency** | **(a)** `E_n(N) = S_n(N) / (N / N_ref)` ← **PRIMARY, sesuai soal `P = jumlah node`** · **(b)** `E_w(W) = S_w(W) / W` ← sekunder, paralelisme thread | **Tabel utama laporan memakai versi node.** Versi worker diberi label jelas agar tidak dianggap salah rumus. |
| **Latency** | `avg` (wajib soal) + `p50`, `p95`, `p99` durasi RPC per metode (ms), dari interceptor client. **Plus** durasi `ReplicateChunk` = komunikasi **antar worker node** | Soal: "waktu rata-rata komunikasi antar node" → sertakan angka node→node, bukan hanya client→node |
| **Fault tolerance** | `detection_time` (stop → `NODE_DOWN`), `recovery_time` (stop → `CLUSTER_HEALTHY`), integritas data setelah pemulihan, keberhasilan upload saat node mati | Dari log master, di-parse `fault_test.py` |
| **Scalability** | throughput & speedup untuk **N = 2, 3, 4, 8, 16** | Prosedur & anggaran di C3 |

**Kejujuran metodologis (wajib ditulis di laporan):** semua node berbagi satu host (CPU, memori, disk bersama). Skala **tidak akan ideal**. Catat angka mentah apa adanya; analisis Amdahl, bottleneck GIL/disk/9P untuk bab Analisis Ilmiah.

**Formula Amdahl untuk analisis:** dari `S(P)` hitung fraksi serial `s`:
```
S(P) = 1 / (s + (1 - s)/P)   →   s = (1/S(P) - 1/P) / (1 - 1/P)
```
Laporkan `s` per titik ukur, lalu bahas apakah konsisten (kalau tidak, jelaskan penyebab: kontensi disk, bukan serial murni).

## C2. Strategi dua tahap: develop `lite`, uji `full`

| Aspek | Tahap DEV (`lite`) | Tahap UJI (`full`) |
|---|---|---|
| Kapan | Fase 1–3 (Hari 1–5) | Fase 4 (Hari 6) |
| Dataset | 100 × 5 MB sparse | **100 × 50 MB acak asli** |
| Data logis | 0.5 GB | **5 GB** |
| Disk fisik dataset | ≈ 0 | **5 GB** |
| Disk fisik chunk (RF=2) | ≈ 1 GB | **10 GB** |
| Tujuan | Validasi logika, chunking, replikasi, fault recovery | **Angka untuk laporan** |
| Waktu per run | detik | menit |

**Aturan transisi:**
1. Semua fitur harus **bebas bug di `lite`** sebelum menyentuh `full`.
2. Sebelum run `full`: `podman-compose -p dfsx down -v` → `up -d` → `wait-ready` → cek free space VM ≥ 18 GB.
3. Angka `lite` **tidak boleh** masuk grafik yang sama dengan angka `full`.
4. Fault test F1–F6 dijalankan **dua kali**: sekali di `lite` (cepat, untuk debugging), sekali di `full` atau `integrity` (untuk bukti laporan).

## C3. Matriks benchmark (3 tier, terukur waktunya)

Semua run: `time.perf_counter()` dari awal sampai akhir seluruh operasi. Run-1 = warm-up (dibuang), sisanya dicatat.

### Tier 1 — WAJIB: headline `full`, speedup & efficiency vs worker
```
profil = full (100 × 50 MB, acak asli)
N = 3 (konfigurasi default soal)
W ∈ {1, 2, 4, 8}
ulangan = 1 warm-up + 2 recorded (target 3)
operasi = upload, lalu download (verifikasi checksum)
bersihkan antar-run: rm --all → tunggu chunk_count = 0
```
Estimasi: 8–12 run × ~4–5 menit ≈ **45–70 menit** (termasuk cleanup).
Menghasilkan: **speedup, efficiency, throughput (task/s & MiB/s), latency** — 4 parameter soal.

### Tier 2 — WAJIB: skalabilitas sampai 16 node
```
profil = lite (konsisten di SEMUA N — ini kunci validitas perbandingan)
W = 4 (tetap)
N ∈ {2, 3, 4, 8, 16}
ulangan = 1 warm-up + 2 recorded
```
Estimasi: 10–15 run, lite cepat ≈ **20–35 menit**.
Menghasilkan: **scalability (2, 4, 8, 16)** — parameter soal #6.

**Prosedur khusus N = 16 (mode hemat):**
```powershell
python scripts/gen_compose.py --nodes 16 --out docker-compose.gen.yml `
       --dn-mem 128m --dn-cpus 0.25 --dn-workers 4 --client-workers 4
podman-compose -p dfsx -f docker-compose.gen.yml up -d
podman exec dfs-client python -m client.cli wait-ready --min-nodes 16 --timeout 120
```
- Anggaran RAM: 16 × 128m = 2 GB + master 256m + client 384m ≈ **2.7 GB** → muat di machine 3 GB **jika** RSS Python per node ≤ ~80 MB.
- **Wajib** turunkan `GRPC_WORKERS=4` per datanode dan `CLIENT_WORKERS=4`.
- Monitoring **mati**.
- Verifikasi dulu dengan 1 file sebelum menjalankan 100 file.
- **Jika tetap gagal (OOM / timeout):** turun ke `--dn-mem 96m --dn-workers 2`, catat percobaan di `docs/DECISIONS.md`. Jika masih gagal, **laporkan batas maksimum yang tercapai (mis. 12 node) beserta penyebabnya** dan sertakan analisis teoretis untuk 16 node. **Jangan mengarang angka.** Keterbatasan yang dijelaskan dengan data justru bernilai di rubrik Analisis Ilmiah (15%).

### Tier 3 — JIKA WAKTU: cross-validation `full` pada N lain
```
profil = full, W = 4, N ∈ {2, 4}, 2 recorded
```
Estimasi ≈ 20–30 menit. Disk tetap ~15 GB (RF=2 tidak bergantung N). Gunanya: membuktikan tren skalabilitas juga berlaku pada data nyata, bukan hanya lite.

### Skrip orkestrasi: `scripts/bench_matrix.py` (host, Python)

```
python scripts/bench_matrix.py --tier 1
python scripts/bench_matrix.py --tier 2
python scripts/bench_matrix.py --tier 3
```

Alur per kombinasi:
```
gen_compose(N) → up → wait-ready → gen-data (sekali per N)
  untuk W dalam daftar:
    ulangi (1 warm-up + K recorded):
      bench upload  (semua file) → catat
      bench download (semua file, verifikasi checksum) → catat
      rm --all → tunggu chunk_count = 0
down -v
```

**Keluaran wajib:**

| Berkas | Isi |
|---|---|
| `results/bench_runs.csv` | `run_id,ts,tier,profile,op,nodes,rf,workers,files,total_bytes,seconds,tasks_per_sec,throughput_mibps,avg_ms,p50_ms,p95_ms,p99_ms,is_warmup` |
| `results/latency_<run_id>.json` | per metode: `avg,p50,p95,p99,count` |
| `results/env.json` | CPU, RAM, versi Podman, versi Python, ukuran machine, `SPARSE_WRITE`, `FSYNC`, `GRPC_COMPRESSION` |
| `results/efficiency_table.csv` + `.md` | `E_n(N)` (primary) dan `E_w(W)` (sekunder), terpisah jelas |
| `results/amdahl.csv` | `s` per titik ukur |
| `results/SUMMARY.md` | angka kunci siap tempel ke laporan |

### Grafik wajib: `scripts/plots.py` (matplotlib, 150 dpi → `results/plots/`)

| Berkas | Isi |
|---|---|
| `speedup_vs_workers.png` | garis + garis ideal, dari Tier 1 |
| `efficiency_vs_workers.png` | `E_w(W)` |
| `throughput_vs_nodes.png` | upload & download, error bar std, dari Tier 2 |
| `scalability_nodes.png` | `S_n(N)` & `E_n(N)` vs N (2,3,4,8,16) |
| `latency_per_op.png` | bar avg/p95 per metode |
| `tasks_per_sec.png` | throughput task/detik (definisi literal soal) |
| `recovery_timeline.png` | stop → NODE_DOWN → REREPLICATE → CLUSTER_HEALTHY |
| `storage_per_node.png` | distribusi chunk/byte per node (bukti partitioning & load balancing) |
| `access_time.png` | waktu akses (upload/download) per skenario — **diminta eksplisit Tema 5** |

## C4. Skenario fault tolerance — `scripts/fault_test.py` (host, Python)

Skrip memanggil `podman stop/start dfs-dnX`, membaca `podman logs dfs-master`, mengambil timestamp peristiwa. Simpan log ke `results/fault/<id>.log`, ringkasan ke `results/fault_summary.csv`.

| ID | Skenario | Kriteria lulus | Prioritas |
|---|---|---|---|
| **F1** | Matikan `dn-2` **saat upload file besar berjalan** | Upload selesai; `ReallocateChunk`/under-replicated ditangani; setelah pemulihan semua chunk RF=2; checksum download = asli | 🔴 Wajib |
| **F2** | Upload → stop `dn-2` → tunggu pemulihan → stop `dn-1` | Download tetap berhasil dari `dn-3` (bukti replika baru terbentuk); catat detection/recovery time | 🔴 Wajib |
| **F3** | Start `dn-2` kembali | `NODE_UP`, full block report, trimming over-replication setelah `TRIM_GRACE_S`, tidak ada chunk hilang | 🔴 Wajib |
| **F4** | Download saat satu node mati | Berhasil via fallback replika | 🟡 Murah, kerjakan |
| **F5** | Rusakkan 1 chunk di disk datanode (ubah byte, profil `integrity`) | Client mendeteksi `CHECKSUM_MISMATCH`, fallback ke replika lain, file tetap benar | 🟡 Murah, nilai tinggi |
| **F6** | Restart master (`podman restart dfs-master`) | Metadata terbaca dari SQLite; `ls`/download normal setelah heartbeat masuk | 🟡 Murah, kerjakan |

**Variasi untuk analisis:** ulangi F2 dengan **N = 2** (tidak ada target baru → tetap under-replicated, sistem tetap melayani baca) dan **N = 4**. Ini bahan analisis bagus: batas teori RF=2 vs jumlah node.

**Kolom `fault_summary.csv`:**
`scenario,nodes,profile,detection_time_s,recovery_time_s,rereplications,upload_ok,download_ok,checksum_ok,under_replicated_final,lost_final,verdict`

**Untuk video demo (show-stopper):** F1 atau F2 dengan monitoring nyala — tunjukkan panel under-replicated naik lalu turun ke 0.

---

# BAGIAN D — RENCANA FASE 7 HARI

Setiap fase: implementasi → jalankan verifikasi → simpan `docs/evidence/phaseN.md` → commit.

## Fase 1 — Setup & prototipe (Hari 1–2)

**Kerjakan:** A6 (setup), struktur repo, `dfs.proto` lengkap + generate stub, `common/*`, Dockerfile, compose 3 node, registrasi node via heartbeat, `ClusterStatus`, `check_no_print.py`.

**Perintah verifikasi:**
```powershell
podman build -t localhost/dfsx:latest .
podman-compose -p dfsx up -d
podman ps
podman exec dfs-client python -m client.cli wait-ready --min-nodes 3 --timeout 60
podman exec dfs-client python -m client.cli status
podman logs dfs-master | Select-String "HEARTBEAT|NODE_UP"
python scripts/check_no_print.py
```

**Kriteria penerimaan:** `status` menampilkan **3 node ALIVE**; log master mencatat heartbeat tiap ±2 s; `check_no_print` lolos; `podman volume ls` menampilkan 5 volume.

## Fase 2 — Fitur inti (Hari 3–4)

**Kerjakan:** metadata + placement anti-affinity, `StoreChunk`/`RetrieveChunk` streaming, upload/download paralel, CLI penuh, RF=2, checksum SHA-256, SQLite WAL, `gendata` (lite/full/integrity + manifest).

**Perintah verifikasi:**
```powershell
# uji integritas dengan data acak asli
podman exec dfs-client python -m client.cli gen-data --profile integrity
podman exec dfs-client python -m client.cli upload /work/testdata/f001.bin f001.bin
podman exec dfs-client python -m client.cli info f001.bin      # 2 replika/chunk, node BERBEDA
podman exec dfs-client python -m client.cli download f001.bin /work/out.bin
podman exec dfs-client python -m client.cli verify f001.bin
podman exec dfs-client python -m client.cli ls

# persistensi metadata
podman restart dfs-master
podman exec dfs-client python -m client.cli wait-ready --min-nodes 3
podman exec dfs-client python -m client.cli ls                 # file masih ada

# sanity test
python -m pytest tests -q
```

**Kriteria penerimaan:** sha256 `f001.bin` == sha256 `out.bin`; file 20 MB → 3 chunk; tiap chunk 2 replika di node berbeda; restart master tidak menghilangkan file.

**Uji chunking 50 MB (wajib sesuai soal):**
```powershell
podman exec dfs-client python -m client.cli gen-data --profile full --count 1 --size 50
podman exec dfs-client python -m client.cli upload /work/testdata/f001.bin big.bin
podman exec dfs-client python -m client.cli info big.bin       # harus 7 chunk, terakhir 2 MiB
```

## Fase 3 — Fault recovery (Hari 5)

**Kerjakan:** failure detector, replication manager queue-based, block report, `ReallocateChunk`, trimming, GC orphan, `fault_test.py` F1–F6.

**Perintah verifikasi (dev di `lite`, cepat):**
```powershell
podman exec dfs-client python -m client.cli gen-data --profile lite
python scripts/fault_test.py --profile lite --scenarios F1,F2,F3,F4,F6
python scripts/fault_test.py --profile integrity --scenarios F5
```

**Kriteria penerimaan:** F1–F6 lulus; log berisi rantai `NODE_DOWN → REREPLICATE_START/DONE → CLUSTER_HEALTHY` dengan waktu; `status` kembali `under_replicated=0`; `results/fault_summary.csv` terisi.

## Fase 4 — Benchmark & monitoring (Hari 6)

**Kerjakan:** Tier 1 (`full`, headline), Tier 2 (skalabilitas 2/3/4/8/16), Tier 3 jika waktu; Prometheus + Grafana ter-provision; semua plot.

**Perintah verifikasi:**
```powershell
# cluster bersih untuk headline
podman-compose -p dfsx down -v
podman-compose -p dfsx up -d
podman exec dfs-client python -m client.cli wait-ready --min-nodes 3

python scripts/bench_matrix.py --tier 1      # full, N=3, W=1/2/4/8
python scripts/bench_matrix.py --tier 2      # lite, N=2/3/4/8/16
python scripts/plots.py

# monitoring untuk demo (bukan saat benchmark)
podman-compose -p dfsx -f docker-compose.yml -f docker-compose.monitoring.yml up -d
# buka http://localhost:3000
```

**Kriteria penerimaan:** `results/bench_runs.csv` terisi untuk Tier 1 & 2; 9 grafik ada di `results/plots/`; `efficiency_table.md` terisi; dashboard Grafana tampil otomatis; `results/SUMMARY.md` berisi angka kunci.

## Fase 5 — Dokumentasi & pengiriman (Hari 7)

**Kerjakan:** `README.md`, `docs/DECISIONS.md`, `docs/evidence/`, laporan IEEE (.docx + .pdf), PPT, video ≤10 menit, ZIP source.

**Verifikasi akhir (clean-room test):**
```powershell
podman-compose -p dfsx down -v
podman system prune -f
podman build -t localhost/dfsx:latest .
podman-compose -p dfsx up -d
podman exec dfs-client python -m client.cli wait-ready --min-nodes 3
podman exec dfs-client python -m client.cli gen-data --profile integrity
podman exec dfs-client python -m client.cli upload /work/testdata/f001.bin demo.bin
podman exec dfs-client python -m client.cli verify demo.bin
python scripts/check_no_print.py
```

**Kriteria penerimaan:** semua kotak Bagian E tercentang; urutan perintah di README membawa cluster dari nol ke siap pakai; video menunjukkan kill-node + recovery.

---

# BAGIAN E — DEFINITION OF DONE

## E1. Fungsional
- [ ] 1 master + 3 datanode berjalan di Podman, tiap node = 1 container, jaringan khusus `dfsnet`
- [ ] Semua RPC pada B3 terimplementasi **dan dipakai**
- [ ] Upload file 50 MB → **7 chunk** (terakhir 2 MiB), paralel, **RF=2**, replika di **node berbeda** (anti-affinity), checksum SHA-256 diverifikasi
- [ ] Download paralel, gabung chunk, verifikasi checksum per-chunk **dan** per-file, fallback ke replika lain
- [ ] CLI lengkap: `upload`, `download`, `ls`, `rm`, `info`, `status`, `verify`, `wait-ready`, `gen-data`, `bench`
- [ ] Penyimpanan chunk di **named volume** (`dn1_data`, `dn2_data`, `dn3_data`)
- [ ] Heartbeat 2 s; DOWN setelah 10 s; **re-replication otomatis via queue**; node kembali → rejoin + block report
- [ ] Metadata master **persisten** (restart master tidak menghilangkan file)
- [ ] `MIN_FREE_MB` ditolak dengan `DISK_LOW`; tulis chunk atomik (`.tmp` + `os.replace`)
- [ ] Seluruh kode memakai `logging`; `check_no_print.py` lolos
- [ ] Tidak ada `except: pass`; semua `grpc.RpcError` di-log dengan konteks

## E2. Pengujian & benchmark
- [ ] Generator profil `lite` (sparse, dev), `full` (**acak asli**, uji), `integrity` (acak, checksum) + `manifest.json`
- [ ] **Tier 1 selesai:** `full` 100 × 50 MB pada 3 node, W = 1/2/4/8 → speedup, efficiency, throughput, latency
- [ ] **Tier 2 selesai:** skalabilitas N = 2, 3, 4, 8, **16** (profil konsisten di semua N)
- [ ] Throughput dilaporkan dalam **task/detik DAN MiB/s**
- [ ] Efficiency dilaporkan dengan **P = jumlah node** (primary) dan P = worker (sekunder), berlabel jelas
- [ ] Latency: avg + p50 + p95 + p99 per metode, **plus** latency antar-node (`ReplicateChunk`)
- [ ] Fault F1–F6 lulus dengan log bukti + `fault_summary.csv`
- [ ] `results/bench_runs.csv`, `latency_*.json`, `env.json`, `efficiency_table.md`, `amdahl.csv`, `SUMMARY.md`
- [ ] 9 grafik di `results/plots/` (termasuk `access_time.png` yang diminta Tema 5)
- [ ] Sanity test placement / chunking / checksum lolos

## E3. Bonus & dokumentasi
- [ ] Prometheus + Grafana dengan dashboard **ter-provision otomatis** (+5%)
- [ ] `README.md`: cara run di Windows/Podman, dependensi, topologi, contoh perintah, troubleshooting
- [ ] `docs/DECISIONS.md` terisi (semua asumsi & penyimpangan)
- [ ] `docs/evidence/phase1..5.md` terisi keluaran perintah nyata
- [ ] `.gitattributes` LF, `.gitignore` benar, tidak ada `*.bin` / `results/` di git

## E4. Deliverable pengumpulan
- [ ] Laporan `.docx` **dan** `.pdf` (struktur C soal, IEEE-style)
- [ ] PPT presentasi
- [ ] Source code `.zip` (kode + README + results + docs)
- [ ] Video demo **≤ 10 menit**, memuat kill-node + recovery
- [ ] Clean-room test lulus (E Fase 5)

---

# BAGIAN F — PEMETAAN KE KETENTUAN SOAL (untuk lampiran laporan)

| Ketentuan soal | Wujud implementasi | Bukti |
|---|---|---|
| **Tema 5:** 1 master + 3 data node | `docker-compose.yml` 3 datanode + generator N node | `status`, evidence Fase 1 |
| **Tema 5:** replication | `placement.py` RF=2 anti-affinity | `info` per chunk, log `UPLOAD_COMMIT` |
| **Tema 5:** partitioning | chunking 8 MiB + distribusi load-aware (`planned_bytes`) | `storage_per_node.png`, `info` |
| **Tema 5:** fault recovery | `failure.py` + `replication.py` queue | log `NODE_DOWN`/`REREPLICATE_*`/`CLUSTER_HEALTHY`, F1–F6 |
| **Tema 5:** consistency control | 2-phase commit (`Allocate → Store → Commit`) + SHA-256 per chunk & per file | sequence diagram, F5 `CHECKSUM_MISMATCH` |
| **Tema 5:** uji 100 file (50 MB) | `gendata --profile full` = 100 × 50 MB **data acak asli** | `manifest.json`, `bench_runs.csv` Tier 1 |
| **Tema 5 output:** hasil uji upload/download | `bench_runs.csv` (op=upload/download) | CSV + tabel laporan |
| **Tema 5 output:** grafik throughput & waktu akses | `throughput_vs_nodes.png`, `access_time.png`, `tasks_per_sec.png` | PNG 150 dpi |
| **B1:** Master/Coordinator (scheduling, fault detection, data distribution) | `master/server.py` + `placement.py` + `failure.py` | kode + diagram |
| **B2:** Worker Node (komputasi paralel) | `datanode/server.py`, I/O + checksum paralel, thread pool | kode + metrik bytes in/out |
| **B3:** Communication Layer (RPC) | gRPC `MasterService` + `DataNodeService` | `proto/dfs.proto` |
| **B4:** Distributed Storage (container volume) | named volume `dnX_data` | `podman volume ls` |
| **B5:** Monitoring Module | Prometheus + Grafana, panel node/CPU/network | dashboard ter-provision |
| **F1:** Speedup Ratio | `S_w(W)=T(1)/T(W)`, `S_n(N)=T(2)/T(N)` | `speedup_vs_workers.png`, `scalability_nodes.png` |
| **F2:** Efficiency (P = node) | `E_n(N)=S_n(N)/(N/2)` primary; `E_w(W)` sekunder | `efficiency_table.md` |
| **F3:** Throughput (task/detik) | `files_completed/seconds` + MiB/s | `tasks_per_sec.png`, CSV |
| **F4:** Latency (rata-rata antar node) | interceptor client per metode + durasi `ReplicateChunk` | `latency_per_op.png`, `latency_*.json` |
| **F5:** Fault Tolerance Test | `fault_test.py` F1–F6, `podman stop` saat eksekusi | `fault_summary.csv`, log, video |
| **F6:** Scalability 2, 4, 8, 16 | `gen_compose.py --nodes N`, Tier 2 (N=16 mode hemat) | `scalability_nodes.png` |
| **C:** struktur laporan 6 bab | dipetakan di G1 | laporan |
| **D:** source + README | repo + `README.md` | ZIP |
| **E:** presentasi/demo/video ≤10 menit | PPT + MP4 | video |
| **G bonus:** monitoring dashboard | Prometheus + Grafana (+5%) | screenshot |
| **Aturan:** logging bukan print | `common/logging_setup.py` + `check_no_print.py` | hasil check |

---

# BAGIAN G — DELIVERABLE PELAPORAN

## G1. Struktur laporan (sesuai soal Bagian C, format IEEE 2-kolom)

| Bab | Isi | Target halaman |
|---|---|---|
| Judul + Abstrak | 150–200 kata, **sebutkan angka hasil** (throughput, speedup, recovery time) | 1 |
| **1. Pendahuluan** | Latar belakang DFS, masalah storage terpusat & SPOF, rumusan masalah, tujuan, kontribusi | 1–2 |
| **2. Kajian Teori** | GFS/HDFS, replikasi & anti-affinity, CAP theorem, 2-phase commit, Amdahl's law, gRPC & HTTP/2 streaming, container volume & overhead WSL2 | 2 |
| **3. Metodologi & Arsitektur** | Diagram topologi (5 komponen B), sequence diagram upload/download/recovery, pseudocode `pick_targets`, desain queue repair | 2–3 |
| **4. Implementasi** | Stack & alasan Python murni, struktur repo, `dfs.proto`, logging, konfigurasi tanpa `.env`, atomik write, anggaran resource | 3–4 |
| **5. Pengujian & Analisis Performansi** | **6 sub-bab sesuai F.1–F.6** + analisis Amdahl + bottleneck + keterbatasan host | 4–5 |
| **6. Kesimpulan & Saran** | Ringkasan temuan, keterbatasan (master SPOF, RF=2, single-host), saran (multi-master, erasure coding, TLS, K8s) | 1 |
| Daftar Pustaka | minimal 8 referensi, IEEE style | 1 |

**Total target: 12–16 halaman.**

**Sub-bab 5 wajib:**
- 5.1 Speedup Ratio (worker & node)
- 5.2 Efficiency (**P = node** sebagai tabel utama)
- 5.3 Throughput (task/s & MiB/s)
- 5.4 Latency (per metode + antar-node)
- 5.5 Fault Tolerance (F1–F6, detection & recovery time)
- 5.6 Scalability (2, 3, 4, 8, 16)
- 5.7 Analisis Amdahl & bottleneck (GIL, kontensi disk, 9P/WSL2, overhead gRPC)
- 5.8 Keterbatasan pengujian (single host, profil lite untuk N=16, dsb.)

**Kalimat kunci untuk.justifikasi desain (pakai apa adanya kalau ditanya):**
> "Replication Factor 2 dipilih sebagai *resource-aware design* pada host dengan penyimpanan 28 GB. Consistency control dijamin melalui mekanisme two-phase commit dan verifikasi SHA-256 per chunk, bukan oleh jumlah replika. Konfigurasi ini tetap toleran terhadap kegagalan satu node, sebagaimana dibuktikan pada skenario F1–F4."

> "Pengujian skalabilitas hingga 16 node menggunakan profil dataset yang konsisten di seluruh titik ukur agar perbandingan valid. Run headline 100 file × 50 MB menggunakan data acak asli pada konfigurasi 3 node sesuai spesifikasi Tema 5. Seluruh node berbagi satu host fisik, sehingga speedup yang terukur merepresentasikan batas paralelisme pada lingkungan container terkonsolidasi, bukan klaster terdistribusi geografis."

## G2. Struktur PPT (12–15 slide, ≤10 menit)

1. Judul & anggota → 2. Latar belakang → 3. Tujuan → 4. Teori singkat → 5. **Diagram arsitektur (5 komponen)** → 6. Sequence upload (2PC) → 7. Stack & justifikasi → 8. Chunking + RF=2 anti-affinity → 9. Heartbeat + queue repair → 10. **Demo live / video kill-node** → 11. Speedup & efficiency → 12. Throughput & latency → 13. Fault tolerance (timeline) → 14. Scalability 2→16 → 15. Kesimpulan & Q&A

## G3. Video demo (≤10 menit, MP4 H.264 1080p)

| Menit | Isi |
|---|---|
| 0:00–1:00 | Judul, anggota, topologi (`podman ps`, `cli status`) |
| 1:00–2:30 | `gen-data --profile integrity` → `upload` → `info` (tunjukkan 2 replika di node berbeda) |
| 2:30–3:30 | `download` + `verify` → checksum cocok |
| 3:30–6:00 | **Kill-node:** mulai upload → `podman stop dfs-dn2` → tunjukkan log `NODE_DOWN` → `REREPLICATE_DONE` → `CLUSTER_HEALTHY` → Grafana under-replicated naik lalu 0 |
| 6:00–7:00 | `podman start dfs-dn2` → `NODE_UP` → download tetap berhasil |
| 7:00–9:00 | Grafik: speedup, throughput, latency, scalability 2→16 |
| 9:00–10:00 | Kesimpulan + keterbatasan |

**Wajib ada:** bukti visual `podman stop` di tengah eksekusi, dan `under_replicated=0` setelah recovery.

## G4. Isi ZIP pengumpulan

```
DFS-X_Kelompok[N].zip
├── laporan/
│   ├── laporan.docx
│   └── laporan.pdf
├── presentasi/slide.pptx
├── video/demo.mp4              (atau link YouTube di README jika file besar)
└── source/
    ├── (seluruh repo tanpa .git, .venv, *.bin)
    ├── README.md
    ├── results/                (CSV, JSON, PNG — bukti uji)
    └── docs/                   (DECISIONS.md, evidence/)
```

---

# BAGIAN H — COMMAND CHEATSHEET (Podman bawaan, tanpa helper script)

```powershell
# ---------- mesin ----------
podman machine start
podman machine stop
podman info --format "{{.Host.MemTotal}}"

# ---------- build & lifecycle ----------
podman build -t localhost/dfsx:latest .
podman-compose -p dfsx up -d
podman-compose -p dfsx down
podman-compose -p dfsx down -v          # HAPUS volume (reset total)
podman-compose -p dfsx -f docker-compose.yml -f docker-compose.monitoring.yml up -d

# ---------- observasi ----------
podman ps
podman stats --no-stream
podman logs -f dfs-master
podman logs dfs-master | Select-String "NODE_DOWN|REREPLICATE|CLUSTER_HEALTHY"
podman volume ls
podman exec dfs-dn1 du -sh /data/chunks

# ---------- CLI (semua lewat container client) ----------
podman exec dfs-client python -m client.cli status
podman exec dfs-client python -m client.cli wait-ready --min-nodes 3 --timeout 60
podman exec dfs-client python -m client.cli gen-data --profile lite
podman exec dfs-client python -m client.cli gen-data --profile full
podman exec dfs-client python -m client.cli gen-data --profile full --batch 20
podman exec dfs-client python -m client.cli gen-data --profile integrity
podman exec dfs-client python -m client.cli upload /work/testdata/f001.bin f001.bin
podman exec dfs-client python -m client.cli info f001.bin
podman exec dfs-client python -m client.cli download f001.bin /work/out.bin
podman exec dfs-client python -m client.cli verify f001.bin
podman exec dfs-client python -m client.cli ls
podman exec dfs-client python -m client.cli rm f001.bin
podman exec dfs-client python -m client.cli rm --all

# ---------- fault injection ----------
podman stop dfs-dn2
podman start dfs-dn2
podman restart dfs-master

# ---------- benchmark & plot (host) ----------
python scripts/gen_compose.py --nodes 16 --out docker-compose.gen.yml --dn-mem 128m --dn-cpus 0.25 --dn-workers 4
podman-compose -p dfsx -f docker-compose.gen.yml up -d
python scripts/bench_matrix.py --tier 1
python scripts/bench_matrix.py --tier 2
python scripts/fault_test.py --profile lite --scenarios F1,F2,F3,F4,F6
python scripts/plots.py

# ---------- penjaga aturan ----------
python scripts/check_no_print.py
python -m pytest tests -q
ruff check . ; ruff format .

# ---------- kebersihan disk ----------
podman-compose -p dfsx down -v
podman system prune -a
podman volume prune -f
wsl --shutdown
```

---

# BAGIAN I — TROUBLESHOOTING PODMAN DI WINDOWS

| Gejala | Penyebab umum | Solusi |
|---|---|---|
| `podman machine init` gagal | WSL2 belum aktif / virtualisasi mati | `wsl --install`, aktifkan virtualization di BIOS, restart |
| `short-name ... did not resolve` | image tanpa registry | pakai nama penuh `docker.io/library/...` |
| Container tidak saling resolve nama | network default | pastikan `networks: [dfsnet]`; jangan `network_mode: host` |
| `mem_limit` / `cpus` tak berefek | versi `podman-compose` lama | `pip install -U podman-compose`, atau fallback `scripts/cluster.py` |
| Upload lambat sekali | data di bind-mount Windows (9P) | pindahkan dataset & chunk ke **named volume** (`client_work`, `dnX_data`) |
| `RESOURCE_EXHAUSTED` pesan terlalu besar | frame > batas | frame 1 MiB + `max_*_message_length` 16 MB |
| `os.pwrite` tidak ada | client jalan di Windows | client **wajib** di container Linux (`dfs-client`) |
| Disk C: penuh | VHDX WSL membengkak, tidak menyusut otomatis | `down -v` → `podman system prune -a --volumes` → `wsl --shutdown` → set sparse |
| Port 3000/9090/50051 bentrok | aplikasi lain | ubah mapping port di compose |
| Node tak terdeteksi DOWN | heartbeat memakai jam node | pastikan `last_seen` = `time.monotonic()` **master** |
| Node false-DOWN saat beban tinggi | timeout terlalu ketat | naikkan `HEARTBEAT_TIMEOUT_S` ke 15; catat di DECISIONS.md |
| OOM saat N = 16 | limit RAM & thread terlalu besar | `--dn-mem 128m --dn-workers 4`, monitoring mati, profil `lite` |
| `podman-compose` error YAML anchor | versi lama tidak dukung merge key | tulis compose eksplisit tanpa `<<:` (generator bisa emit versi flat) |
| Upload gagal `DISK_LOW` | ruang VM < `MIN_FREE_MB` | prune, atau pakai `--batch 20`, atau turunkan `MIN_FREE_MB` (catat) |
| Checksum mismatch terus | bug offset tulis / frame urutan | cek `os.pwrite` offset = `index*chunk_size`; cek `chunk_id` di frame pertama |
| Chunk yatim menumpuk | upload gagal tanpa GC | pastikan GC orphan jalan pada `full_report` + `UPLOAD_TTL_S` |

---

# LAMPIRAN — Template `docs/DECISIONS.md`

Salin dan isi seiring pengerjaan. Ini yang menyelamatkanmu saat ditanya dosen "kenapa begini?".

```markdown
# Keputusan Desain & Penyimpangan — DFS-X

## Keputusan terkunci
| # | Keputusan | Nilai | Alasan |
|---|---|---|---|
| 1 | Runtime | Podman rootless (Windows + WSL2) | Hanya Podman tersedia; sintaks kompatibel Docker |
| 2 | Bahasa | Python 3.12 murni | Deadline 1 minggu; hindari setup protoc ganda & debug lintas bahasa |
| 3 | RF | 2 | 5 GB × 2 = 10 GB muat di 28 GB; toleran 1 node gagal |
| 4 | Chunk | 8 MiB | 700 chunk / 100 file; hemat ~46% overhead RPC vs 4 MiB |
| 5 | Heartbeat | 2 s / timeout 10 s | WSL2 jitter; 6 s rawan false-positive |
| 6 | Repair | queue-based di master | Heartbeat response tidak andal untuk task; perlu retry eksplisit |
| 7 | Data dev | profil lite (sparse) | Iterasi cepat, disk ≈ 0 |
| 8 | Data uji | profil full (os.urandom, 5 GB nyata) | Benchmark jujur, tidak mengakali I/O |
| 9 | SPARSE_WRITE | 0 | Data acak tidak berlubang; matikan agar hasil jujur |
| 10 | Efficiency primary | P = jumlah node | Sesuai rumus soal F.2 |
| 11 | Throughput | task/detik + MiB/s | Soal F.3 literal task/detik; Tema 5 minta grafik throughput |
| 12 | Skalabilitas N=16 | profil lite + mode hemat (128m, 0.25 cpu, 4 worker) | Batas RAM host; profil konsisten di semua N agar valid |

## Asumsi terhadap soal (perlu diverifikasi)
- [ ] "100 file (50 MB)" dibaca sebagai 100 file × 50 MB = 5 GB logis
- [ ] Docker Compose dianggap memenuhi "Docker Compose + Kubernetes" (K8s opsional/bonus)
- [ ] P pada Efficiency = jumlah worker node (bukan thread CPU)
- [ ] Format laporan IEEE-style bebas (template IEEE Conference)

## Penyimpangan & keterbatasan yang dicatat saat pengerjaan
| Tanggal | Item | Nilai awal | Nilai akhir | Alasan | Dampak ke hasil |
|---|---|---|---|---|---|
| | | | | | |

## Keterbatasan yang diakui di laporan
- Master tunggal = SPOF (tidak ada leader election)
- Semua node pada satu host fisik → kontensi CPU/disk bersama, speedup tidak ideal
- RF=2 hanya toleran 1 node gagal serentak
- N=16 diuji dengan profil lite karena batas RAM host
- Tanpa TLS/auth (di luar cakupan soal)
- FSYNC=0 (page cache) untuk benchmark; dampak durabilitas dibahas terpisah
```

---

## RINGKASAN EKSEKUSI

| Hari | Fokus | Bukti selesai |
|---|---|---|
| 1–2 | Setup Podman, proto, compose, heartbeat | `cli status` → 3 node ALIVE |
| 3–4 | Upload/download, RF=2 anti-affinity, checksum, SQLite | sha256 cocok, `info` 2 replika beda node, restart master aman |
| 5 | Fault recovery queue-based, F1–F6 (di `lite`) | log `NODE_DOWN → REREPLICATE → CLUSTER_HEALTHY` |
| 6 | **Benchmark `full` 100×50MB** + skalabilitas 2→16 + Grafana | `bench_runs.csv`, 9 grafik, dashboard |
| 7 | Laporan, PPT, video, clean-room test | semua kotak Bagian E tercentang |

**Tiga hal yang tidak boleh gagal:** checksum cocok · RF=2 anti-affinity · fault recovery otomatis.
**Tiga hal yang boleh dikorbankan terakhir:** Grafana · Tier 3 · replikasi percobaan ke-3.

---

*Dokumen ini adalah spesifikasi mengikat. Setiap perubahan wajib dicatat di `docs/DECISIONS.md`.*
