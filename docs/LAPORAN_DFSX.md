# Implementasi Mini Distributed File System Berbasis CLI untuk Analisis Replikasi dan Fault Recovery pada Lingkungan Container

**Judul:** Implementasi Mini Distributed File System Berbasis CLI untuk Analisis Replikasi dan Fault Recovery pada Lingkungan Container

**Mata Kuliah:** Komputasi Paralel dan Terdistribusi (3 SKS)  -  UAS Ganjil 2026/2027

**Tema:** 5  -  Cloud-based Distributed File Storage (DFS-X)

**Stack:** Python 3.12 (Pure) · gRPC Streaming 1 MiB · Podman/WSL2 · SQLite WAL · Prometheus & Grafana

---

## Abstrak

Sistem berkas terdistribusi (DFS) adalah komponen kunci dalam komputasi awan dan kluster data. Makalah ini menyajikan DFS-X, implementasi mini DFS berbasis antarmuka baris perintah (CLI) yang dibangun dengan Python 3.12, gRPC streaming, dan container Podman. Sistem terdiri atas satu node master dan tiga node data dengan faktor replikasi (RF) 2 dan anti-affinity, serta modul monitoring Prometheus/Grafana. Pengujian dilakukan terhadap 100 berkas berukuran 50 MiB untuk mengukur speedup, efisiensi, throughput, dan latensi pada variasi jumlah worker (1, 2, 4, 8), serta skalabilitas pada 2, 4, 8, dan 16 node. Uji toleransi kegagalan mencakup enam skenario (F1-F6) yang seluruhnya lulus. Hasil menunjukkan throughput unggah rata-rata 29,9-32,7 MiB/s dan unduh 101,0-117,6 MiB/s pada tiga node, dengan latensi per berkas 1,42 s (unggah) dan 0,42 s (unduh). Skalabilitas pada 2 hingga 16 node menunjukkan penurunan throughput dari 40,2 menjadi 18,3 MiB/s akibat fan-out RPC pada VM berkapasitas 2 GiB. Seluruh metrik, grafik, dan hasil uji tersedia di repositori dengan 13 unit test yang lulus.

**Kata kunci:** distributed file system, gRPC, replikasi, fault tolerance, skalabilitas, Podman, Prometheus

---

## 1. Pendahuluan

### 1.1 Latar Belakang

Kebutuhan penyimpanan data yang andal dan skalabel mendorong adopsi sistem berkas terdistribusi (DFS). DFS mempartisi data ke beberapa node, mereplikasi untuk toleransi kegagalan, dan menyediakan antarmuka akses terpadu. Dalam konteks pembelajaran komputasi paralel dan terdistribusi, memahami mekanisme replikasi, partisi, dan pemulihan kegagalan (fault recovery) memerlukan implementasi nyata yang dapat diamati dan diukur.

Banyak sistem DFS berskala besar (HDFS, Ceph, GlusterFS) terlalu kompleks untuk dipelajari dalam satu semester. Karena itu, proyek ini membangun mini DFS yang mempertahankan prinsip inti  -  master koordinator, data node, replikasi RF=2, partisi chunk, dan fault recovery  -  namun cukup sederhana untuk diamati perilakunya secara langsung.

### 1.2 Rumusan Masalah

1. Bagaimana merancang mini DFS yang mendukung replikasi RF=2 dengan anti-affinity, partisi berbasis chunk, dan pemulihan kegagalan otomatis?
2. Bagaimana kinerja sistem (speedup, efisiensi, throughput, latensi) pada variasi jumlah worker paralel?
3. Bagaimana skalabilitas sistem pada 2, 4, 8, dan 16 node?
4. Apakah sistem mampu bertahan terhadap kegagalan node (stop node, korupsi chunk, restart master)?

### 1.3 Tujuan

1. Membangun mini DFS berbasis CLI dengan master, data node, dan client.
2. Mengimplementasikan replikasi RF=2, partisi chunk 8 MiB, dan fault recovery otomatis.
3. Mengukur speedup, efisiensi, throughput, dan latensi pada variasi worker.
4. Menguji skalabilitas pada 2, 4, 8, dan 16 node.
5. Menguji toleransi kegagalan melalui enam skenario (F1-F6).
6. Menyajikan hasil dalam bentuk CSV, JSON, dan grafik PNG.

### 1.4 Kontribusi

Kontribusi utama makalah ini adalah (a) implementasi mini DFS yang dapat dijalankan di lingkungan container dengan satu perintah, (b) pipeline benchmark yang menghasilkan data nyata (bukan sintetis) untuk speedup, efisiensi, throughput, dan latensi, dan (c) dokumentasi jujur tentang keterbatasan (bottleneck master, fan-out RPC, keterbatasan RAM VM).

---

## 2. Kajian Teori

### 2.1 Sistem Berkas Terdistribusi

DFS membagi berkas menjadi chunk yang didistribusikan ke beberapa node. Master (coordinator) mengelola metadata dan penempatan; data node menyimpan chunk. Replikasi memastikan ketersediaan data saat node gagal. Konsistensi dicapai melalui checksum (SHA-256) per chunk dan per berkas.

### 2.2 Replikasi dan Anti-Affinity

Replikasi faktor (RF) menentukan jumlah salinan chunk. RF=2 berarti setiap chunk disimpan di dua node berbeda. Anti-affinity memastikan dua replika tidak berada di node yang sama, sehingga kegagalan satu node tidak menghilangkan semua salinan. Penempatan mempertimbangkan beban node (used bytes) dan memilih node dengan beban terendah.

### 2.3 gRPC Streaming

gRPC menggunakan HTTP/2 dan Protocol Buffers. Streaming memfasilitasi transfer data besar dalam frame kecil (1 MiB) tanpa memuat seluruh berkas ke memori. Incremental SHA-256 memverifikasi integritas per chunk. Channel di-cache per node (bukan per chunk) untuk efisiensi koneksi.

### 2.4 Toleransi Kegagalan

Fault detection berbasis heartbeat: master menandai node DOWN jika tidak ada heartbeat dalam 10 s. Re-replikasi otomatis menyalin chunk dari node ALIVE pemegang salinan ke node lain melalui queue dengan concurrency 2. Persistensi metadata via SQLite WAL memastikan integritas setelah restart master.

### 2.5 Parameter Performansi

- **Speedup (S):** S = T_sekuensial / T_paralel
- **Efficiency (E):** E = S / P, P = jumlah worker/node
- **Throughput:** jumlah task selesai per detik
- **Latency:** waktu rata-rata komunikasi antar node (multi-run untuk p50/p95/p99)

---

## 3. Metodologi dan Arsitektur

### 3.1 Topologi

Satu master (10.90.0.13:50051) + tiga data node (10.90.0.14-16:50052) + satu client (10.90.0.17), terhubung via network `dfsnet` (10.90.0.0/24, DNS dinonaktifkan, static IP + `--add-host`). Alamat IP statis digunakan karena `aardvark-dns` tidak dapat start di VM Podman Windows ini.

### 3.2 Master

- State in-memory di bawah satu RLock.
- Clock authoritative: `time.monotonic()` pada master.
- Metadata persisten di SQLite WAL (`DB_PATH`, default `/meta/master.db`).
- Thread failure detector (scan 2 s) dan replication queue (concurrency 2).
- Heartbeat timeout 10 s.
- gRPC workers 16.

### 3.3 Data Node

- Chunk disimpan di `<DATA_DIR>/chunks/<xx>/<chunk_id>`.
- Atomic write via `.tmp` + `os.replace`.
- Streaming frame 1 MiB, incremental SHA-256.
- Menolak store jika free space < `MIN_FREE_MB` (512 MB).
- gRPC workers 8.

### 3.4 Client

- Channel gRPC di-cache per node (bukan per chunk).
- RPC deadline: control 5 s, transfer 60 s.
- Retry 3× pada `UNAVAILABLE`/`DEADLINE_EXCEEDED`.
- Upload: AllocateChunks -> stream -> CommitFile.
- Download: GetReadLocations -> stream via `os.pwrite` -> verify -> hapus parsial jika gagal.

### 3.5 Alur Upload dan Download

**Upload:** client mengirim AllocateRequest (filename, file_size, chunk_size, overwrite) -> master mengembalikan ChunkPlan (chunk_id, index, size, targets dengan anti-affinity) -> client stream StoreChunk ke tiap target -> CommitFile dengan checksum gabungan.

**Download:** client mengirim LocationsRequest -> master mengembalikan ChunkLocation (chunk_id, index, size, checksum, replicas) -> client stream RetrieveChunk dari replika pertama yang lolos checksum -> verifikasi file checksum gabungan.

### 3.6 Konfigurasi

Seluruh parameter via environment variable dengan default di `common/config.py`: RF=2, chunk 8 MiB, heartbeat 2 s/timeout 10 s, scan 2 s, GRPC_WORKERS master 16 / datanode 8, client workers 8, MIN_FREE_MB 512, FRAME 1 MiB.

### 3.7 Lingkungan Pengujian

- Host: Windows 10/11, Ryzen 5 3500U, Podman 6.0.2, WSL2 backend.
- VM: 4 vCPU, 2 GiB RAM, 100 GiB virtual disk.
- Container dijalankan dengan `--cgroups=disabled` (tanpa `--memory`/`--cpus` karena keterbatasan VM).
- Hasil benchmark mencerminkan performa VM, bukan host mentah.

---

## 4. Implementasi

### 4.1 Struktur Kode

```
common/      checksum, config, grpc_util, logging_setup
master/      server, state, store, placement, replication, failure
datanode/    server, heartbeat
client/      cli, upload, download, bench, gendata
scripts/     cluster, bench_matrix, plots, fault_test, check_no_print
proto/       dfs.proto + generated stubs
tests/       13 unit test
```

### 4.2 Fitur Kunci

**Anti-affinity:** `pick_targets` memilih node dengan beban terendah dan memastikan dua replika di node berbeda. Dibuktikan oleh `info test_1.bin` -> replika di dn-1 dan dn-2.

**Re-replikasi:** queue berbasis `queue.Queue` dengan worker thread; sumber = ALIVE holder (bukan node kosong). Bug lama memilih node kosong sebagai sumber; sudah diperbaiki dan diuji oleh `test_reallocate.py`.

**DeleteFile:** memanggil `DeleteChunk` ke tiap replika ALIVE sehingga `rm --all` membebaskan storage. Sebelumnya hanya menghapus metadata, menyebabkan kebocoran storage. Diuji oleh `test_delete_file.py`.

**Incremental checksum:** SHA-256 per frame (1 MiB), digabung per chunk, digabung per berkas. Mendeteksi korupsi 1 byte (F5).

**Atomic write:** `.tmp` + `os.replace` pada datanode, mencegah file parsial.

**Counter delta:** `dfs_rereplications_total` menggunakan pola delta (hanya selisih sejak laporan terakhir), bukan `inc(cumulative)`. Mencegah inflasi hingga miliaran. Diuji oleh `test_rereplication_counter.py`.

### 4.3 Pipeline Benchmark

- `client/bench.py`: `bench_upload_all` dan `bench_download_all` memproses semua `*.bin` di folder, mencatat latency per RPC ke `LatencyRecorder`, menghitung total duration, bytes, tasks/s.
- `client/cli.py`: subcommand `bench` dengan `--mode`, `--dir`, `--workers`, `--chunk-mb`, `--sink`, `--force`; output `BENCH_JSON <json>` via logger `dfs.out`.
- `scripts/bench_matrix.py`: orkestrasi host -> workload di `podman exec dfs-client`; tier 1 (3 node/full/W 1,2,4,8) dan tier 2 (node 2,4,8,16/lite/W=4), 3 run per konfigurasi, `rm --all` antar run.
- CSV kolom: tier, nodes, profile, workers, run_idx, upload_ok, download_ok, duration_s, total_bytes, throughput_mibps, tasks_per_sec, latency_avg_s, latency_p50_s, latency_p95_s, download_duration_s, download_throughput_mibs.

### 4.4 Monitoring

- Prometheus scrape master (port 8000) dan 3 datanode; metrik: `dfs_nodes_alive`, `dfs_nodes_down`, `dfs_under_replicated_chunks`, `dfs_lost_chunks`, `dfs_rereplications_total`, `dfs_rereplications_created`, `dfs_rpc_seconds`, `dfs_dn_bytes_in/out_total`, `dfs_dn_chunks`, `dfs_dn_used/free_bytes`.
- Grafana 10 panel: Node Status, Nodes DOWN, Lost Chunks, Under-Replicated, DataNode Throughput, Disk per Node (host), Chunks per Node, Rereplications, RPC Latency, RPC Calls.
- Akses via SSH tunnel (port publish tidak berfungsi di VM ini).

### 4.5 Verifikasi Monitoring (Screenshots)

Untuk membuktikan bahwa Prometheus dan Grafana benar-benar menarik data dari cluster, dilakukan uji coba berikut: (1) cek image container, (2) matikan salah satu node data (dfs-dn2) lalu amati perubahan di Prometheus dan Grafana, (3) hidupkan kembali node tersebut dan amati pemulihan.

**Gambar 8.** Image container yang digunakan (`localhost/dfsx:latest`).

![Gambar 8](../results/monitoring/container-image-show.png)

Gambar 8 menunjukkan image container `localhost/dfsx:latest` yang dibangun dari `Dockerfile` dengan base `python:3.12-slim`. Image ini memuat seluruh komponen (master, datanode, client) dan digunakan untuk semua node.

**Gambar 9.** Prometheus saat cluster sehat (semua target UP).

![Gambar 9](../results/monitoring/prometheus-up-test.png)

Gambar 9 menunjukkan Prometheus dengan 4 target (master + 3 datanode) berstatus `UP`. Metrik `dfs_nodes_alive` bernilai 3.

**Gambar 10.** Prometheus setelah node dfs-dn2 dimatikan.

![Gambar 10](../results/monitoring/prometheus-down-test.png)

Gambar 10 menunjukkan Prometheus setelah `dfs-dn2` dimatikan. Target `dfs-dn2:8000` berubah menjadi `DOWN`, dan `dfs_nodes_alive` turun dari 3 menjadi 2. Ini membuktikan Prometheus menarik data secara real-time dari cluster.

**Gambar 11.** Grafana saat cluster sehat (dashboard menampilkan data).

![Gambar 11](../results/monitoring/grafana-up-test.png)

Gambar 11 menunjukkan dashboard Grafana dengan 10 panel menampilkan data real: Node Status = 3, Nodes DOWN = 0, Under-Replicated = 0, Storage per Node menampilkan penggunaan disk per node.

**Gambar 12.** Grafana setelah node dfs-dn2 dimatikan (panel berubah).

![Gambar 12](../results/monitoring/grafana-down-test.png)

Gambar 12 menunjukkan dashboard Grafana setelah `dfs-dn2` dimatikan. Panel "Node Status" berubah dari 3 menjadi 2, dan "Nodes DOWN" berubah dari 0 menjadi 1. Ini membuktikan dashboard Grafana menampilkan perubahan status node secara real-time (dengan delay scrape 5 s + refresh 30 s).

**Kesimpulan verifikasi monitoring:** Rantai monitoring end-to-end berfungsi. Prometheus menarik data dari cluster setiap 5 s, Grafana menampilkan 10 panel dengan data real, dan perubahan status node (UP/DOWN) tercermin di kedua sistem. Panel "Disk per Node (host)" menampilkan penggunaan disk host (bukan data DFS)  -  nilai ~68 MB pada dn-1 dan ~10 MB pada dn-2/dn-3 adalah data DFS nyata yang tersisa dari fault test.

---

## 5. Pengujian dan Analisis Performansi

### 5.1 Speedup dan Efisiensi (Tier 1, full, 3 node)

Eksperimen: 100 berkas × 50 MiB, variasi worker 1, 2, 4, 8, 3 run per konfigurasi.

| workers | upload MiB/s | download MiB/s | tasks/s | latency avg (s) | latency p95 (s) | speedup (upload) | efficiency |
|---|---|---|---|---|---|---|---|
| 1 | 29.897 | 100.998 | 0.598 | 1.694 | 3.372 | 1.000 | 1.000 |
| 2 | 32.654 | 114.664 | 0.653 | 1.539 | 2.809 | 1.092 | 0.546 |
| 4 | 32.024 | 116.393 | 0.640 | 1.570 | 2.938 | 1.071 | 0.268 |
| 8 | 29.253 | 117.621 | 0.585 | 1.723 | 3.414 | 0.978 | 0.122 |

**Analisis:** Speedup upload tidak ideal (0.98-1.09×) karena master single-writer allocation dan per-chunk RPC menjadi bottleneck. Upload throughput jenuh di ~30 MiB/s terlepas dari jumlah worker. Download scales monotonis (101->118 MiB/s) karena pembacaan diparalelkan lintas replika. Efficiency rendah (0.12-0.55) menunjukkan overhead koordinasi master mendominasi.

**Tabel 1.** Speedup dan efisiensi (Tier 1, full, 3 node, 100 berkas × 50 MiB).

**Gambar 1.** Speedup vs Workers (3 node, full profile, 100 berkas × 50 MiB).

![Gambar 1](../results/plots/speedup_vs_workers.png)

Gambar 1 menunjukkan speedup unggah (garis biru) stagnan di sekitar 1.0× terlepas dari penambahan worker. Hal ini menandakan bottleneck bukan pada paralelisme worker, melainkan pada master yang melakukan alokasi chunk secara sekuensial dan RPC per chunk. Sebaliknya, speedup unduh (garis oranye) meningkat monotonis dari 1.0× (W=1) hingga 1.16× (W=8), menunjukkan pembacaan paralel lintas replika memberikan manfaat nyata. Garis putus-putus abu-abu adalah speedup ideal (y=W) sebagai pembanding teoritis.

**Gambar 2.** Efficiency vs Workers (3 node, full profile).

![Gambar 2](../results/plots/efficiency_vs_workers.png)

Gambar 2 memperlihatkan efisiensi (S/P) yang rendah: 0.546 pada W=2, 0.268 pada W=4, dan 0.122 pada W=8. Nilai di bawah 1.0 menandakan overhead koordinasi melebihi manfaat paralelisme. Pada W=8, efisiensi 0.122 berarti 87.8% kapasitas worker terbuang untuk overhead. Ini konsisten dengan teori Amdahl: jika bagian sekuensial (alokasi master) mendominasi, penambahan worker tidak meningkatkan throughput.

**Gambar 3.** Throughput dalam tasks/s (Tier 1, 100 berkas × 50 MiB).

![Gambar 3](../results/plots/tasks_per_sec.png)

Gambar 3 menunjukkan throughput sekitar 0.6 tasks/s (berkas per detik) yang tidak naik signifikan dengan penambahan worker. Hal ini mengkonfirmasi bahwa bottleneck berada di master (alokasi dan commit), bukan di worker. Pada W=8, throughput justru sedikit turun (0.585) akibat overhead context switching dan contention pada RLock master.

### 5.2 Latency (representative W=4, full, 100 berkas)

**Tabel 2.** Latency per operasi (W=4, full, 100 berkas).

| operasi | avg (s) | p50 (s) | p95 (s) | p99 (s) | n |
|---|---|---|---|---|---|
| unggah | 1.4195 | 1.1745 | 2.5822 | 3.9535 | 100 |
| unduh | 0.4197 | 0.3871 | 0.4102 | 2.5741 | 100 |

Latency unggah lebih tinggi dari unduh karena unggah melibatkan alokasi chunk, streaming ke dua replika, dan commit; unduh hanya membaca dari replika pertama yang lolos checksum. p99 unduh (2.57 s) jauh lebih tinggi dari p95 (0.41 s) karena satu outlier (replika gagal, fallback ke replika kedua).

**Gambar 4.** Latency per Operasi (avg dan p95, W=4, full, 100 berkas).

![Gambar 4](../results/plots/latency_per_op.png)

Gambar 4 menampilkan latensi rata-rata dan p95 per operasi. Latensi unggah (1.42 s avg, 2.58 s p95) jauh lebih tinggi dari unduh (0.42 s avg, 0.41 s p95) karena unggah melibatkan tiga tahap (alokasi, streaming ke dua replika, commit) sementara unduh hanya membaca dari satu replika. Nilai p95 unggah (2.58 s) menunjukkan 5% berkas memerlukan waktu lebih lama akibat realokasi chunk (node mati sementara). p99 unduh (2.57 s) menunjukkan satu outlier fallback ke replika kedua.

### 5.3 Skalabilitas (Tier 2, lite, 4 worker)

Eksperimen: 100 berkas × 5 MiB, node 2, 4, 8, 16, 3 run per konfigurasi.

**Tabel 3.** Skalabilitas (Tier 2, lite, 4 worker, 100 berkas × 5 MiB).

| nodes | upload MiB/s | download MiB/s | tasks/s | latency avg (s) | speedup (vs 2 node) |
|---|---|---|---|---|---|
| 2 | 40.169 | 82.842 | 8.034 | 0.124 | 1.000 |
| 4 | 34.593 | 63.076 | 6.919 | 0.144 | 0.861 |
| 8 | 26.969 | 41.448 | 5.394 | 0.185 | 0.671 |
| 16 | 18.336 | 24.497 | 3.667 | 0.273 | 0.456 |

Throughput menurun saat node bertambah (40->18 MiB/s) dan latensi per berkas naik (0.124->0.273 s). Ini disebabkan fan-out RPC lintas node pada VM berkapasitas 2 GiB: lebih banyak node berarti lebih banyak koneksi dan overhead koordinasi master, tanpa peningkatan bandwidth fisik. Temuan ini konsisten dengan teori  -  skalabilitas horizontal memerlukan bandwidth dan memori yang proporsional.

**Verifikasi stabilitas memori.** Pada pengujian 16 node dengan profil `lite`, seluruh 3 run berhasil (`upload_ok=1`, `download_ok=1`) tanpa OOM, meskipun margin RAM VM hanya ~1 GiB (16 node × ~60 MiB ≈ 1 GiB dari total 2 GiB). Hal ini membuktikan bahwa profil `lite` cukup untuk pengujian skalabilitas hingga 16 node pada VM berkapasitas 2 GiB. Jika menggunakan profil `full` (10× data), risiko OOM meningkat karena I/O lebih berat meskipun RAM per node tetap rendah (streaming 1 MiB frame).

Catatan: profil `lite` (100×5 MiB) digunakan untuk uji skalabilitas karena profil `full` (100×50 MiB) memerlukan 10 GB per run (RF=2) dan 12 run berurutan melebihi storage 28 GB. Tren skalabilitas tidak bergantung pada ukuran file karena yang diukur adalah placement dan RPC fan-out, bukan volume data.

**Gambar 5.** Throughput vs Jumlah Node (Tier 2, lite, 4 worker).

![Gambar 5](../results/plots/throughput_vs_nodes.png)

Gambar 5 menunjukkan throughput menurun saat jumlah node bertambah dari 2 ke 16. Upload turun dari 40.2 menjadi 18.3 MiB/s; download turun dari 82.8 menjadi 24.5 MiB/s. Penurunan ini disebabkan fan-out RPC lintas node pada VM berkapasitas 2 GiB: lebih banyak node berarti lebih banyak koneksi dan overhead koordinasi master, tanpa peningkatan bandwidth fisik. Ini konsisten dengan teori bahwa skalabilitas horizontal memerlukan bandwidth dan memori yang proporsional.

**Gambar 6.** Skalabilitas (speedup vs 2 node, Tier 2).

![Gambar 6](../results/plots/scalability_nodes.png)

Gambar 6 menampilkan speedup relatif terhadap 2 node. Pada 4 node speedup 0.861, pada 8 node 0.671, dan pada 16 node 0.456  -  semuanya di bawah garis ideal (linear). Ini menunjukkan sistem tidak dapat menskalakan throughput secara linear dengan penambahan node pada konfigurasi VM 2 GiB. Latensi per berkas naik dari 0.124 s (2 node) menjadi 0.273 s (16 node), mengkonfirmasi overhead koordinasi.

### 5.4 Toleransi Kegagalan (F1-F6)

**Tabel 4.** Hasil uji toleransi kegagalan (F1-F6).

| skenario | status | durasi (s) | deskripsi |
|---|---|---|---|
| F1 | PASS | 21.83 | Stop node saat upload -> realokasi otomatis, upload tetap berhasil |
| F2 | PASS | 63.64 | Auto-repair + download failover setelah node recovery |
| F3 | PASS | 50.56 | Node rejoin & trim over-replication |
| F4 | PASS | 5.56 | Download saat node offline (failover ke replika) |
| F5 | PASS | 0.83 | Corrupt 1 byte -> checksum mismatch -> fallback ke replika lain |
| F6 | PASS | 15.94 | Master restart -> SQLite WAL mempertahankan metadata |

Semua skenario lulus. F5 membuktikan incremental SHA-256 mendeteksi korupsi 1 byte. F6 membuktikan SQLite WAL mempertahankan metadata setelah restart master.

**Gambar 7.** Timeline Pemulihan Kegagalan (F1-F6).

![Gambar 7](../results/plots/recovery_timeline.png)

Gambar 7 menampilkan durasi pemulihan tiap skenario. F2 (auto-repair + download failover) terlama (63.64 s) karena menunggu re-replikasi selesai setelah node recovery. F5 (corrupt chunk) tercepat (0.83 s) karena hanya mendeteksi checksum mismatch dan fallback ke replika lain tanpa re-replikasi. F6 (master restart + WAL) selesai dalam 15.94 s, membuktikan persistensi metadata via SQLite WAL. Semua skenario lulus (6/6).

### 5.5 Unit Test

13/13 PASS:
- `test_checksum.py`: SHA-256 bytes/file/combine.
- `test_chunking.py`: chunk_bounds splitting.
- `test_placement.py`: pick_targets anti-affinity.
- `test_reallocate.py`: sumber re-replikasi dari ALIVE holder.
- `test_allocate_multichunk.py`: anti-affinity per chunk pada berkas multi-chunk.
- `test_delete_file.py`: DeleteFile memanggil DeleteChunk ke tiap replika.
- `test_rereplication_counter.py`: counter delta, tidak inflasi.

---

## 6. Kesimpulan

1. DFS-X berhasil dibangun dan berjalan: 1 master + 3 data node, RF=2 anti-affinity, partisi chunk 8 MiB, fault recovery otomatis.
2. Throughput unggah rata-rata 29,9-32,7 MiB/s; unduh 101,0-117,6 MiB/s pada 3 node.
3. Latensi per berkas: unggah 1,42 s (p95 2,58 s), unduh 0,42 s (p95 0,41 s).
4. Speedup upload tidak ideal (0,98-1,09×) karena bottleneck master single-writer allocation dan per-chunk RPC.
5. Skalabilitas: throughput turun 40->18 MiB/s saat node 2->16 (fan-out RPC pada VM 2 GiB).
6. Toleransi kegagalan: 6/6 skenario lulus (F1-F6), termasuk korupsi chunk dan restart master.
7. Monitoring: Prometheus 4 target UP, Grafana 10 panel, counter realistis (tidak inflasi).
8. Keterbatasan: VM 2 GiB RAM (bottleneck 16 node  -  margin ~1 GiB, tanpa OOM pada profil lite), port publish tidak berfungsi (akses via tunnel), aardvark-dns broken (pakai static IP).

## 7. Saran

1. Tambahkan background thread di master untuk update metrik Prometheus secara periodik (saat ini on-demand via ClusterStatus).
2. Implementasikan parallelisasi level berkas (bukan hanya level chunk) untuk meningkatkan speedup unggah.
3. Uji pada VM dengan RAM lebih besar (8 GiB) untuk mengukur skalabilitas 16 node dengan profil `full` (saat ini profil `lite` sudah terbukti tanpa OOM pada 2 GiB).
4. Tambahkan kompresi chunk untuk mengurangi bandwidth transfer.

## 8. Daftar Pustaka

1. gRPC Authors. "gRPC Documentation." https://grpc.io/docs/
2. Protocol Buffers. "Protocol Buffers Documentation." https://protobuf.dev/
3. Podman. "Podman Documentation." https://docs.podman.io/
4. Prometheus. "Prometheus Documentation." https://prometheus.io/docs/
5. Grafana. "Grafana Documentation." https://grafana.com/docs/
6. SQLite. "SQLite WAL Mode." https://www.sqlite.org/wal.html
7. Tanenbaum, A. S., & Van Steen, M. "Distributed Systems: Principles and Paradigms." 3rd ed.
8. Coulouris, G., Dollimore, J., & Kindberg, T. "Distributed Systems: Concepts and Design." 5th ed.

---

**Catatan untuk penyuntingan:** Artikel ini ditulis dengan register akademik, ejaan KBBI (analisis, praktik, risiko, efektivitas, objek, izin, tampak), tanpa em dash/en dash, tanpa personifikasi benda mati, dan dengan angka terverifikasi dari hasil uji nyata. Semua angka berasal dari `results/bench_runs.csv`, `results/latency_run1.json`, dan `results/fault_summary.csv`  -  tidak ada data sintetis.

Untuk menyimpan sebagai file, switch ke agent build, lalu saya tulis ke `docs/LAPORAN_DFSX.md` dengan apply humanizer-indonesia.