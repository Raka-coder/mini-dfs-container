# DFS-X Benchmark SUMMARY

tier=1
profile=full (representative f001.bin, 50 MiB)
nodes=3

| workers | upload MiB/s | download MiB/s | upload s | speedup | efficiency |
|---|---|---|---|---|---|
| 1 | 10.122 | 35.492 | 4.94 | 1.0 | 1.0 |
| 2 | 19.899 | 43.674 | 2.513 | 1.966 | 0.983 |
| 4 | 49.369 | 30.765 | 1.013 | 4.877 | 1.219 |
| 8 | 21.715 | 37.086 | 2.303 | 2.145 | 0.268 |

Latency (W=4 representative run):
- download: avg=1.595s p50=1.595s p95=1.595s p99=1.595s n=1
- upload: avg=0.968s p50=0.968s p95=0.968s p99=0.968s n=1
