# DFS-X Benchmark SUMMARY

tier=1
profile=full (representative f001.bin, 50 MiB)
nodes=3

| workers | upload MiB/s | download MiB/s | upload s | speedup | efficiency |
|---|---|---|---|---|---|
| 1 | 11.255 | 21.557 | 4.443 | 1.0 | 1.0 |
| 2 | 27.565 | 44.399 | 1.814 | 2.449 | 1.224 |
| 4 | 26.173 | 43.531 | 1.91 | 2.326 | 0.582 |
| 8 | 66.011 | 45.439 | 0.757 | 5.869 | 0.734 |

Latency (W=4 representative run):
- download: avg=1.795s p50=1.795s p95=1.795s p99=1.795s n=1
- upload: avg=1.040s p50=1.040s p95=1.040s p99=1.040s n=1
