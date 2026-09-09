# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `ff6f039e60a087d9a80b8aa3d18352fe7bbb11d7ee2c9739877a3fe686690534`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 390.854 | 6649.361 | 2403.602 | 18.193 | 6.823 |
| Q2 | 77.479 | 68.649 | 1345.272 | 337.937 | 2.529 | 1.367 |
| Q3 | 430.604 | 328.505 | 3939.540 | 1976.796 | 10.374 | 2.243 |
| Q4 | 209.989 | 184.893 | 2949.705 | 1685.489 | 13.348 | 4.410 |
| Q5 | 374.163 | 219.404 | 4842.277 | 1997.345 | 11.350 | 2.980 |
| Q6 | 79.652 | 5.281 | 1816.369 | 513.654 | 5.277 | 0.220 |
| Q7 | 521.101 | 333.422 | 4598.745 | 2005.950 | 12.115 | 4.404 |
| Q8 | 658.360 | 281.074 | 5292.148 | 1998.868 | 13.210 | 2.231 |
| Q9 | 2174.809 | 1387.493 | 8905.443 | 6388.575 | 19.029 | 2.808 |
| Q10 | 1160.066 | 546.810 | 5006.751 | 3003.827 | 16.395 | 6.331 |
| Q11 | 30.496 | 31.494 | 941.140 | 323.997 | 3.533 | 1.142 |
| Q12 | 341.982 | 153.065 | 3434.824 | 1494.902 | 10.101 | 1.359 |
| Q13 | 1093.312 | 476.574 | 4769.721 | 5595.847 | 32.872 | 1.876 |
| Q14 | 132.674 | 123.623 | 2248.999 | 1124.596 | 6.579 | 0.271 |
| Q15 | 146.978 | 64.608 | 4184.006 | 829.427 | 6.392 | 0.538 |
| Q16 | 213.195 | 203.842 | 1378.175 | 1001.576 | 1.511 | 1.157 |
| Q17 | 272.281 | 270.780 | 4134.933 | 2563.513 | 7.262 | 5.807 |
| Q18 | 733.755 | 728.317 | 8271.662 | 4802.045 | 9.688 | 6.594 |
| Q19 | 217.095 | 105.509 | 4909.323 | 2547.487 | 12.207 | 2.087 |
| Q20 | 211.185 | 126.473 | 3173.749 | 1371.021 | 12.052 | 2.027 |
| Q21 | 682.195 | 640.951 | 13533.714 | 6558.207 | 16.468 | 6.774 |
| Q22 | 375.958 | 212.145 | 1147.882 | 1177.929 | 2.615 | 1.314 |

Cumulative memory peak: **6.914 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: Q11.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-compact-width-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
