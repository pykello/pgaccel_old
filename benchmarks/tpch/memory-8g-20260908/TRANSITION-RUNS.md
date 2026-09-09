# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `56e55e08dcf4af50a3a68b2021b30d0fde05969a71f8fad4740bb51547dd8bef`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 405.161 | 6649.361 | 2403.602 | 18.303 | 6.822 |
| Q2 | 77.479 | 68.236 | 1345.272 | 337.937 | 2.551 | 1.381 |
| Q3 | 430.604 | 330.216 | 3939.540 | 1976.796 | 10.485 | 2.256 |
| Q4 | 209.989 | 182.576 | 2949.705 | 1685.489 | 13.233 | 4.423 |
| Q5 | 374.163 | 243.052 | 4842.277 | 1997.345 | 11.511 | 2.926 |
| Q6 | 79.652 | 5.903 | 1816.369 | 513.654 | 5.329 | 0.287 |
| Q7 | 521.101 | 378.401 | 4598.745 | 2005.950 | 12.218 | 4.231 |
| Q8 | 658.360 | 284.811 | 5292.148 | 1998.868 | 13.262 | 2.262 |
| Q9 | 2174.809 | 1583.725 | 8905.443 | 6388.575 | 18.944 | 2.596 |
| Q10 | 1160.066 | 550.995 | 5006.751 | 3003.827 | 16.393 | 6.366 |
| Q11 | 30.496 | 30.581 | 941.140 | 323.997 | 3.522 | 1.121 |
| Q12 | 341.982 | 168.705 | 3434.824 | 1494.902 | 10.150 | 1.377 |
| Q13 | 1093.312 | 484.663 | 4769.721 | 5595.847 | 32.639 | 1.895 |
| Q14 | 132.674 | 119.459 | 2248.999 | 1124.596 | 6.573 | 0.288 |
| Q15 | 146.978 | 66.255 | 4184.006 | 829.427 | 6.402 | 0.541 |
| Q16 | 213.195 | 197.402 | 1378.175 | 1001.576 | 1.528 | 1.165 |
| Q17 | 272.281 | 214.448 | 4134.933 | 2563.513 | 7.203 | 5.825 |
| Q18 | 733.755 | 573.199 | 8271.662 | 4802.045 | 9.506 | 6.613 |
| Q19 | 217.095 | 108.905 | 4909.323 | 2547.487 | 12.443 | 2.118 |
| Q20 | 211.185 | 129.049 | 3173.749 | 1371.021 | 11.953 | 2.099 |
| Q21 | 682.195 | 646.841 | 13533.714 | 6558.207 | 16.442 | 6.820 |
| Q22 | 375.958 | 211.641 | 1147.882 | 1177.929 | 2.603 | 1.298 |

Cumulative memory peak: **6.960 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: Q11.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-sorted-runs-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
