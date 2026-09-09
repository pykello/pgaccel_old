# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `840b995dfb4a480ead98bdaf9bdcd4906485fbb83c8da0f156fba08dfab5c983`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 356.617 | 6649.361 | 2403.602 | 28.390 | 6.870 |
| Q2 | 77.479 | 70.659 | 1345.272 | 337.937 | 2.607 | 1.341 |
| Q3 | 430.604 | 228.366 | 3939.540 | 1976.796 | 10.990 | 2.489 |
| Q4 | 209.989 | 156.901 | 2949.705 | 1685.489 | 13.729 | 4.767 |
| Q5 | 374.163 | 219.301 | 4842.277 | 1997.345 | 11.227 | 3.028 |
| Q6 | 79.652 | 5.440 | 1816.369 | 513.654 | 5.377 | 0.427 |
| Q7 | 521.101 | 363.585 | 4598.745 | 2005.950 | 12.617 | 4.235 |
| Q8 | 658.360 | 301.017 | 5292.148 | 1998.868 | 13.310 | 2.117 |
| Q9 | 2174.809 | 1409.874 | 8905.443 | 6388.575 | 19.692 | 3.025 |
| Q10 | 1160.066 | 377.556 | 5006.751 | 3003.827 | 17.633 | 6.366 |
| Q11 | 30.496 | 30.761 | 941.140 | 323.997 | 3.535 | 1.308 |
| Q12 | 341.982 | 178.161 | 3434.824 | 1494.902 | 10.979 | 1.565 |
| Q13 | 1093.312 | 535.430 | 4769.721 | 5595.847 | 38.276 | 1.653 |
| Q14 | 132.674 | 80.310 | 2248.999 | 1124.596 | 6.477 | 0.473 |
| Q15 | 146.978 | 65.292 | 4184.006 | 829.427 | 6.594 | 0.654 |
| Q16 | 213.195 | 193.731 | 1378.175 | 1001.576 | 1.507 | 1.180 |
| Q17 | 272.281 | 268.068 | 4134.933 | 2563.513 | 7.151 | 5.990 |
| Q18 | 733.755 | 668.766 | 8271.662 | 4802.045 | 10.049 | 6.681 |
| Q19 | 217.095 | 90.257 | 4909.323 | 2547.487 | 13.067 | 2.302 |
| Q20 | 211.185 | 127.268 | 3173.749 | 1371.021 | 13.003 | 2.193 |
| Q21 | 682.195 | 561.717 | 13533.714 | 6558.207 | 16.962 | 6.885 |
| Q22 | 375.958 | 186.768 | 1147.882 | 1177.929 | 2.738 | 1.482 |

Cumulative memory peak: **6.930 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: Q11.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-dense-active-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
