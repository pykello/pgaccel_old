# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `8f8c8cb6adf7378f56a551774c3a264a3a6e1919bee7100395c8ba0e8d8541c2`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 421.917 | 6649.361 | 2403.602 | 28.260 | 6.870 |
| Q2 | 77.479 | 83.833 | 1345.272 | 337.937 | 3.914 | 1.342 |
| Q3 | 430.604 | 230.971 | 3939.540 | 1976.796 | 18.417 | 2.455 |
| Q4 | 209.989 | 161.455 | 2949.705 | 1685.489 | 16.093 | 4.760 |
| Q5 | 374.163 | 215.950 | 4842.277 | 1997.345 | 19.488 | 3.055 |
| Q6 | 79.652 | 6.161 | 1816.369 | 513.654 | 10.009 | 0.393 |
| Q7 | 521.101 | 360.277 | 4598.745 | 2005.950 | 21.576 | 4.236 |
| Q8 | 658.360 | 306.575 | 5292.148 | 1998.868 | 22.653 | 2.071 |
| Q9 | 2174.809 | 1402.861 | 8905.443 | 6388.575 | 31.295 | 2.997 |
| Q10 | 1160.066 | 382.540 | 5006.751 | 3003.827 | 25.124 | 6.331 |
| Q11 | 30.496 | 33.933 | 941.140 | 323.997 | 4.561 | 1.300 |
| Q12 | 341.982 | 336.554 | 3434.824 | 1494.902 | 18.319 | 7.470 |
| Q13 | 1093.312 | 539.256 | 4769.721 | 5595.847 | 40.266 | 1.638 |
| Q14 | 132.674 | 148.570 | 2248.999 | 1124.596 | 11.676 | 0.446 |
| Q15 | 146.978 | 65.015 | 4184.006 | 829.427 | 12.125 | 0.659 |
| Q16 | 213.195 | 200.331 | 1378.175 | 1001.576 | 2.308 | 1.181 |
| Q17 | 272.281 | 270.529 | 4134.933 | 2563.513 | 12.484 | 5.968 |
| Q18 | 733.755 | 706.088 | 8271.662 | 4802.045 | 15.624 | 6.683 |
| Q19 | 217.095 | 88.875 | 4909.323 | 2547.487 | 18.234 | 2.288 |
| Q20 | 211.185 | 125.375 | 3173.749 | 1371.021 | 20.561 | 2.196 |
| Q21 | 682.195 | 572.623 | 13533.714 | 6558.207 | 19.929 | 6.887 |
| Q22 | 375.958 | 374.757 | 1147.882 | 1177.929 | 3.497 | 1.478 |

Cumulative memory peak: **7.680 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: Q2, Q11, Q14.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-scope-filter-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
