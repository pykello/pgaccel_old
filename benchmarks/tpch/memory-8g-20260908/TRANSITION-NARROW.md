# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `09ec9a6fc221b1420a50c2129a6d5299eb30d9d7203e44ffaa7034a1643ff49d`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 454.791 | 6649.361 | 2403.602 | 28.114 | 6.870 |
| Q2 | 77.479 | 67.047 | 1345.272 | 337.937 | 2.598 | 1.341 |
| Q3 | 430.604 | 228.686 | 3939.540 | 1976.796 | 10.446 | 2.489 |
| Q4 | 209.989 | 159.002 | 2949.705 | 1685.489 | 13.482 | 4.767 |
| Q5 | 374.163 | 222.702 | 4842.277 | 1997.345 | 11.238 | 3.028 |
| Q6 | 79.652 | 5.340 | 1816.369 | 513.654 | 5.501 | 0.427 |
| Q7 | 521.101 | 366.509 | 4598.745 | 2005.950 | 12.641 | 4.251 |
| Q8 | 658.360 | 304.922 | 5292.148 | 1998.868 | 13.243 | 2.117 |
| Q9 | 2174.809 | 1414.905 | 8905.443 | 6388.575 | 19.827 | 3.025 |
| Q10 | 1160.066 | 373.475 | 5006.751 | 3003.827 | 17.615 | 6.366 |
| Q11 | 30.496 | 31.874 | 941.140 | 323.997 | 3.571 | 1.307 |
| Q12 | 341.982 | 343.101 | 3434.824 | 1494.902 | 13.186 | 7.470 |
| Q13 | 1093.312 | 538.427 | 4769.721 | 5595.847 | 38.386 | 1.654 |
| Q14 | 132.674 | 79.811 | 2248.999 | 1124.596 | 6.181 | 0.472 |
| Q15 | 146.978 | 66.569 | 4184.006 | 829.427 | 6.568 | 0.653 |
| Q16 | 213.195 | 192.620 | 1378.175 | 1001.576 | 1.519 | 1.180 |
| Q17 | 272.281 | 269.810 | 4134.933 | 2563.513 | 7.154 | 5.987 |
| Q18 | 733.755 | 696.411 | 8271.662 | 4802.045 | 9.970 | 6.680 |
| Q19 | 217.095 | 91.122 | 4909.323 | 2547.487 | 13.095 | 2.326 |
| Q20 | 211.185 | 126.432 | 3173.749 | 1371.021 | 12.906 | 2.192 |
| Q21 | 682.195 | 577.019 | 13533.714 | 6558.207 | 16.541 | 6.883 |
| Q22 | 375.958 | 377.226 | 1147.882 | 1177.929 | 2.911 | 1.480 |

Cumulative memory peak: **7.681 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: Q1, Q11, Q12, Q22.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-narrow-member-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
