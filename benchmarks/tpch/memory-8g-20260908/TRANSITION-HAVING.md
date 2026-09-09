# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `d7fd0177c11058cb6ca7125e07cfbfe1991add38a817815816d8b30e9f9f23f7`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 375.513 | 6649.361 | 2403.602 | 19.986 | 6.810 |
| Q2 | 77.479 | 68.432 | 1345.272 | 337.937 | 2.589 | 1.365 |
| Q3 | 430.604 | 334.033 | 3939.540 | 1976.796 | 10.910 | 2.241 |
| Q4 | 209.989 | 182.149 | 2949.705 | 1685.489 | 13.540 | 4.408 |
| Q5 | 374.163 | 221.939 | 4842.277 | 1997.345 | 12.258 | 2.978 |
| Q6 | 79.652 | 5.381 | 1816.369 | 513.654 | 5.440 | 0.219 |
| Q7 | 521.101 | 400.987 | 4598.745 | 2005.950 | 12.568 | 4.293 |
| Q8 | 658.360 | 279.821 | 5292.148 | 1998.868 | 13.375 | 2.263 |
| Q9 | 2174.809 | 1584.471 | 8905.443 | 6388.575 | 19.375 | 2.559 |
| Q10 | 1160.066 | 545.674 | 5006.751 | 3003.827 | 16.590 | 6.329 |
| Q11 | 30.496 | 29.873 | 941.140 | 323.997 | 3.561 | 1.101 |
| Q12 | 341.982 | 174.975 | 3434.824 | 1494.902 | 10.236 | 1.356 |
| Q13 | 1093.312 | 479.027 | 4769.721 | 5595.847 | 33.230 | 1.865 |
| Q14 | 132.674 | 115.375 | 2248.999 | 1124.596 | 6.516 | 0.268 |
| Q15 | 146.978 | 65.686 | 4184.006 | 829.427 | 6.445 | 0.521 |
| Q16 | 213.195 | 194.663 | 1378.175 | 1001.576 | 1.504 | 1.127 |
| Q17 | 272.281 | 277.730 | 4134.933 | 2563.513 | 7.242 | 5.804 |
| Q18 | 733.755 | 768.500 | 8271.662 | 4802.045 | 9.728 | 6.592 |
| Q19 | 217.095 | 106.164 | 4909.323 | 2547.487 | 12.695 | 2.110 |
| Q20 | 211.185 | 129.493 | 3173.749 | 1371.021 | 12.185 | 2.023 |
| Q21 | 682.195 | 653.714 | 13533.714 | 6558.207 | 16.807 | 6.742 |
| Q22 | 375.958 | 207.193 | 1147.882 | 1177.929 | 2.690 | 1.222 |

Cumulative memory peak: **6.882 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: Q17, Q18.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-growing-bound-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
