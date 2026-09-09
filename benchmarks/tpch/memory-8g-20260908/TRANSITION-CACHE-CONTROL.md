# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `f3a884fabfcec469822428890d817b25a3a014d39b92a1ec08710cdfbe5b8a08`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 377.521 | 6649.361 | 2403.602 | 8.466 | 6.844 |
| Q2 | 77.479 | 69.353 | 1345.272 | 337.937 | 1.951 | 1.365 |
| Q3 | 430.604 | 227.005 | 3939.540 | 1976.796 | 7.478 | 2.352 |
| Q4 | 209.989 | 159.279 | 2949.705 | 1685.489 | 9.943 | 4.766 |
| Q5 | 374.163 | 178.861 | 4842.277 | 1997.345 | 7.696 | 2.153 |
| Q6 | 79.652 | 5.801 | 1816.369 | 513.654 | 2.071 | 0.455 |
| Q7 | 521.101 | 377.239 | 4598.745 | 2005.950 | 7.816 | 4.211 |
| Q8 | 658.360 | 296.969 | 5292.148 | 1998.868 | 7.656 | 2.266 |
| Q9 | 2174.809 | 1421.228 | 8905.443 | 6388.575 | 16.691 | 2.824 |
| Q10 | 1160.066 | 374.576 | 5006.751 | 3003.827 | 10.875 | 6.658 |
| Q11 | 30.496 | 19.485 | 941.140 | 323.997 | 2.812 | 0.194 |
| Q12 | 341.982 | 169.928 | 3434.824 | 1494.902 | 5.696 | 1.414 |
| Q13 | 1093.312 | 506.340 | 4769.721 | 5595.847 | 13.018 | 1.747 |
| Q14 | 132.674 | 98.719 | 2248.999 | 1124.596 | 2.349 | 0.313 |
| Q15 | 146.978 | 65.555 | 4184.006 | 829.427 | 2.657 | 0.532 |
| Q16 | 213.195 | 201.917 | 1378.175 | 1001.576 | 1.105 | 1.147 |
| Q17 | 272.281 | 208.411 | 4134.933 | 2563.513 | 4.886 | 5.853 |
| Q18 | 733.755 | 529.843 | 8271.662 | 4802.045 | 6.978 | 6.739 |
| Q19 | 217.095 | 103.409 | 4909.323 | 2547.487 | 8.168 | 2.123 |
| Q20 | 211.185 | 127.534 | 3173.749 | 1371.021 | 5.944 | 2.117 |
| Q21 | 682.195 | 560.546 | 13533.714 | 6558.207 | 12.380 | 6.987 |
| Q22 | 375.958 | 209.649 | 1147.882 | 1177.929 | 2.043 | 1.353 |

Cumulative memory peak: **6.988 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: none.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, so changed-SQL end-to-end parity remains unproven. The original-block cache, when enabled, does not remove that preparation boundary. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-input-cache-control-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
