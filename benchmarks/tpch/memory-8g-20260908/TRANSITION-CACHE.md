# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `f3a884fabfcec469822428890d817b25a3a014d39b92a1ec08710cdfbe5b8a08`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons. A 512 MiB original-block cache survives SQL changes; its admission charge is included in the measured process, and hits still copy data into the mutable prepared input.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 385.851 | 6649.361 | 2403.602 | 8.346 | 6.843 |
| Q2 | 77.479 | 67.300 | 1345.272 | 337.937 | 2.440 | 1.685 |
| Q3 | 430.604 | 226.758 | 3939.540 | 1976.796 | 7.707 | 2.768 |
| Q4 | 209.989 | 162.571 | 2949.705 | 1685.489 | 10.105 | 5.134 |
| Q5 | 374.163 | 176.616 | 4842.277 | 1997.345 | 7.921 | 2.532 |
| Q6 | 79.652 | 6.322 | 1816.369 | 513.654 | 2.280 | 1.107 |
| Q7 | 521.101 | 381.828 | 4598.745 | 2005.950 | 7.729 | 4.509 |
| Q8 | 658.360 | 285.340 | 5292.148 | 1998.868 | 7.866 | 2.729 |
| Q9 | 2174.809 | 1458.379 | 8905.443 | 6388.575 | 16.844 | 3.162 |
| Q10 | 1160.066 | 377.192 | 5006.751 | 3003.827 | 11.040 | 7.040 |
| Q11 | 30.496 | 18.977 | 941.140 | 323.997 | 2.811 | 0.562 |
| Q12 | 341.982 | 175.102 | 3434.824 | 1494.902 | 6.025 | 2.063 |
| Q13 | 1093.312 | 509.293 | 4769.721 | 5595.847 | 13.420 | 2.151 |
| Q14 | 132.674 | 99.176 | 2248.999 | 1124.596 | 2.470 | 0.710 |
| Q15 | 146.978 | 65.694 | 4184.006 | 829.427 | 2.774 | 1.224 |
| Q16 | 213.195 | 208.266 | 1378.175 | 1001.576 | 1.401 | 1.652 |
| Q17 | 272.281 | 212.080 | 4134.933 | 2563.513 | 5.418 | 6.266 |
| Q18 | 733.755 | 565.997 | 8271.662 | 4802.045 | 7.546 | 7.074 |
| Q19 | 217.095 | 105.141 | 4909.323 | 2547.487 | 8.313 | 2.597 |
| Q20 | 211.185 | 133.797 | 3173.749 | 1371.021 | 6.269 | 2.529 |
| Q21 | 682.195 | 653.233 | 13533.714 | 6558.207 | 12.565 | 7.245 |
| Q22 | 375.958 | 194.361 | 1147.882 | 1177.929 | 2.210 | 1.728 |

Cumulative memory peak: **7.385 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: none.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, so changed-SQL end-to-end parity remains unproven. The original-block cache, when enabled, does not remove that preparation boundary. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-input-cache-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
