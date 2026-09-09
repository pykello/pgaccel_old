# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `6c84c537605d3d576ff4d6c4ff3ba8029aad04b3b0a64a574a2c2fa067a7f012`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 359.295 | 6649.361 | 2403.602 | 20.321 | 6.810 |
| Q2 | 77.479 | 70.007 | 1345.272 | 337.937 | 2.577 | 1.366 |
| Q3 | 430.604 | 329.082 | 3939.540 | 1976.796 | 11.550 | 2.126 |
| Q4 | 209.989 | 182.786 | 2949.705 | 1685.489 | 13.877 | 4.409 |
| Q5 | 374.163 | 177.042 | 4842.277 | 1997.345 | 11.767 | 2.121 |
| Q6 | 79.652 | 5.876 | 1816.369 | 513.654 | 5.517 | 0.218 |
| Q7 | 521.101 | 343.631 | 4598.745 | 2005.950 | 12.941 | 4.395 |
| Q8 | 658.360 | 283.906 | 5292.148 | 1998.868 | 13.554 | 2.230 |
| Q9 | 2174.809 | 1573.459 | 8905.443 | 6388.575 | 23.261 | 2.561 |
| Q10 | 1160.066 | 551.140 | 5006.751 | 3003.827 | 16.620 | 6.329 |
| Q11 | 30.496 | 17.852 | 941.140 | 323.997 | 3.328 | 0.138 |
| Q12 | 341.982 | 168.777 | 3434.824 | 1494.902 | 10.976 | 1.357 |
| Q13 | 1093.312 | 496.923 | 4769.721 | 5595.847 | 33.031 | 1.865 |
| Q14 | 132.674 | 118.489 | 2248.999 | 1124.596 | 6.686 | 0.269 |
| Q15 | 146.978 | 65.237 | 4184.006 | 829.427 | 6.628 | 0.521 |
| Q16 | 213.195 | 204.157 | 1378.175 | 1001.576 | 1.506 | 1.135 |
| Q17 | 272.281 | 207.672 | 4134.933 | 2563.513 | 7.372 | 5.804 |
| Q18 | 733.755 | 572.943 | 8271.662 | 4802.045 | 9.573 | 6.592 |
| Q19 | 217.095 | 104.659 | 4909.323 | 2547.487 | 12.847 | 2.086 |
| Q20 | 211.185 | 125.032 | 3173.749 | 1371.021 | 12.492 | 2.029 |
| Q21 | 682.195 | 641.069 | 13533.714 | 6558.207 | 17.148 | 6.920 |
| Q22 | 375.958 | 205.098 | 1147.882 | 1177.929 | 2.649 | 1.399 |

Cumulative memory peak: **7.060 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: none.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-chain-domain-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).
