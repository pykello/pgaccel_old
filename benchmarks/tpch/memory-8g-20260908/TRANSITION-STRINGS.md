# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `efc5b28871356204445de4a0bcf19fe9356b680e706afc4dbf4227bda0806996`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 383.416 | 6649.361 | 2403.602 | 8.455 | 6.844 |
| Q2 | 77.479 | 68.049 | 1345.272 | 337.937 | 1.975 | 1.366 |
| Q3 | 430.604 | 229.991 | 3939.540 | 1976.796 | 7.077 | 2.345 |
| Q4 | 209.989 | 182.505 | 2949.705 | 1685.489 | 9.835 | 4.608 |
| Q5 | 374.163 | 179.203 | 4842.277 | 1997.345 | 7.008 | 2.147 |
| Q6 | 79.652 | 5.695 | 1816.369 | 513.654 | 2.090 | 0.450 |
| Q7 | 521.101 | 377.100 | 4598.745 | 2005.950 | 7.911 | 4.221 |
| Q8 | 658.360 | 284.404 | 5292.148 | 1998.868 | 7.543 | 2.259 |
| Q9 | 2174.809 | 1433.294 | 8905.443 | 6388.575 | 15.964 | 2.802 |
| Q10 | 1160.066 | 572.116 | 5006.751 | 3003.827 | 10.813 | 6.283 |
| Q11 | 30.496 | 16.090 | 941.140 | 323.997 | 2.704 | 0.163 |
| Q12 | 341.982 | 172.280 | 3434.824 | 1494.902 | 5.683 | 1.399 |
| Q13 | 1093.312 | 476.858 | 4769.721 | 5595.847 | 12.953 | 1.867 |
| Q14 | 132.674 | 97.158 | 2248.999 | 1124.596 | 2.356 | 0.323 |
| Q15 | 146.978 | 64.205 | 4184.006 | 829.427 | 2.653 | 0.534 |
| Q16 | 213.195 | 165.457 | 1378.175 | 1001.576 | 1.044 | 1.128 |
| Q17 | 272.281 | 209.036 | 4134.933 | 2563.513 | 4.795 | 5.816 |
| Q18 | 733.755 | 605.091 | 8271.662 | 4802.045 | 6.989 | 6.596 |
| Q19 | 217.095 | 103.616 | 4909.323 | 2547.487 | 8.573 | 2.122 |
| Q20 | 211.185 | 126.183 | 3173.749 | 1371.021 | 5.965 | 2.057 |
| Q21 | 682.195 | 630.409 | 13533.714 | 6558.207 | 12.362 | 6.775 |
| Q22 | 375.958 | 210.196 | 1147.882 | 1177.929 | 2.021 | 1.261 |

Cumulative memory peak: **6.954 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: none.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-parallel-sequence-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).

## Preparation compared with the preceding complete sweep

The sum of one first call per query falls from **168.749 to 146.768 seconds**
(13.0% lower) versus [the read-ahead sweep](TRANSITION-READ-AHEAD.md).
This combines small header buffers, string allocation reuse, amortized stream
setup and parallel ordered-literal matching. Q13 falls from 28.888 to 12.953
seconds in these full sequences. The isolated experiment reports retain their
own controls; no faster individual sample replaces a value here.

Some warm medians are slower than that preceding sweep, including Q10 and
Q18, but every warm median remains below the original saved pgaccel baseline.
First calls span 1.044–15.964
seconds and still include statement-specific preparation.
