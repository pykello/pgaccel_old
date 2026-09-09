# SF100: all 22 queries in one 8 GiB database

One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.

Module SHA-256: `86bd2c2c1fa69355500bd22a8ff9ed5f9332ef064b9f59892ac820c6690f3d5d`.

Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.

| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 416.995 | 6649.361 | 2403.602 | 8.679 | 6.844 |
| Q2 | 77.479 | 68.269 | 1345.272 | 337.937 | 2.071 | 1.366 |
| Q3 | 430.604 | 227.209 | 3939.540 | 1976.796 | 7.393 | 2.368 |
| Q4 | 209.989 | 163.629 | 2949.705 | 1685.489 | 9.984 | 4.767 |
| Q5 | 374.163 | 180.003 | 4842.277 | 1997.345 | 7.584 | 2.169 |
| Q6 | 79.652 | 5.535 | 1816.369 | 513.654 | 2.132 | 0.471 |
| Q7 | 521.101 | 376.955 | 4598.745 | 2005.950 | 7.672 | 4.171 |
| Q8 | 658.360 | 294.006 | 5292.148 | 1998.868 | 7.577 | 2.298 |
| Q9 | 2174.809 | 1439.594 | 8905.443 | 6388.575 | 17.441 | 2.838 |
| Q10 | 1160.066 | 376.194 | 5006.751 | 3003.827 | 11.506 | 6.685 |
| Q11 | 30.496 | 17.620 | 941.140 | 323.997 | 2.857 | 0.177 |
| Q12 | 341.982 | 157.818 | 3434.824 | 1494.902 | 5.706 | 1.399 |
| Q13 | 1093.312 | 503.282 | 4769.721 | 5595.847 | 28.888 | 1.840 |
| Q14 | 132.674 | 83.308 | 2248.999 | 1124.596 | 2.384 | 0.335 |
| Q15 | 146.978 | 66.399 | 4184.006 | 829.427 | 2.701 | 0.524 |
| Q16 | 213.195 | 195.735 | 1378.175 | 1001.576 | 1.191 | 1.123 |
| Q17 | 272.281 | 207.199 | 4134.933 | 2563.513 | 4.881 | 5.841 |
| Q18 | 733.755 | 530.028 | 8271.662 | 4802.045 | 7.096 | 6.709 |
| Q19 | 217.095 | 108.861 | 4909.323 | 2547.487 | 8.506 | 2.135 |
| Q20 | 211.185 | 127.100 | 3173.749 | 1371.021 | 7.604 | 2.051 |
| Q21 | 682.195 | 573.402 | 13533.714 | 6558.207 | 12.590 | 6.961 |
| Q22 | 375.958 | 211.471 | 1147.882 | 1177.929 | 2.307 | 1.309 |

Cumulative memory peak: **6.968 GiB**. Maximum sampled file-cache charge: 0 bytes. Maximum recorded memory-limit events: 0; OOM kills: 0. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.

Warm medians above the saved pgaccel baseline: none.

The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.

[Evidence](native-transitions-full-read-ahead-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).

## Preparation compared with the preceding complete milestone

Summing one first call per query gives **256.221 → 168.749 seconds**
(34.1% lower) versus the complete chain-domain sweep. This
comparison includes dense-stripe retention, byte dictionary flags and direct
read-ahead together; it does not attribute the combined gain to I/O alone.
The narrower controls in [READ-AHEAD.md](READ-AHEAD.md) isolate the most recent
reader change. First-call sums are not warm workload totals or throughput.

Both complete runs are retained. The earlier [chain-domain report](TRANSITION-CHAINS.md)
remains evidence for its own frozen module; no individual faster samples
were substituted in this sweep.
