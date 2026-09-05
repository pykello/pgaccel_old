# Full checkpoint 1: 18 of 43 wins; goal not met

Both engines use eight threads on the complete 99,997,497-row, 105-column
fixture. Queries are unchanged. Client latency includes the complete response;
warm time is the minimum of trials two and three. Engines and compilation
were kept separate during timing. No additional persistent indexes or query
result caches are present in this checkpoint.

All 129 PGAccel trial answers pass the full gate, including the declared Q4
wide-AVG policy and existing independent tied-order refinements. All 129 fresh
ClickHouse trials also validate. Its 22 differences from the saved oracle are
permitted tied rows, independently checked against the Parquet source with
DuckDB 1.4.3. Supplemental verification queries are not benchmark timings.
For count-ordered grouped outputs, verify each complete candidate aggregate,
unique group keys, and the saved reference's rank-count sequence. For Q25,
verify the mandatory earlier rows and permissible boundary-timestamp rows.

| Query | PG warm client ms | CH warm client ms | PG / CH | Winner |
| --- | ---: | ---: | ---: | --- |
| Q1 | 0.033 | 3.711 | 0.009 | PGAccel |
| Q2 | 1.061 | 3.992 | 0.266 | PGAccel |
| Q3 | 5.212 | 64.432 | 0.081 | PGAccel |
| Q4 | 32.801 | 71.879 | 0.456 | PGAccel |
| Q5 | 836.590 | 474.599 | 1.763 | ClickHouse |
| Q6 | 3156.476 | 426.035 | 7.409 | ClickHouse |
| Q7 | 0.139 | 25.063 | 0.006 | PGAccel |
| Q8 | 12.123 | 28.305 | 0.428 | PGAccel |
| Q9 | 1437.716 | 658.786 | 2.182 | ClickHouse |
| Q10 | 2122.592 | 738.066 | 2.876 | ClickHouse |
| Q11 | 634.702 | 175.622 | 3.614 | ClickHouse |
| Q12 | 396.625 | 227.567 | 1.743 | ClickHouse |
| Q13 | 976.747 | 513.573 | 1.902 | ClickHouse |
| Q14 | 1218.150 | 643.766 | 1.892 | ClickHouse |
| Q15 | 842.275 | 601.387 | 1.401 | ClickHouse |
| Q16 | 1344.945 | 343.868 | 3.911 | ClickHouse |
| Q17 | 3018.440 | 1389.146 | 2.173 | ClickHouse |
| Q18 | 8.476 | 449.403 | 0.019 | PGAccel |
| Q19 | 6255.106 | 2442.779 | 2.561 | ClickHouse |
| Q20 | 6.479 | 6.251 | 1.037 | ClickHouse |
| Q21 | 274.015 | 336.286 | 0.815 | PGAccel |
| Q22 | 270.629 | 96.774 | 2.797 | ClickHouse |
| Q23 | 426.411 | 537.647 | 0.793 | PGAccel |
| Q24 | 271.047 | 96.147 | 2.819 | ClickHouse |
| Q25 | 46.202 | 67.882 | 0.681 | PGAccel |
| Q26 | 167.731 | 238.741 | 0.703 | PGAccel |
| Q27 | 46.495 | 54.572 | 0.852 | PGAccel |
| Q28 | 881.738 | 156.374 | 5.639 | ClickHouse |
| Q29 | 10727.091 | 1914.740 | 5.602 | ClickHouse |
| Q30 | 3.773 | 96.117 | 0.039 | PGAccel |
| Q31 | 867.660 | 323.675 | 2.681 | ClickHouse |
| Q32 | 1298.369 | 436.341 | 2.976 | ClickHouse |
| Q33 | 11807.536 | 2777.187 | 4.252 | ClickHouse |
| Q34 | 4806.243 | 2303.124 | 2.087 | ClickHouse |
| Q35 | 4780.689 | 2340.376 | 2.043 | ClickHouse |
| Q36 | 1254.487 | 330.542 | 3.795 | ClickHouse |
| Q37 | 41.842 | 55.696 | 0.751 | PGAccel |
| Q38 | 7.845 | 47.586 | 0.165 | PGAccel |
| Q39 | 6.248 | 45.258 | 0.138 | PGAccel |
| Q40 | 178.005 | 125.165 | 1.422 | ClickHouse |
| Q41 | 7.757 | 27.694 | 0.280 | PGAccel |
| Q42 | 15.352 | 31.256 | 0.491 | PGAccel |
| Q43 | 33.128 | 24.405 | 1.357 | ClickHouse |

The paired geometric mean with the ClickBench 10 ms shift is **1.205092×
PGAccel / ClickHouse**. The target is below 1.0 and at least 39 wins; the
checkpoint has **18 wins**. The previous baseline was 13 wins and 3.830459×.
Summing each query's warm minimum gives 60.527 seconds for PGAccel
and 21.752 seconds for ClickHouse; that is a different metric from
the benchmark's geometric mean. Neither overall measure has reached the goal.

The frozen runner is `../clickbench-work/research-20260905/bin/checkpoint1-query-runner`.
Raw evidence in `../docs/clickbench/research-20260905/`:
`pg-checkpoint1.jsonl`, `pg-checkpoint1.meta.json`, `pg-checkpoint1-gate.json`,
`ch-checkpoint1.jsonl`, `ch-checkpoint1.meta.json`, and `ch-checkpoint1-gate.json`.
The initial direct CH comparison, including raw tie differences, is retained
as `ch-checkpoint1-answer-check.json`. Full query text and machine-readable
comparisons are in `checkpoint1-comparison.json`.

Continue with dictionary-level expression reuse, shared logical identities,
compact grouped state, and general structural indexing experiments. Maintain
this unindexed checkpoint as a separate reference.
