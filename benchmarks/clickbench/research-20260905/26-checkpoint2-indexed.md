# Full indexed checkpoint 2: 32 of 43 wins; goal not met

PGAccel beats ClickHouse overall on the agreed paired geometric mean with
a 10 ms shift: **0.468085×**. It wins **32/43 queries**,
below the required **39/43**. The goal remains active.

This is the explicit experimental indexed configuration, measured as one
complete run. The ordinary unindexed checkpoint remains separately reported
in [checkpoint 1](12-checkpoint1.md). This report does not mix subset runs.

## Method and validation

Both engines use eight threads on all 99,997,497 rows and 105 columns.
All 43 original queries are unchanged. Each engine runs three trials;
warm client latency is the minimum of trials two and three and includes
the complete response. Engines, compilation, and correctness recomputation
were kept separate during timed runs. Query-result caches remain disabled.

PG engine code is cb30e83; measurement metadata records documentation HEAD
17b68c2 and the binary SHA. All 129 PG trial answers pass the full gate,
including the declared Q4 wide-AVG policy and existing deterministic tie
refinements. Q4's wide arithmetic differs from ClickHouse's native Int64
overflow behavior; this is retained explicitly, not silently treated as
identical arithmetic.

All 129 fresh ClickHouse trial answers also validate. Its 23 permitted
tie differences were independently checked against source Parquet using
DuckDB 1.4.3. Count-ordered candidates require unique group keys, the exact
reference count-rank sequence, and all independently recomputed aggregates.
Q22 now includes exact MIN(URL) verification. Q25 uses independently
verified mandatory earlier rows and the boundary-timestamp row bag.
These supplemental verification queries are not benchmark timings.

| Query | PG warm client ms | CH warm client ms | PG / CH | Winner |
| --- | ---: | ---: | ---: | --- |
| Q1 | 0.033 | 3.657 | 0.009 | PGAccel |
| Q2 | 1.075 | 3.780 | 0.284 | PGAccel |
| Q3 | 5.146 | 68.555 | 0.075 | PGAccel |
| Q4 | 31.495 | 76.572 | 0.411 | PGAccel |
| Q5 | 0.052 | 433.654 | 0.000 | PGAccel |
| Q6 | 0.050 | 409.030 | 0.000 | PGAccel |
| Q7 | 0.106 | 18.740 | 0.006 | PGAccel |
| Q8 | 6.388 | 25.887 | 0.247 | PGAccel |
| Q9 | 569.150 | 670.641 | 0.849 | PGAccel |
| Q10 | 840.358 | 762.971 | 1.101 | ClickHouse |
| Q11 | 286.918 | 173.796 | 1.651 | ClickHouse |
| Q12 | 313.576 | 219.490 | 1.429 | ClickHouse |
| Q13 | 91.996 | 500.579 | 0.184 | PGAccel |
| Q14 | 527.992 | 618.364 | 0.854 | PGAccel |
| Q15 | 288.425 | 580.758 | 0.497 | PGAccel |
| Q16 | 38.790 | 350.601 | 0.111 | PGAccel |
| Q17 | 1682.662 | 1364.565 | 1.233 | ClickHouse |
| Q18 | 8.334 | 449.963 | 0.019 | PGAccel |
| Q19 | 2095.656 | 2420.274 | 0.866 | PGAccel |
| Q20 | 0.162 | 3.943 | 0.041 | PGAccel |
| Q21 | 167.137 | 331.402 | 0.504 | PGAccel |
| Q22 | 175.944 | 98.313 | 1.790 | ClickHouse |
| Q23 | 262.257 | 569.416 | 0.461 | PGAccel |
| Q24 | 159.158 | 109.364 | 1.455 | ClickHouse |
| Q25 | 44.683 | 77.764 | 0.575 | PGAccel |
| Q26 | 164.054 | 249.726 | 0.657 | PGAccel |
| Q27 | 45.083 | 62.188 | 0.725 | PGAccel |
| Q28 | 861.838 | 167.751 | 5.138 | ClickHouse |
| Q29 | 10466.637 | 1934.052 | 5.412 | ClickHouse |
| Q30 | 3.738 | 52.479 | 0.071 | PGAccel |
| Q31 | 615.776 | 292.493 | 2.105 | ClickHouse |
| Q32 | 79.100 | 422.209 | 0.187 | PGAccel |
| Q33 | 444.147 | 2730.306 | 0.163 | PGAccel |
| Q34 | 390.431 | 2269.064 | 0.172 | PGAccel |
| Q35 | 393.995 | 2331.246 | 0.169 | PGAccel |
| Q36 | 21.140 | 307.621 | 0.069 | PGAccel |
| Q37 | 3.544 | 67.016 | 0.053 | PGAccel |
| Q38 | 5.599 | 49.144 | 0.114 | PGAccel |
| Q39 | 5.907 | 44.385 | 0.133 | PGAccel |
| Q40 | 186.773 | 118.877 | 1.571 | ClickHouse |
| Q41 | 8.006 | 25.487 | 0.314 | PGAccel |
| Q42 | 12.160 | 27.087 | 0.449 | PGAccel |
| Q43 | 30.715 | 22.548 | 1.362 | ClickHouse |

The sum of warm client times is 21.336 seconds
for PGAccel and 21.516 seconds for ClickHouse.
This sum is additional context; the agreed overall goal uses the shifted
paired geometric mean. Q29 remains PGAccel's largest absolute loss.

## Index costs and benchmark boundary

Native load: 51.825 seconds.
Additional index construction: 138.266 seconds.
Combined load: 190.091 seconds.
Retained auxiliary indexes: 28,105,839,348 bytes
(26.176 GiB).
Peak index construction allocation: 28,105,842,912 bytes.
Loaded process peak RSS: 64,549,326,848 bytes.

Indexes retain complete column dictionaries, forward mappings, exact row
postings, and conservative string signatures. They contain no stored query
answers or preaggregated workload-specific tables. They are built from
generic column layout and repetition properties when the experimental
index option is enabled. Native on-disk files are unchanged.

This configuration uses additional memory and setup time. Its results do
not establish that the default unindexed engine has reached this score.
See [the structural plan](11-structural-encoding-plan.md) for the benchmark
indexing boundary and production decisions still required.

## Evidence and current work

External raw directory: ../docs/clickbench/research-20260905/.

- pg-checkpoint2-indexed.jsonl, .meta.json, and -gate.json
- ch-checkpoint2.jsonl, .meta.json, and -gate.json
- ch-checkpoint2-gate-before-q22.json preserves the initial unsupported tie
- checkpoint2-indexed-comparison.json contains this complete score
- sanitize-cb30e83.log: all 73 address/undefined-behavior sanitizer tests pass

Frozen PG executable:
../clickbench-work/research-20260905/bin/checkpoint2-indexed-query-runner.

The next stage targets the eleven losses: Q10, Q11, Q12, Q17, Q22,
Q24, Q28, Q29, Q31, Q40, and Q43. Current experiments reduce per-value
distinct sorting, pack actual numeric ranges, and partition numeric
aggregate records. They are not included in this checkpoint.
All new implementations are original and introduce no dependency.
