# Remaining TPC-H optimization work

[Current same-binary SF10/SF100 comparison](SIX.md) covers Q1, Q3, Q4,
Q12, Q14, and Q19. All six pass the fourfold median target.

Continue from the validated Q1, Q14, and Q3 checkpoint. Every new query
uses SF10 iteration, exact answer validation, and a fresh SF100 comparison
against DuckDB and ClickHouse. The target is fourfold warm-median speed.

| Query | Current status | Evidence |
|---|---|---|
| Q4 | SF10 and SF100 gates pass | [Q4 results](Q4.md) |
| Q12 | SF10 and SF100 gates pass | [Q12 results](Q12.md) |
| Q19 | SF10 and SF100 gates pass | [Six-query confirmation](SIX.md) |

Q2, Q5–Q11, Q13, Q15–Q18, and Q20–Q22 remain queued after this first
cohort. Their prior [full SF100 baseline](../sf100-20260905/RESULTS.md)
is unchanged, including Q20's query-allocation failure. No full-workload
win is claimed from the queries completed so far.

[Earlier Q1/Q14/Q3 results](../simd-20260906/README.md) remain tied to their
frozen binaries. New changes are small commits on
`feature/tpch-coverage-20260905`; reports and compact trial evidence are
mirrored in the repository under `benchmarks/tpch/`.

Next: [Q20/Q17/Q2 correlated-query plan](CORRELATED-PLAN.md).

[Q20/Q17/Q2 iteration results](CORRELATED.md) record large reductions
in execution time and temporary memory; these three still miss the
fourfold target. Q20 SF100 capacity validation is running.
