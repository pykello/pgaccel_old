# Remaining TPC-H optimization work

Continue from the validated Q1, Q14, and Q3 checkpoint. Every new query
uses SF10 iteration, exact answer validation, and a fresh SF100 comparison
against DuckDB and ClickHouse. The target is fourfold warm-median speed.

| Query | Current status | Evidence |
|---|---|---|
| Q4 | SF10 and SF100 gates pass | [Q4 results](Q4.md) |
| Q12 | SF10 implementation and iteration in progress | [Iterations](RESULTS.md) |
| Q19 | Baseline recorded; next after Q12 | [Plan](PLAN.md) |

Q2, Q5–Q11, Q13, Q15–Q18, and Q20–Q22 remain queued after this first
cohort. Their prior [full SF100 baseline](../sf100-20260905/RESULTS.md)
is unchanged, including Q20's query-allocation failure. No full-workload
win is claimed from the queries completed so far.

[Earlier Q1/Q14/Q3 results](../simd-20260906/README.md) remain tied to their
frozen binaries. New changes are small commits on
`feature/tpch-coverage-20260905`; reports and compact trial evidence are
mirrored in the repository under `benchmarks/tpch/`.
