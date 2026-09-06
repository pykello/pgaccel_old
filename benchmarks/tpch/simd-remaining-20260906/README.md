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
| Q2, Q17 | Large SF10 gains; latency target unmet | [Iterations](CORRELATED.md) |
| Q20 | SF100 capacity fixed; latency target unmet | [Iterations](CORRELATED.md) |

Q5–Q11, Q13, Q15–Q16, Q18, and Q21–Q22 remain queued. Q2, Q17,
and Q20 are in progress. The prior [full SF100 baseline](../sf100-20260905/RESULTS.md)
remains historical evidence. Q20's allocation failure in that baseline is
resolved by the new capacity check; no fresh full-workload win is claimed.

[Earlier Q1/Q14/Q3 results](../simd-20260906/README.md) remain tied to their
frozen binaries. New changes are small commits on
`feature/tpch-coverage-20260905`; reports and compact trial evidence are
mirrored in the repository under `benchmarks/tpch/`.

Next: [Q20/Q17/Q2 correlated-query plan](CORRELATED-PLAN.md).

[Q20/Q17/Q2 iteration results](CORRELATED.md) record large reductions
in execution time and temporary memory; these three still miss the
fourfold target. Q20 now completes SF100 correctly with 953 MiB peak query allocation;
its latency target remains unmet.

[Phase measurements and next steps](PROFILE-NEXT.md).
