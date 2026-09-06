# Remaining TPC-H optimization work

[Current same-binary SF10/SF100 comparison](SIX.md) covers Q1, Q3, Q4,
Q12, Q14, and Q19. All six pass the fourfold median target.

Continue from the validated Q1, Q14, and Q3 checkpoint. Every new query
uses SF10 iteration, exact answer validation, and a fresh SF100 comparison
against DuckDB and ClickHouse. The revised target is twofold warm-median
speed for Q7, Q8, Q9, Q10, Q21 and Q22; preserve the fourfold target for
the other 16 queries. See the [active targets](ALL-QUERY-GOAL.md).

| Query | Current status | Evidence |
|---|---|---|
| Q5 | Native Python gates pass | [Q5 results](Q5.md) |
| Q16 | Native Python gates pass; CLI remains below target | [Native results](Q16-NATIVE.md) |
| Q13 | SF10 and SF100 gates pass | [Q13 results](Q13.md) |
| Q15 | SF10 and SF100 gates pass | [Q15 results](Q15.md) |
| Q18 | SF10 and SF100 gates pass; Q3 rechecked | [Q18 results](Q18.md) |
| Q11 | SF10 and SF100 gates pass | [Q11 results](Q11.md) |
| Q6 | SF10 and SF100 gates pass | [Q6 results](Q6.md) |
| Q4 | SF10 and SF100 gates pass | [Q4 results](Q4.md) |
| Q12 | SF10 and SF100 gates pass | [Q12 results](Q12.md) |
| Q19 | SF10 and SF100 gates pass | [Six-query confirmation](SIX.md) |
| Q17 | SF10 and SF100 gates pass | [Q17 results](Q17.md) |
| Q2 | SF10 and SF100 gates pass | [Q2 results](Q2.md) |
| Q20 | SF10 and SF100 gates pass | [Q20 results](Q20.md) |

Q7 meets the revised twofold target at both scales in its recorded
confirmations. Q8 also passes both scales; [results](Q8.md). Q9 also passes both scales; [results](Q9.md).
Q10 passes both scales; [results](Q10.md). Q21 passes both scales; [results](Q21.md). Q22 remains. Q5 passes both scales. Q16 passes through the native Python API; Q2 and Q6 pass both scales. Q17 and Q20 pass both scales. The prior [full SF100 baseline](../sf100-20260905/RESULTS.md)
remains historical evidence. Q20's allocation failure in that baseline is
resolved by the new capacity check; no fresh full-workload win is claimed.

[Earlier Q1/Q14/Q3 results](../simd-20260906/README.md) remain tied to their
frozen binaries. New changes are small commits on
`feature/tpch-coverage-20260905`; reports and compact trial evidence are
mirrored in the repository under `benchmarks/tpch/`.

Next: [Q20/Q17/Q2 correlated-query plan](CORRELATED-PLAN.md).

[Q20/Q17/Q2 iteration results](CORRELATED.md) record large reductions
in execution time and temporary memory. Q2 now passes both scales.
Later [Q17](Q17.md) and [Q20](Q20.md) confirmations pass both scales.

[Phase measurements and next steps](PROFILE-NEXT.md).

[Active targets for all 22 queries](ALL-QUERY-GOAL.md).

[Q20 typed-sum iterations](Q20-SUM.md) document the path to its passed gate.
