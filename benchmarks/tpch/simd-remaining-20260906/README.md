# TPC-H timing and memory comparison

[Current complete comparison: all 22 queries, SF10 and SF100](FINAL-ALL1.md)
contains timings for PGAccel, DuckDB and ClickHouse, workload totals,
recorded process memory, service peaks and PGAccel query allocations.

All 44 query/scale cases meet the [revised targets](ALL-QUERY-GOAL.md):
3x for 16 queries and 2x for Q7/Q8/Q9/Q10/Q21/Q22 against both engines.
The same frozen native Python module is used throughout, and every trial
passes answer validation. No measurements were repeated for memory.

At SF100, the sum of warm query medians is 10.578 s / 51.703 s / 97.474 s
for PGAccel / DuckDB / ClickHouse. PGAccel uses more resident memory;
the report separates process RSS from service and query allocation peaks.

## Historical individual checkpoints

These records document implementation and iteration history; use the
complete comparison above for current numbers.

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


- [Q8 design and results](Q8.md)
- [Q9 design and results](Q9.md)
- [Q10 design and results](Q10.md)
- [Q21 design and results](Q21.md)
- [Q22 design and results](Q22.md)

[Earlier six-query checkpoint](SIX.md) · [Correlated results](CORRELATED.md)
