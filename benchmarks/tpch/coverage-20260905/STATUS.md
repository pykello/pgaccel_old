# TPC-H implementation status

Work continues on `feature/tpch-coverage-20260905`, based on the completed
ClickBench branch at `16e5515`. The ClickBench branch and frozen binary
remain available independently.

Seventeen of the 22 unmodified DuckDB TPC-H statements have passed the
complete SF1 oracle at their implementation checkpoints:
**Q1, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q14, Q16, Q18, Q19, Q21, Q22.**

The most recent full corpus audit was the 15-query checkpoint. Q11 and
Q22 were subsequently checked individually. Q11 passed all three trials
under the newer exact-integer/decimal gate. A complete repeat of that
stricter gate, SF10 checks, and a final ClickHouse comparison are pending.
Do not treat these intermediate records as a frozen performance release.

Remaining queries and current work:

| Query | Blocking feature |
|---|---|
| Q2 | Correlated scalar MIN |
| Q13 | Outer-join COUNT semantics and an aggregated derived table |
| Q15 | An aggregated CTE used by both the main query and a scalar query |
| Q17 | Correlated scalar AVG and arithmetic |
| Q20 | Correlated scalar SUM within nested membership queries |

Implemented features include date casts/literals/intervals; calendar date
grouping; exact duplicate-preserving inner joins; scoped EXISTS/NOT EXISTS;
boolean and arithmetic predicates; UTF-8 SUBSTRING; projected derived
tables; aggregate arithmetic; numeric HAVING; nested independent IN sets;
and independent numeric scalar subqueries. The relational path currently
runs serially and keeps join indexes in memory. Scalar aggregates and set
subqueries run inside the measured query execution, with inherited memory
and spill policies.

Current boundaries are explicit: no general NULL-valued column execution,
no Cartesian products, and restricted existence/subquery composition.
Correlated scalar COUNT needs default-row semantics and must not be treated
like an empty SUM. New algorithms are original implementations; there is
no added engine dependency or linked TPC generator.

Raw progress records are in `/home/hadi/projects/docs/tpch/results/`:
`coverage-sets-sf1.jsonl`, `coverage-q11-sf1.jsonl`, and
`coverage-q22-sf1.jsonl`. The ClickHouse driver smoke check is
`ch-driver-check-sf1.jsonl` (Q6 only).
