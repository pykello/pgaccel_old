# Native TPC-H coverage work

Branch: `feature/tpch-coverage-20260905`, based on ClickBench checkpoint
`16e5515` (`research/clickbench-20260905`). Preserve the original branch
and its frozen benchmark binary. Every implementation step is a separate
commit with a wrapped explanation.

## Measurement contract

Use the 22 unmodified SQL statements from DuckDB 1.4.3's TPC-H extension,
with all eight tables from the existing SF1 Parquet/native fixture. Execute
in PGAccel; DuckDB is only an independent answer oracle. A parser success
is not coverage: execution and complete answer comparison must pass.
Report SF10 separately. These results are not comparable to audited TPC
Benchmark Results. Benchmark SQL and generated fixtures remain external;
new engine code must have no restrictive dependency.

`audit.py` records each SQL hash, runner hash, source revision, errors,
answers, query allocation peak, and individual execution times. Current
comparison is ordered with exact integers/decimals and floating tolerance
of max(1e-6 absolute, 1e-12 relative).
Top-K ties may need explicit oracle refinement; a mismatch is not waived.
DuckDB timings include Parquet scanning and are correctness-run diagnostics,
not a resident-engine speed comparison. ClickHouse comparisons come after
coverage and correctness stabilize.

## Stages

1. **Audit and date syntax.** Load all native tables into one registry.
   Support typed date literals, casts, calendar intervals, and BETWEEN by
   lowering to existing encoded range filters. Validate Q1 and Q6.
2. **Inner joins.** Resolve qualified names and aliases; push local filters
   to each input; build exact hash indexes preserving duplicates; stream
   joined row identities into typed aggregates. Validate Q3, Q5, Q10.
   Reject unsupported Cartesian products and enforce query memory budgets.
3. **General predicates.** Add OR, parenthesized expressions, LIKE/IN in
   expressions, column inequalities, and constant arithmetic. Preserve
   cheap native filter paths. Validate Q12 and Q19.
4. **Query composition.** Add derived-table projections, year extraction,
   arithmetic over aggregates, and aggregate HAVING. Target Q7, Q8, Q9,
   Q14, then reusable CTE/subquery support for Q11, Q15, Q18.
5. **Semi/anti joins and correlated subqueries.** Uncorrelated IN first,
   then decorrelated EXISTS and grouped scalar lookups. Target Q4, Q16,
   Q17, Q20, Q21, Q22, Q2. Preserve cardinality, duplicate, and empty-input
   semantics; do not substitute query-specific precomputed answers.
6. **Outer joins and NULLs.** Introduce explicit nullability through reads,
   expressions, filters, aggregation, and output; validate Q13 and adversarial
   NULL fixtures. Do not represent a missing row as an ordinary zero.
7. **Performance and release gate.** Profile supported SF1/SF10 queries,
   improve join order and indexes, then compare against ClickHouse under a
   documented common setup. Run native/sanitizer tests and ClickBench answer
   regressions before claiming this branch preserves the earlier checkpoint.

## Baseline and first checkpoint

The full-corpus baseline at `16e5515` accepts **0/22** queries. Older reports
used custom lineitem SQL and must not be interpreted as full TPC-H coverage.

Date implementation `c97c20d`: **2/22** correct on complete SF1, Q1 and Q6.
Single execution diagnostics: Q1 370.536 ms, Q6 3.424 ms. These are not warm
minima or comparisons with ClickHouse. Raw records:
`../results/coverage-baseline-sf1.jsonl` and
`../results/coverage-dates-sf1.jsonl` in the external TPC-H docs directory.

## Completion checkpoint

The implementation now passes all 22 statements on complete SF1. See
[STATUS.md](STATUS.md) for the current evidence, limits, and memory results.
Stage 6 reached Q13 through a restricted null-extension/count path;
general nullable column execution remains a separate, unfinished feature.
The user requested an SF100 three-engine comparison, preceded by memory
optimization. Compact join keys are implemented and measured; SF100 data
preparation and sanitizer validation are in progress.
