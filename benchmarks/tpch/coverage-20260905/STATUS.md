# TPC-H implementation status

Branch `feature/tpch-coverage-20260905` is based on ClickBench checkpoint
`16e5515` (`research/clickbench-20260905`).

**22/22 unmodified TPC-H queries pass on complete SF1**, at engine commit
`294c36b`. Every result in all three trials was checked against DuckDB
1.4.3 over the matching Parquet data: exact integers and decimals, exact
strings/dates, floating tolerance max(1e-6 absolute, 1e-12 relative).
The latest native test suite passes **128/128**. The ASan/UBSan suite also passes **128/128** after initializing a
temporary native schema descriptor (`ffc3b3c`). SF10 Q13 and Q15 pass all three trials; a full SF10 corpus run has
not yet been made. This is an engineering benchmark, not an audited TPC
Benchmark Result.

## Remaining queries implemented

Q15 now uses a grouped CTE as a numeric intermediate table and can reuse
its definition in a scalar MAX subquery. Child execution occurs inside
query timing. Each reference currently executes its own CTE plan, so the
revenue aggregation is computed twice; there is no cross-query answer
cache or execution during parsing.

Q13 now supports a two-input LEFT JOIN. ON predicates filter potential
matches while preserving unmatched left rows. Direct COUNT(right_column)
and COUNT(DISTINCT right_column) skip the missing-row identity; COUNT(*)
includes it. Customer counts are materialized, then the outer query groups
and sorts their histogram. Tests cover duplicates, an empty right input,
outer-only ON conditions, and rejection of unsupported nullable uses.

Numeric intermediates preserve decimal scale and promote Int32 to Int64.
They own native row groups through parent execution, inherit remaining
memory/spill policies, and charge aligned storage plus retained child
output before construction. NULL and values outside signed 64-bit
intermediate storage are rejected.

## Memory optimization before SF100

Physical join indexes now retain signed 64-bit numeric keys or borrowed
string views instead of a general expression Cell per hash entry. All
matching duplicates remain available to inner, semi, anti, and left joins.
The numeric hash omits the redundant cached hash word. A bounded-memory
regression exercises a 10,000-duplicate build within 600 KiB.

SF10 diagnostics use the same data and a 64 GiB query-allocation limit.
These are **one-trial development measurements**, not stable speed ratios.
All six before/after answers match the oracle.

| Query | Before query peak (GiB) | After query peak (GiB) | Reduction | Before (ms) | After (ms) |
|---|---:|---:|---:|---:|---:|
| Q3 | 1.191 | 0.569 | 52.2% | 10307.54 | 8686.55 |
| Q13 | 2.997 | 1.780 | 40.6% | 38933.65 | 35922.14 |
| Q21 | 12.809 | 4.177 | 67.4% | 58692.57 | 46153.22 |

Loaded SF10 tables occupy about 9.57 GiB RSS in addition to query working
memory. Extrapolating Q21 after this change makes SF100 plausible within
192 GiB, but this is not a measurement or a guarantee. Actual SF100 data
and working-memory peaks will determine whether it fits.

## Current boundaries

- General nullable column execution is still unsupported. LEFT JOIN is
  limited to two inputs with an equality key and direct right-side counts;
  nullable projection, arithmetic, WHERE, and grouping are rejected.
- Materialized outputs must be named numeric columns. String/date and
  general nullable intermediates, recursive CTEs, and shared execution of
  repeated CTE references remain future work.
- Hash joins run serially and retain their indexes in memory; they do not
  spill. Correlated scalar COUNT and broader subquery composition remain
  unsupported even though this particular 22-query corpus now passes.
- The earlier ClickBench branch and frozen binary are preserved. The new
  binary has not repeated that full workload's performance gate.

## Evidence and next steps

Raw records are in `/home/hadi/projects/docs/tpch/results/`:
`coverage-compact-complete-sf1.jsonl`, `coverage-q13-q15-sf10.jsonl`,
`memory-before-sf10.jsonl`, and `memory-after-sf10.jsonl`. The before binary
is preserved at `/home/hadi/projects/tpch-work/checkpoints/`.
`CHECKPOINT.json` records the raw-file hashes and compact per-query results.

1. Sanitizer gate complete: 128/128 tests pass.
2. Prepare SF100 from one generated dataset and compare DuckDB native
   tables, ClickHouse, and PGAccel sequentially under a 192 GiB process
   budget. Record three trials, correctness, load time, memory, and all
   failures/timeouts; distinguish execution timing from client timing.
3. Profile the measured bottlenecks before further changes. Candidates
   include shared CTE materialization, more compact grouped states, and
   parallel joins, with correctness and memory gates on each change.
4. Add general validity masks and SQL three-valued logic when expanding
   beyond this corpus. Recheck the full SF10 corpus and ClickBench before
   making broader coverage or performance claims.

New algorithms are original implementations; no engine dependency was
added. The external query/data provenance is documented in
`benchmarks/tpch/queries/README.md` in the source checkout.
