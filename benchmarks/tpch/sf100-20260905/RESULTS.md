# TPC-H SF100 comparison

The baseline is complete: DuckDB and ClickHouse pass 22/22; PGAccel passes
21/22 and rejects Q20 at its 64 GiB query-allocation limit. All services
use a 192 GiB hard cap with swap disabled. ClickHouse uses MergeTree with
column statistics and explicit floating ratio casts in Q8/Q14/Q17.

See [memory and loading](MEMORY.md), [method and configuration](PLAN.md),
[reproduction commands](REPRODUCE.md), and the [staged follow-up](FOLLOW-UP.md).
The separate [Q20 capacity check](CAPACITY.md) does not replace this baseline.

Times below are **client-observed seconds**, using the smaller of trials 2–3.
Each query has three consecutive trials. A failed or unvalidated query is
shown explicitly and is not silently removed from a full-workload total.

| Query | DuckDB (s) | PGAccel (s) | ClickHouse (s) | Fastest |
|---|---:|---:|---:|---|
| Q1 | 2.447 | 33.108 | 6.617 | duckdb |
| Q2 | 0.353 | 109.843 | 1.278 | duckdb |
| Q3 | 2.001 | 96.070 | 3.887 | duckdb |
| Q4 | 1.717 | 137.647 | 2.902 | duckdb |
| Q5 | 2.014 | 192.140 | 4.697 | duckdb |
| Q6 | 0.514 | 0.178 | 1.758 | pgaccel |
| Q7 | 1.993 | 195.986 | 4.532 | duckdb |
| Q8 | 2.057 | 118.216 | 5.176 | duckdb |
| Q9 | 6.403 | 398.998 | 8.783 | duckdb |
| Q10 | 3.063 | 75.311 | 4.677 | duckdb |
| Q11 | 0.351 | 59.891 | 0.934 | duckdb |
| Q12 | 1.506 | 55.016 | 3.328 | duckdb |
| Q13 | 5.804 | 449.263 | 4.773 | clickhouse |
| Q14 | 1.137 | 20.509 | 2.130 | duckdb |
| Q15 | 0.833 | 25.340 | 4.082 | duckdb |
| Q16 | 0.997 | 41.751 | 1.329 | duckdb |
| Q17 | 2.642 | 654.727 | 4.096 | duckdb |
| Q18 | 4.886 | 595.136 | 7.969 | duckdb |
| Q19 | 2.573 | 321.466 | 4.721 | duckdb |
| Q20 | 1.397 | execution_failure | 3.092 | — |
| Q21 | 6.772 | 525.149 | 13.330 | duckdb |
| Q22 | 1.204 | 142.559 | 1.216 | duckdb |

| Engine | Validated/consistent queries | Sum of all 22 warm minima (s) | Fastest on common queries | Peak process RSS (GiB) |
|---|---:|---:|---:|---:|
| duckdb | 22/22 | 52.663 | 19/21 | 23.58 |
| pgaccel | 21/22 | incomplete | 1/21 | 163.47 |
| clickhouse | 22/22 | 95.306 | 1/21 | 15.60 |

DuckDB supplies the saved typed oracle; its validation column includes
consistency across its trials. PGAccel and ClickHouse are checked against
those answers. Fastest-query counts use only queries validated in all three
phases. A sum of per-query warm minima is a workload summary, not the wall
time of one continuous benchmark execution.

RSS is the process high-water mark including loaded data and all preceding
queries. It differs from budgeted query allocations and from a cgroup limit
that also includes charged file cache. See PLAN.md for memory limits, storage
representations, SQL semantics, timing boundaries, and remaining limitations.

## Raw evidence

- [duckdb trials and answers](/home/hadi/projects/docs/tpch/sf100-20260905/duckdb.jsonl)
- [pgaccel trials and answers](/home/hadi/projects/docs/tpch/sf100-20260905/pgaccel.jsonl)
- [clickhouse trials and answers](/home/hadi/projects/docs/tpch/sf100-20260905/clickhouse.jsonl)

See CHECKPOINT.json for compact results, source versions, settings, and raw-file hashes.
These engineering measurements are not audited TPC Benchmark Results.

## Common-query workload totals

For the 21 queries validated in all three engines, the sums of warm minima
are duckdb: **51.266 s**, pgaccel: **4248.304 s**, clickhouse: **92.214 s**.
This explicitly excludes Q20; it is not a successful full-workload PGAccel total.
