# Q20 fresh SF10 comparison

Checkpoint `2bbcf4a`, eight workers, 15 sequential trials per engine.
Warm medians exclude only trial 1. Client elapsed time includes execution,
fetch and answer conversion. Every trial validates against DuckDB.

| Engine | Warm median ms | Warm min ms | Warm max ms |
|---|---:|---:|---:|
| pgaccel | 64.211 | 50.611 | 71.539 |
| duckdb | 130.878 | 128.817 | 143.825 |
| clickhouse | 446.406 | 411.990 | 473.944 |

PGAccel is 2.04× faster than DuckDB and 6.95× faster than ClickHouse.
The fourfold target against both is **32.720 ms**, which remains unmet.

This fresh comparison supersedes the earlier 145.186 ms DuckDB baseline
for Q20 promotion decisions. No SF100 latency promotion has been run for
this checkpoint. Earlier SF100 capacity results use an older implementation.

Pre-sizing the tuple directory reduced SF10 peak query allocations from
30.73 to 29.70 MiB, but its separate nine-trial median did not improve.
Keep it provisionally for the smaller directory footprint and upcoming
parallel row collection; do not claim a measured latency improvement.

All 167 native tests and focused sanitizer checks pass. The tuple
reservation tests cover stable slots, bounded allocation failure and
insertion without further growth.

[Iteration history](Q20-SUM.md) · [All-query objective](ALL-QUERY-GOAL.md)
