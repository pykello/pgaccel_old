# Q20 SF10 confirmation attempt 1

Checkpoint `640dce8`, 15 trials per engine, eight workers, sequential
engines. Client-time medians exclude only trial 1. Every trial matches
DuckDB. This confirmation does **not** pass the fourfold target.

| Engine | Warm median ms | Warm min ms | Warm max ms |
|---|---:|---:|---:|
| pgaccel | 37.346 | 28.827 | 42.169 |
| duckdb | 136.716 | 130.166 | 147.847 |
| clickhouse | 457.847 | 442.610 | 477.819 |

PGAccel is 3.66× faster than DuckDB and 12.26× faster than ClickHouse.
The current target is **34.179 ms**.
The confirmation script stopped before starting SF100 because the SF10
gate failed. No SF100 result is implied for this binary.

This supersedes the earlier [three-engine checkpoint](Q20-PEERS.md) for
the current Q20 threshold. Both comparisons retain their full measurements.
The same algorithm previously measured lower times in iteration runs;
those runs do not replace this failed confirmation.

[Iteration history](Q20-SUM.md) · [All-query objective](ALL-QUERY-GOAL.md)
