# Q20 SF10 confirmation attempt 2

Checkpoint `ec6f893`, 15 trials per engine, eight workers, sequential
engines. Client-time medians exclude only trial 1. Every trial matches
DuckDB. This confirmation does **not** pass the fourfold target.

| Engine | Warm median ms | Warm min ms | Warm max ms |
|---|---:|---:|---:|
| pgaccel | 34.400 | 27.271 | 39.713 |
| duckdb | 129.976 | 127.607 | 146.702 |
| clickhouse | 444.809 | 424.262 | 485.471 |

PGAccel is 3.78× faster than DuckDB and 12.93× faster than ClickHouse.
The current target is **32.494 ms**.
The confirmation script stopped before starting SF100 because the SF10
gate failed. No SF100 result is implied for this binary.

This supersedes the earlier [three-engine checkpoint](Q20-PEERS.md) for
the current Q20 threshold. Both comparisons retain their full measurements.
The same algorithm previously measured lower times in iteration runs;
those runs do not replace this failed confirmation.

[Iteration history](Q20-SUM.md) · [All-query objective](ALL-QUERY-GOAL.md)
