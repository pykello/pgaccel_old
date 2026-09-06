# Fresh SF10 confirmation: Q1, Q14, and Q3

Fifteen trials per engine and query; every typed answer validates.
The comparison uses **client elapsed time for all three engines**, including
execution, fetch, and answer conversion. Warm statistics exclude only
trial 1. Each engine ran in a separate phase with eight workers.

| Query | PGAccel median (ms) | DuckDB median (ms) | ClickHouse median (ms) | DuckDB / PGAccel | ClickHouse / PGAccel |
|---|---:|---:|---:|---:|---:|
| Q1 | 48.856 | 226.617 | 697.765 | 4.64× | 14.28× |
| Q14 | 14.147 | 104.119 | 262.626 | 7.36× | 18.56× |
| Q3 | 30.745 | 168.937 | 500.454 | 5.49× | 16.28× |

All three pass the fourfold **warm-median** target. Each PGAccel median
is also below one quarter of the fastest warm trial of either competitor.
This does not establish a worst-case latency guarantee: Q3 trial 2 is
43.078 ms, above one quarter of DuckDB's fastest warm trial (161.902 ms).

| Query | PGAccel first trial (ms) | Warm min (ms) | Warm max (ms) |
|---|---:|---:|---:|
| Q1 | 58.376 | 48.175 | 50.715 |
| Q14 | 14.116 | 13.760 | 15.359 |
| Q3 | 53.079 | 29.860 | 43.078 |

Frozen binary: `tpch-work/checkpoints/q3-star-final-query-runner`.
Engine revision: `68b24ea`. The working tree was clean when confirmation
started. No results or join directories are retained between queries.
All queries include temporary index construction and final ordering.

DuckDB 1.4.3 uses the persistent SF10 database; ClickHouse 26.8.2.7 uses
the preserved `tpch_sf10` MergeTree database on port 8123. Q14 uses the
documented ClickHouse DOUBLE ratio adaptation. PGAccel uses native
snapshots and a 64 GiB query allocation budget. SF10 phases have no
service memory cap; the subsequent SF100 phases have a 192 GiB cap and
zero swap. Loading is measured separately from query execution.

All 146 native tests pass. The full sanitizer suite passed 145 tests
before the final empty-state guard; all seven focused rank/join sanitizer
tests, including that new case, pass afterward. Boundary cases cover
invalid SIMD lanes, partial blocks, sparse keys, duplicates, concurrent
group updates, zero sums, projection-only groups, LIMIT, and OFFSET.

See [Q3-SF10.md](Q3-SF10.md) for retained and rejected iterations,
[ALGORITHMS.md](ALGORITHMS.md) for algorithms and eligibility, and
`SF10-CONFIRM.json` for every trial, settings, SQL hashes, and binary hash.
Raw files and the rerun script `run-q3-confirm-sf10.py` are beside this
report under `/home/hadi/projects/docs/tpch/simd-20260906/`.
