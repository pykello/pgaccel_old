# Q2 refreshed phase profile

Diagnostic binary: clean engine `b12ce48` plus stderr phase markers.
All nine SF10 answers match DuckDB. These instrumented timings identify
where time goes; they are not a promotion comparison. Warm phase medians
exclude trial 1.

| Input count / phase | Warm median ms |
|---|---:|
| 5/domains | 1.577 |
| 4/domains | 0.000 |
| 4/scalars | 0.004 |
| 4/memberships | 3.048 |
| 4/rows | 7.238 |
| 4/join-build | 1.958 |
| 4/join-aggregate | 8.279 |
| 4/output | 2.971 |
| 5/scalars | 26.955 |
| 5/memberships | 6.072 |
| 5/rows | 10.707 |
| 5/join-build | 4.526 |
| 5/join-aggregate | 81.546 |
| 5/output | 0.116 |

The five-table parent spends about 81.55 ms joining and projecting rows.
Its scalar-child phase takes 26.95 ms and includes the four-table child
phases above; do not add parent and child totals together. Optimizing only
the MIN child cannot reach the roughly 16 ms historical SF10 target.

The next path combines both stages: verify unique consecutive dimension
keys, build separate outer/child filter domains, select part keys, scan
partsupp once into compact per-part minima, and project every matching
fact row through verified dimension lookups. Preserve MIN ties and duplicate
facts. Use the general executor for unsupported shapes or failed key proofs.

Research context: ClickHouse describes how [direct joins and flat lookup
layouts](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)
avoid hash-table probes when a suitable key lookup exists. Here the physical
sequence is proved during each query and only a stripe directory is built;
there is no preloaded dictionary or result cache. DuckDB discusses [decorrelation
into grouped aggregation and joins](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql),
which motivates treating correlated MIN as a group operation. The new code
is original and reuses project helpers; no source code or dependency is imported.

[All-query goal](ALL-QUERY-GOAL.md)
