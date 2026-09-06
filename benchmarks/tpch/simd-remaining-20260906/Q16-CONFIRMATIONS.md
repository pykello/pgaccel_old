# Q16 SF10 confirmation attempts

Fresh 15-trial comparisons; warm medians exclude only trial one.
All answers and every trial must be correct before a timing can pass.

| Attempt | PGAccel ms | DuckDB ms | ClickHouse ms | DuckDB / PGAccel | ClickHouse / PGAccel | Gate |
|---|---:|---:|---:|---:|---:|---|
| 1 | 53.621 | 189.865 | 318.782 | 3.54x | 5.95x | Fail |
| 2 | 53.564 | 188.606 | 319.084 | 3.52x | 5.96x | Fail |

Attempt 1 uses checkpoint `10878e9`. All 22 SF1 answers also match on
that frozen binary. Its SF10 DuckDB gate fails, so no SF100 run was
started for that attempt. The comparison driver stopped at its gate.
Attempt 2 uses `1b05f3d`, also passes all 22 SF1 answers, and also fails
the DuckDB timing gate. Its small iteration improvements did not carry
into the fresh comparison; SF100 was not started.

[Iteration evidence](Q16-ITERATIONS.md) · [All confirmation trials](Q16-CONFIRMATIONS.json)
