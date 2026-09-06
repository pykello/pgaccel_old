# Q7 native Python confirmations

Fresh 15-trial SF10 and nine-trial SF100 phases, sequential engines, eight
workers. Warm medians exclude only trial one. SF100 processes are capped
at 192 GiB with zero swap; PGAccel uses a 64 GiB query budget. All rows and
values must match the saved DuckDB oracle before evaluating the timing gate.

| Attempt | Scale | PGAccel native ms | DuckDB ms | CH ms | DuckDB / PG | CH / PG | Gate |
|---|---|---:|---:|---:|---:|---:|---|
| 1 | SF10 | 43.297 | 177.313 | 588.180 | 4.10x | 13.58x | Pass |
| 1 | SF100 | 545.443 | 1949.356 | 4497.004 | 3.57x | 8.24x | Fail |

Attempt 1 uses the same frozen module at `7b452eb` for both scales. All22
SF1 answers also pass. SF100 peak query allocation is 493,989,310 bytes;
systemd reports a 117.1 GiB process peak and zero swap. The DuckDB timing
gate fails at SF100, so Q7 is not added to the confirmed count.

The next experiment fixes a probe-order estimate that treated gaps in order
keys as extra selectivity. It uses actual proved key cardinalities and an
approximate count of reads for dense versus bitmap/rank payload lookups.
This is a plan heuristic only; all joins and group values remain exact.

[Iteration history](Q7-ITERATIONS.md) · [All confirmation trials](Q7-CONFIRMATIONS.json)
