# Small integer domains before inner joins

Checkpoint `df36317` uses at most 64 filtered integer build rows to
discard impossible probe keys before hash probing. Small domains fit in
one exact 64-bit bitmap when their key range permits it; wider ranges
use exact binary search. The original build rows remain available so
duplicate join multiplicity is preserved.

Only required physical inner equalities participate. Left and existence
joins are excluded, disjunctions do not supply required edges, and both
columns must be integer keys. Discovery is budgeted and skipped when
resources are tight. Fully covered physical probe ranges skip pruning.
Edges from the smallest inputs run first, allowing filtered region keys
to restrict nations before nation keys restrict larger inputs.

This is an original implementation of a small semijoin reduction. It
does not cache query results or change table contents. The earlier
[optimizer research](https://duckdb.org/2024/11/14/optimizers) describes
filter pushdown and join filtering; applying exact tiny domains to the
retained row identities here is a local implementation choice.

All 172 native and ASan/UBSan tests pass. The tests cover signed extrema,
duplicates, sparse ranges, memory limits, empty builds, multi-hop filtering,
full domains, outer joins and non-required disjunctions.

All 22 TPC-H queries also match a fresh DuckDB oracle at SF1. This is a
one-trial-per-query correctness sweep, not a speed comparison. Its database
was built from the existing SF1 Parquet files, with 6,001,215 lineitems.

| SF10 query | Warm client median ms | Correct trials |
|---|---:|---:|
| Q20 | 33.654 | 9 |
| Q17 | 17.874 | 9 |
| Q2 | 145.168 | 9 |

Q20 remains above the fresh 32.720 ms fourfold threshold. Q2 also remains
above its target. Q17 continues to pass its SF10 regression gate. No new
SF100 promotion is claimed.

[Evidence summary](SMALL-DOMAINS.json) · [Q20 iterations](Q20-SUM.md)
