# Experiment 28: bounded mixed numeric and distinct aggregation

Keep the mixed path, although Q10 still trails ClickHouse. For a complete
packed group domain of at most 16 bits, the selected-row pass accumulates
shared row counts and wide numeric sums in worker-local dense arrays.
The inverse-posting pass supplies exact distinct counts. Finalization
combines the two, normalizing AVG with the source type's scale.

Only direct-column SUM and AVG plus ordinary COUNT are accepted alongside
one direct-column COUNT(DISTINCT). Unsupported expressions retain the
existing executor. All sums use checked 128-bit arithmetic, and all arrays
are query-local and budgeted. There are no precomputed query aggregates.

## Full-data result

Implementation a71b597; measured in complete indexed checkpoint 2.
Original Q10, eight threads, minimum client latency of trials two and three:

- PGAccel mixed path: 840.358 ms.
- Fresh ClickHouse: 762.971 ms.
- Earlier unindexed checkpoint 1 PGAccel: 2122.592 ms.

The mixed path improves the earlier implementation substantially but
does not yet win. Per-value sorting, generic numeric reads, and temporary
buffer initialization remain candidates for follow-up measurement.

All 129 PG checkpoint answers validate. The 73-test native and sanitizer
suites pass. Mixed-aggregate regressions cover signed values, wide sums,
scaled averages, composite keys, and filters.

Raw evidence: pg-checkpoint2-indexed.jsonl, its metadata and gate.
See [the complete checkpoint](26-checkpoint2-indexed.md) for the separate
index construction and memory costs. This implementation is original.
