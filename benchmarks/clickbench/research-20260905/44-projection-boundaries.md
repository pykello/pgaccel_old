# Experiment 44: push top-K boundaries into projection filtering

Implementation: `4fc6dea`. Ordered projection already maintains a bounded
candidate heap, but previously used it only after evaluating the full WHERE
filter. Once a worker has LIMIT+OFFSET candidates, expose its worst primary
ordering value and apply an inclusive encoded comparison before other filters
on later stripes. The bound can skip stripes by native statistics or produce
a sparse row selection for expensive string predicates.

Eligibility requires an ordered projection with a limit and a direct column
as the first ORDER BY expression. Ascending uses <=; descending uses >=.
Equal primary keys remain eligible, preserving secondary ordering and existing
deterministic tie-breaks. Each worker's bound is independently safe: rows worse
than its complete local candidate set cannot enter the final global top K.
No persistent index or materialized query answer is added.

| Query | Warm client ms |
| --- | ---: |
| Q20 control | 0.146 |
| Q24 | 19.601 |
| Q25 | 3.030 |
| Q26 | 4.434 |
| Q27 | 3.368 |
| Q28 control | 149.843 |

Q24 previously took 121.319 ms; the earlier complete ClickHouse measurement
was 114.553 ms. The new Q24 trials were 20.163, 19.601 and 25.621 ms. All 18
trial answers pass the gate, including valid Q25 boundary alternatives.
All 92 native tests pass. The added test compares bounded output against a
complete sorted result over 24 stripes, with numeric and string order keys,
ascending and descending directions, ties, OFFSET, scalar and AVX filtering,
and serial and parallel execution.

Decision: keep. Q24 becomes a substantial provisional win. Q29 remains the
only known loss, pending a fresh complete paired comparison of the final code.

Artifacts in the external mirror: `pg-projection-boundaries.jsonl`, its
metadata and its full trial gate.
