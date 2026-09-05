# Experiment 27: exact candidate bitmaps for filtered inverse distinct

Keep the query-local candidate bitmap. While materializing selected group
identities, each worker marks the global IDs of input values that actually
appear in those selected rows. OR the worker bitmaps and visit only the
marked values' complete inverse posting lists.

This does not approximate or cache a result. It excludes values proven
absent from the current selection; postings of a marked value still undergo
the exact row-selection and per-group deduplication checks. For UserID,
one complete candidate bitmap needs about 2.1 MiB, allocated per worker
inside the query budget. An unfiltered query keeps the direct value loop.

## Full-data evidence

Implementation 3c87d31; measured in complete indexed checkpoint 2.
Original SQL, eight threads, minimum client milliseconds of trials two and
three. The prior inverse-posting subset used the same exact distinct
algorithm without candidate pruning.

| Query | Visit all values | Candidate bitmap | Fresh ClickHouse |
| ---: | ---: | ---: | ---: |
| 11 | 358.203 | 286.918 | 173.796 |
| 12 | 363.339 | 313.576 | 219.490 |
| 14 | 536.182 | 527.992 | 618.364 |

Q11 and Q12 improve but remain losses. Q14 remains a win. The code is
retained, with further work on per-value deduplication and temporary buffer
initialization measured separately.

All 129 PG checkpoint answers validate, and all 73 native and sanitizer
tests pass. Exact cross-stripe filters and empty selections are covered.
Evidence is in pg-checkpoint2-indexed.jsonl and its metadata/gate;
the complete comparison is [checkpoint 2](26-checkpoint2-indexed.md).
Implementation is original and adds no dependency.
