# Experiment 14: shared dictionaries and scalar distinct — keep experimental

This is a separate indexed configuration, enabled by
`PGACCEL_BUILD_DICTIONARIES=1`, with a 64 GiB construction allocation limit.
The default unindexed checkpoint remains available. Columns are selected by
storage layout: dictionary columns and raw columns needing at least four
bytes somewhere in the table. No SQL, benchmark column list, or expected
answer is consulted during construction.

The representation retains exact reverse values, lookup hash slots, and
per-stripe forward mappings. Existing local dictionary labels map to global
IDs; raw stripes store one global ID per row. Native validation and import
guarantee that every source dictionary label is used. No aggregate-output
cache or preaggregated table is stored.

Construction on the full table:

- Base native load: 48.306 seconds.
- Additional dictionary construction: 101.394 seconds.
- Total reported load: 149.700 seconds.
- Indexed columns: 50 of 105.
- Retained index allocation: 18,385,236,532 bytes (17.123 GiB).
- Peak index construction allocation: 18,385,236,532 bytes.
- Loaded process peak RSS: 54,825,693,184 bytes.

Each column's cardinality, retained bytes, construction peak, and build time
is recorded in `indexed-distinct.meta.json`. The indexes are additional to
the resident native data, not a replacement, and currently must be rebuilt
when the runner restarts. That cost is material for short-lived workloads.

| Query | Indexed warm client ms | Fresh CH warm client ms |
| --- | ---: | ---: |
| Q5 | 0.033605 | 474.599 |
| Q6 | 0.033960 | 426.035 |
| Q20 control | 6.371664 | 6.251 |

All nine trial answers validate. Unfiltered distinct uses exact complete
reverse-dictionary cardinality. Filtered distinct still executes predicates,
marks selected global IDs in query-local bitmaps, and unions them exactly.
There are 56 passing native tests, including filtered strings, signed values,
dates, empty selections, failed construction, and stable interning IDs.

The build found 99,997,493 distinct WatchID values in 99,997,497 rows.
**WatchID is not unique.** Any future near-unique grouping optimization must
retain and correctly aggregate duplicate exceptions; no uniqueness shortcut
has been enabled by this experiment.

Keep this as an explicit storage experiment and continue with general grouped
operators over IDs. These three queries do not establish an indexed overall
score. Original implementation; no new dependency. Raw files and gates live
in `../docs/clickbench/research-20260905/`; the frozen binary is
`../clickbench-work/research-20260905/bin/indexed-distinct-query-runner`.
