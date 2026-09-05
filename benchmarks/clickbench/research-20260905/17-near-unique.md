# Experiment 16: sparse postings and near-unique groups — keep

An exact global dictionary reveals how many occurrences repeat, but it does
not justify pretending the column is unique. For raw columns with sufficiently
few repetitions (at most one repeat per 1,024 distinct values), retain a sparse
inverse index. IDs are assigned in first-occurrence row order; duplicate
boundaries reconstruct each ID's first row, while additional postings retain
all repeated row positions. Irregular stripe sizes are handled by row offsets.
The structure supports actual inverse lookup, not just stored group counts.

When a composite grouping key contains such a column, rows outside its repeated
identities are proven singleton groups. Send those rows directly to bounded
typed output candidates. All rows of every repeated identity, including the
first occurrence, still go through normal complete aggregation. Secondary
keys may either merge or split these exceptions. No data-specific key name or
expected answer is used by the planner.

Restrict this path to direct grouping columns and safe column aggregates;
computed aggregate expressions retain normal checked evaluation. Filters,
HAVING, and global pagination remain intact. All 62 native tests pass,
including repeated boundaries, signed keys, cross-worker duplicate groups,
secondary-key splits, offsets, and unsafe expression errors.

| Query | Unindexed checkpoint ms | Indexed experiment ms | Fresh CH ms |
| --- | ---: | ---: | ---: |
| Q18 | 8.476 | 9.374 | 449.403 |
| Q31 | 867.660 | 982.572 | 323.675 |
| Q32 | 1298.369 | 79.848 | 436.341 |
| Q33 | 11807.536 | 461.336 | 2777.187 |

All 12 trial answers validate, including independent Q32/Q33 tie refinements.
Q32 and Q33 are new wins. Q31 is an ineligible control and regresses in this
run; its packing and loop dispatch remain follow-up work. No gain is claimed
for Q31. Q33's tracked query allocation falls from
29,796,335,616 to 57,536 bytes,
but the persistent dictionary allocation remains separate and substantial.

This uses the explicit indexed configuration. Build time and retained/peak
index bytes are recorded in `near-unique.meta.json`; no construction is hidden
inside the warm samples. Raw files:
`../docs/clickbench/research-20260905/near-unique.jsonl` and
`near-unique-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/near-unique-query-runner`.

Original implementation; no new dependency. Keep this exact exception-aware
path. Four repeated WatchID occurrences in this fixture are handled as real
rows, not discarded or treated as a uniqueness constraint.
