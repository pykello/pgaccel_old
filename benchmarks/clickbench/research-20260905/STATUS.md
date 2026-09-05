# Research status

Goal remains active: beat ClickHouse overall and on at least 39/43 queries.

Latest complete validated comparison:
[indexed checkpoint 2](26-checkpoint2-indexed.md).
PGAccel wins **32/43** with a paired geometric mean ratio of
**0.468085×** after a 10 ms shift. Both engines have
129 validated trial answers. The overall metric passes; the query-win
target does not.

This is an experimental indexed configuration with
26.176 GiB of auxiliary indexes and
138.266 seconds of construction after native loading.
The separate [unindexed checkpoint 1](12-checkpoint1.md) remains
18/43 wins and 1.205092×.

Raw run-time comparisons are in checkpoint2-indexed-comparison.json,
pg-checkpoint2-indexed.jsonl, and ch-checkpoint2.jsonl in this directory.
The report links validation, construction costs, and the frozen binary.

Current work proceeds one experiment at a time toward the remaining
aggregation and date-grouping losses. See the numbered experiment reports
for accepted and rejected ideas, and the [structural plan](11-structural-encoding-plan.md).
No result cache or new external algorithm dependency has been introduced.
