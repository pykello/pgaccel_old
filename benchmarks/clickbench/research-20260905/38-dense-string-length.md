# Experiment 38: dense indexed string-length aggregates

Hypothesis: Q28 spends unnecessary time in generic expression evaluation and
aggregate hash lookup despite having only 6,506 CounterID groups. Bind string
dictionary readers per stripe and accumulate directly by exact global group ID.

Implementation commit: `3eb5a0d`. Eligibility is structural: one direct group
column with a global dictionary of at most 65,536 values, one to three direct
string LENGTH sums/averages, and optional counts. Other shapes fall back.
Each worker owns a dense group array with checked 128-bit sums, counts and a
representative row. Finalization preserves HAVING, typed projection, ordering,
LIMIT and OFFSET. Index construction and its costs are unchanged.

| Warm client measurement | Q28 ms |
| --- | ---: |
| Previous complete PGAccel checkpoint | 876.656 |
| Dense string-length path | 144.838 |
| Previous complete ClickHouse checkpoint | 162.139 |

All trial client times were 166.285, 146.343 and 144.838 ms. Warm means the
minimum of trials two and three. Peak query allocation was 2,516,704 bytes.
All three full-data answers pass the correctness gate. All 85 native tests
pass, including explicit byte-length expectations for non-ASCII strings and
comparison with the unindexed fallback across scalar/AVX and serial/parallel
execution, filters, HAVING, empty selection and pagination.

Decision: keep. This is a provisional Q28 win against the prior ClickHouse
checkpoint, not a replacement for a fresh complete paired comparison.
No new dependency or persistent metadata was added.

Raw artifacts in the external docs mirror: `pg-dense-length.jsonl`,
`pg-dense-length.meta.json`, and `pg-dense-length-gate.json`.
