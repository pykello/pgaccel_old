# Experiment 15: partitioned shared-ID grouped counts — keep

Materialize selected 32-bit global IDs and scatter them into disjoint ID
ranges. Count each range in a small contiguous array; sparse ranges use a
compact hash map instead of clearing and scanning unrelated identities.
Each complete group belongs to one range, so no per-group count union is
needed. Decode values only when output candidate selection needs them.

Use 32-bit counters only when the complete table's row count proves they
cannot overflow; otherwise use 64-bit counters. Preserve filters, HAVING,
LIMIT/OFFSET, string identity, and logical date output. This is execution over
actual selected rows, not stored preaggregated counts.

| Query | Unindexed checkpoint ms | Indexed experiment ms | Fresh CH ms |
| --- | ---: | ---: | ---: |
| Q8 | 12.123 | 9.256 | 28.305 |
| Q13 | 976.747 | 82.520 | 513.573 |
| Q16 | 1344.945 | 428.350 | 343.868 |
| Q34 | 4806.243 | 448.483 | 2303.124 |
| Q35 | 4780.689 | 451.159 | 2340.376 |
| Q37 | 41.842 | 3.674 | 55.696 |
| Q38 | 7.845 | 7.036 | 47.586 |
| Q39 | 6.248 | 5.628 | 45.258 |

All 24 trial answers validate, including Q39's independently checked tie.
Q13, Q34, and Q35 are new wins relative to the unindexed checkpoint. Q16
improves but still loses. All 59 native tests pass, including independent
counts, sparse filters, date keys, HAVING, pagination, and spill fallback.

This uses the explicit shared-dictionary configuration. Its measured native
base load is 46.415 seconds and additional construction
is 98.483 seconds, retaining 18,385,236,532 bytes.
The entire per-column account is in `indexed-count.meta.json`. These selected
queries do not constitute a fresh indexed overall score.

Original implementation; no new dependency. Raw files:
`../docs/clickbench/research-20260905/indexed-count.jsonl` and
`indexed-count-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/indexed-count-query-runner`.
