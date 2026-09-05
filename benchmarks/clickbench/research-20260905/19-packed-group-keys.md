# Experiment 19: exact packed group keys

Keep the compact identity path. For each independent group expression,
reserve enough bits for its complete domain: a global dictionary ID,
a stored signed numeric width, or a bounded timestamp part. Combine the
fields only if they fit in 64 bits. Otherwise retain logical key comparison.

For example, UserID's 17,630,976 IDs need 25 bits, a minute needs six,
and SearchPhrase's 6,019,103 IDs need 23: the complete Q19 group identity
fits in 54 bits. The word is an identity, not a truncated hash. Hash-table
placement still mixes it. Output ordering still compares logical values.

Timestamp extraction uses floor-corrected division for negative epochs.
Safe affine expressions can share their explicit base-column identity.
Full-width numeric composites and unsupported expressions retain the
general path. No query answer is stored in an index.

## Full-data subset

Engine 26f3db7, indexed configuration, eight threads, all original SQL.
The same binary contains the separately measured substring signatures.
These grouping queries do not use LIKE. Warm client time is the minimum
of trials two and three; the baseline is the unindexed checkpoint 1.

| Query | Unindexed checkpoint 1 | Packed indexed PG | ClickHouse |
| ---: | ---: | ---: | ---: |
| 15 | 842.275 | 422.804 | 601.387 |
| 17 | 3018.440 | 1754.765 | 1389.146 |
| 19 | 6255.106 | 3629.805 | 2442.779 |
| 31 | 867.660 | 741.060 | 323.675 |
| 36 | 1254.487 | 1126.520 | 330.542 |

Q15 wins. Q17, Q19, Q31, and Q36 still trail ClickHouse. These results
justify retaining the path, while showing that cheaper identities alone
do not remove the cost of partial aggregate tables and their merge.

All 27 answers in the combined nine-query experiment pass the gate.
The 67-test native suite passes, including independent expected counts for
hour, minute, and second at both Int64 extremes and negative boundaries;
cross-stripe dictionaries; and wider-than-64-bit composite keys.

Raw evidence: packed-signatures.jsonl, packed-signatures.meta.json,
packed-signatures-gate.json in the external research directory.
Frozen binary: ../clickbench-work/research-20260905/bin/packed-signatures-query-runner.
Implementation is original with no new dependency.
