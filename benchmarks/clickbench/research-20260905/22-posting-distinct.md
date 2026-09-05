# Experiment 22: grouped distinct counts through inverse postings

Keep the original inverse-posting algorithm for bounded group domains.
For each selected row, materialize its exact packed group identity.
Visit the physical rows of each distinct input value, sort and deduplicate
the encountered group identities, and increment each affected group once.
Disjoint input-value ranges give workers independent counters; adding
their counters produces exact distinct counts without set unions.

An input value occurring under groups A, A, B contributes one to A and
one to B. The temporary per-value list uses sixteen stack entries before
a reusable budgeted overflow buffer. The complete packed group domain
must fit within 24 bits; wider shapes retain the existing executor.
All temporary arrays are allocated inside the timed query.

## Full-data measurement

Engine 575eecf; experimental indexed configuration, eight threads.
Original queries, minimum client milliseconds from trials two and three.
The reference is the validated checkpoint 1 ClickHouse run.

| Query | Inverse distinct PG | ClickHouse |
| ---: | ---: | ---: |
| 9 | 516.420 | 658.786 |
| 11 | 358.203 | 175.622 |
| 12 | 363.339 | 227.567 |
| 14 | 536.182 | 643.766 |

Q9 and Q14 win. Q11 and Q12 remain losses. Visiting every input value
still wastes work on filtered-out users; a follow-up will measure an
exact query-local candidate bitmap. No mixed-subset overall score is claimed.

All 24 answers in the combined eight-query run validate. The 72-test
suite passes. Distinct tests cover cross-stripe repetitions, composite
keys, signed extremes, empty selections, and pagination. The existing
parser does not support HAVING COUNT(DISTINCT ...); this experiment does
not extend that syntax.

Raw evidence: posting-distinct-filter.jsonl, its .meta.json, and
posting-distinct-filter-gate.json in the external research directory.
Frozen binary: ../clickbench-work/research-20260905/bin/posting-distinct-filter-query-runner.
The exact index build and storage costs remain separately recorded.
Implementation is original; no new dependency.
