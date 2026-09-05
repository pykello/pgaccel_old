# Experiment 21: complete counts from exact inverse row postings

Keep the indexed count path. Repeated raw values now have a CSR inverse
map: a contiguous list of every matching physical row position, plus
offsets delimiting each value's list. This supports retrieving actual rows.
The list length supplies an unfiltered group count without a new hash table.

For nearly unique columns, the preceding sparse-exception representation
remains smaller. The new dense representation applies to the remaining
indexed raw columns, independent of any particular query text. It is built
from all rows and retains their physical positions in increasing order.
Construction scratch and persistent storage both use the index budget.

The count executor accepts one independent column identity, including safe
affine projections whose explicit base column determines the group.
It evaluates projected values from an actual row in the posting list.
Filtered counts and other aggregate functions retain existing paths.

## Full-data measurements

Engine 32ab6cc; explicit experimental indexed configuration. Eight threads,
original queries, minimum client milliseconds of trials two and three.
The ClickHouse reference is the independently validated checkpoint 1 run.

| Query | Posting count PG | ClickHouse |
| ---: | ---: | ---: |
| 16 | 39.579 | 343.868 |
| 36 | 22.363 | 330.542 |

Both queries win. All six answers pass validation. This subset does not
establish a new full-workload score.

Native loading takes 48.311 seconds; all index construction
takes 139.845 seconds. Retained auxiliary storage is
28,105,839,348 bytes (26.176 GiB).
Peak construction allocation is 28,105,842,912 bytes;
loaded process peak RSS is 64,549,007,360 bytes.

These are actual inverse indexes, not stored query answers or standalone
preaggregated count tables. The benchmark still reports this configuration
separately from the ordinary unindexed baseline, with build and memory costs.

## Validation and reproduction

The 70-test suite passes. Tests cover every inverse row exactly once,
signed extremes, empty stripes, ordered postings, failed construction
without partial publication, pagination, HAVING, affine projections, and
fallback for filtered or non-count aggregates.

Raw files: posting-count.jsonl, posting-count.meta.json,
posting-count-gate.json in the external research directory.
Frozen binary: ../clickbench-work/research-20260905/bin/posting-count-query-runner.
All implementation is original and adds no dependency.
