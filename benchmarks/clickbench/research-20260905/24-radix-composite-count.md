# Experiment 24: radix partitioning for packed composite counts

Keep the original partitioned count path. Scan each selected row once
to materialize its exact 64-bit group identity and a representative row
position. Count hash-prefix buckets, scatter complete records into disjoint
partitions, then build one exact count table per partition. All instances
of a group meet in one partition, eliminating partial-table unions.

The forward records and scatter buffer are query-local and budgeted.
For 100 million rows, two 16-byte record buffers can occupy about 3.2 GB
at peak. They are not persistent indexes or reused query results.
Unordered LIMIT queries retain the existing bounded prefix algorithm.

## Full-data subset

Engine a11aa5c, indexed configuration, eight threads, original SQL.
Minimum client milliseconds of trials two and three. Packed-only numbers
come from the preceding indexed run, except Q18's checkpoint 1 control.

| Query | Prior packed PG | Radix PG | ClickHouse |
| ---: | ---: | ---: | ---: |
| 15 | 422.804 | 293.780 | 601.387 |
| 17 | 1754.765 | 1686.980 | 1389.146 |
| 18 | 8.476 | 8.544 | 449.403 |
| 19 | 3629.805 | 2059.573 | 2442.779 |

Q19 becomes a win. Q15 improves an existing win. Q17 remains a loss,
so partition materialization and local table costs still need attention.
The Q18 bounded-prefix control remains fast.

All 24 answers in the combined eight-query run validate. The native suite
at measurement has 75 passing tests, including exact cross-stripe groups,
filters, HAVING, pagination, negative timestamp parts, and wide-key fallback.

Evidence: prepared-radix.jsonl, its .meta.json, and prepared-radix-gate.json.
Frozen binary: ../clickbench-work/research-20260905/bin/prepared-radix-query-runner.
This binary also contains the separate rejected substring preparation
experiment, which these four grouping queries do not invoke.
