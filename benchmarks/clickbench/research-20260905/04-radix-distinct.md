# Experiment 5: radix-partitioned numeric distinct — keep

Materialize selected raw values and histogram their high hash bits. Scatter
into 1,024 disjoint partitions, then build and destroy an exact integer set
for each partition. Each partition contains all occurrences of its values,
so final set sizes can be summed without worker-local set unions. The result
remains exact; zero and negative bit patterns are ordinary values.

The query allocator covers both materialization arrays and the active sets.
All 38 native tests pass, including a 17-stripe independent-set oracle for the
new path and all existing spill checks. All three full-data Q5 trials validate.

| Query | Flat owner sets warm ms | Radix warm ms | Speedup |
| --- | ---: | ---: | ---: |
| Q5 | 2463.171 | 944.235 | 2.61× |

Raw client trials: 948.479, 970.323, 944.235 ms.
Tracked query peak: 1873861608 bytes. The additional
materialization memory is explicit and remains below the 32 GiB query budget.
ClickHouse's previous warm value is 483.858 ms, so Q5 has not yet reached the
performance target. Keep the improvement and test buffer reservation next.

This is an original implementation of hash partitioning and exact sets; no
new library dependency. See the aggregation references in `references.md`.
Raw files: `../docs/clickbench/research-20260905/radix-integer-distinct.jsonl`
and `radix-integer-distinct-gate.json`.
