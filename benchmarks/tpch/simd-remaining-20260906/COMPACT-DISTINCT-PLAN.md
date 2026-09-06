# Next experiment: compact exact join-distinct aggregation

This is a proposed Q16 experiment, not an implemented path or a performance
claim. Finish Q13 measurement before starting its implementation.

Q16 joins partsupp to filtered part rows and counts distinct supplier keys
per brand, type and size, excluding a supplier-comment domain. The design
below keeps exact distinctness while reducing general row and string state.

1. Measure selected part/fact counts and grouping cardinalities at SF10.
   Confirm physical key layouts; do not assume TPC-H key ranges or dictionary
   IDs without checking the input. Preserve the general executor whenever
   a proof fails.
2. Unify the small grouping dictionaries into exact query-local IDs. Verify
   unique consecutive part primary keys and build an array mapping each part
   key to its selected group ID, with a distinct unselected sentinel. Local
   stripe IDs alone cannot identify equal strings across stripes.
3. Execute the anti-membership child under its own clamped resource limits.
   Keep its exact supplier-key domain, including duplicate and empty-result
   semantics. The new ordered-LIKE filter may help its comment predicate;
   measure that contribution rather than assuming a benefit.
4. Scan partsupp and emit selected `(group ID, supplier key)` pairs. Normalize
   the supplier key only after proving an injective bounded integer range.
   Pack the pair into 64 bits only when both domains fit; otherwise fall back.
   Repeated facts remain repeated pairs until exact duplicate elimination.
5. Partition by group-ID bits so every group belongs to exactly one worker.
   Sort packed pairs within each partition and count adjacent unique pairs.
   Project the original group values for ordinary SQL ordering and slicing.
   Compare this against a compact exact hash-set variant if sorting remains
   the bottleneck. No approximate distinct counts are allowed.
6. Account for the group array, child output and both partition buffers
   before allocating. Validate duplicate facts, repeated suppliers, signed
   extremes, cross-stripe dictionary identity, empty anti-membership and
   memory limits. Run all 22 SF1 answers, then fresh 15-trial SF10 and
   same-binary nine-trial SF100 comparisons with eight workers.

For example, `(group A, supplier 7), (group A, supplier 7), (group A,
supplier 9)` becomes two distinct suppliers for A. A local string ID 7 in
one stripe must not be confused with local ID 7 in a different dictionary.
The compact representation must encode verified global equality.

DuckDB's [parallel grouped aggregation article](https://duckdb.org/2022/03/07/aggregate-hashtable)
explains how assigning each group to one partition permits independent
parallel combination. Its [external aggregation article](https://duckdb.org/2024/03/29/external-aggregation)
describes partition-wise combination and bounded local state. Those are
background references; using exact compact pairs and per-partition sorting
here is a proposed experiment, not a claim about DuckDB's Q16 implementation.

Implement new code originally using existing project primitives. Do not
copy third-party implementations or introduce dependencies with prohibited
licenses. Retain or reject each change from measured evidence in a separate
contained commit.
