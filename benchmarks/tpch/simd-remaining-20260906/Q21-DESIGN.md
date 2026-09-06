# Q21: summarize correlated supplier differences by order

The implementation is at `9b3fd59`; both benchmark gates pass; see [results](Q21.md). It targets the revised
2x warm-median gate against both DuckDB and ClickHouse at both scales.

For an order, record its first supplier and whether any supplier differs.
Also record its first late supplier, whether any late supplier differs,
and the number of late rows. The Q21 correlated predicates are satisfied
exactly when there are at least two suppliers overall, exactly one late
supplier, and at least one late row. That supplier receives the late-row
count, provided the ordinary order/supplier/nation joins and filters pass.

This equivalence requires proving that the outer lineitem and both inner
aliases read the same physical table and compare the same order/supplier
columns. The positive EXISTS must use order equality and supplier
inequality. The anti EXISTS must add exactly the same late-date predicate
as the outer row. Additional predicates and alias-local filters must be
proved equivalent or cause fallback; they cannot be silently ignored.

Execution:

1. Prove order, supplier and nation keys and build order-eligibility bits
   plus supplier identities surviving the nation filter.
2. Scan outer rows for eligible suppliers, late dates and eligible orders.
   Mark candidate orders in an exact bitmap. This pass narrows work but
   does not answer either correlated predicate yet.
3. Scan all rows of candidate orders, including suppliers outside the
   selected nation. Build the order summary above using actual date values
   through the existing dictionary-threshold date comparator.
4. Use non-overlapping stripe key ranges where available; verify selected
   keys are nondecreasing within each stripe. Merge summaries for equal
   boundary keys across stripes, including groups spanning many stripes.
   Fall back if the ordering proof fails.
5. Count accepted late rows by supplier identity, then merge actual supplier
   names by value. Duplicate display names must form the same SQL group.

No fixed lineitem-per-order fanout, supplier-key formula or uniqueness of
supplier names may be assumed. Reference regressions cover multiple late
suppliers, duplicate rows, missing ordinary joins, split groups, reordered
stripes, negative keys, duplicate labels, unequal date dictionaries and
extra-predicate fallback. Exact reference comparisons precede timing.

The summary helper is also checked against the correlated definition for
every sequence of one to four rows over three supplier IDs and two late
states, including every possible split-and-merge boundary. A runtime fixture
includes an order spanning several shuffled stripes. The implementation
uses existing project components and original code; no external algorithm
implementation or new licensed dependency was introduced.

All 252 native tests and five focused ASan/UBSan tests pass. All22 SF1
answers match on the frozen module used in both scale confirmations.
