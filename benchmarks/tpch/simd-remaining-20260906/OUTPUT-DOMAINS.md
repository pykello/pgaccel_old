# Restricting correlated sums with parent output keys

Implementation checkpoints: `5ac3023` (typed numeric projection output),
`adfc6bc` (additional correlated-key memberships), `409c4ca` (parent domain
rewrite). All 174 native tests and 12 focused sanitizer tests pass. Performance
confirmation is pending.

Q20 previously built correlated sums for forest parts across all suppliers,
then discarded suppliers outside the requested country. The parent now
projects its supplier key through the existing supplier/nation join and
country filter. The sum child intersects this domain with both its outer
partsupp rows and its matching lineitem correlation key.

This restricts complete `(part, supplier)` groups. Every quantity belonging
to a retained group still contributes to SUM; there is no sampling or
partial aggregate. Duplicate partsupp rows and duplicate parent supplier
rows retain their original multiplicity. Membership domains use set
semantics, so duplicate keys in the derived domain do not multiply sums.

The derived query drops all parent memberships and output slicing, making
its keys a superset of the keys the original parent can return. It retains
every local predicate and inner join. Recognition is limited to a two-table
parent with a filtered input of at most 64 rows, and an unlimited ungrouped
child whose sole output projects an integer correlation key. Anti-membership,
outer/existence joins, materialized inputs and unsupported shapes are skipped.
There are no table names, country names or benchmark query numbers in the
recognizer.

Numeric membership output preserves the raw scaled value and avoids a
format/parse round trip. Ordinary client output remains formatted. Child
execution uses the remaining query and spill budgets; conversion conservatively
charges the child's peak while constructing the next exact membership set.
Buffers are query-local and destroyed after execution; no result cache or
persistent auxiliary index is added.

Tests cover decimal projection scales, duplicates, grouping, slicing, empty
results, composite SUM keys, different country domains, empty domains,
stricter child budgets and skipped rewrites. Native tests exercise scalar
and AVX execution with and without parallel workers.

This is an original implementation using existing project primitives, with
no new dependencies. The general motivation is consistent with DuckDB's
[discussion of filter pushdown and join filter pushdown](https://duckdb.org/2024/11/14/optimizers).
No external source code was incorporated.
