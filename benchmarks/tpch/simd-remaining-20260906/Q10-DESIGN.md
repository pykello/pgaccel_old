# Q10: aggregate by a proved unique dimension key

The implementation is at `2e427ac`; benchmark confirmation is pending. The target is 2x faster
than both DuckDB and ClickHouse at SF10 and SF100 with validated answers.

The query groups by customer key plus customer attributes and nation name.
Once the customer key is proved unique and the nation key join is proved
unique, those additional values are determined by the customer identity.
Grouping can therefore use that identity without hashing or copying the
text fields for every contributing lineitem.

Prove a single chain from the fact through unique dimension keys. Require
all group expressions to be direct columns, include the chosen center's
unique key, and belong to that center or its downstream dimensions. Extra
group columns from upstream tables, duplicate keys and unsupported shapes
retain the general executor.

Apply dimension filters and propagate surviving customer identities to
orders. Filter fact rows and probe order membership using the existing
bounded SIMD bitmap helper. Gather arithmetic only for matching rows and
accumulate private wide sums by customer identity. During result selection,
compare aggregate values first and resolve text attributes only when a
candidate enters the result heap. Keep limit, offset and tie handling in
the existing QueryResults implementation.

The first implementation uses one dense sum/seen array per worker. Full
query timing must include allocation, initialization, merge and cleanup.
If this dominates at SF100, compare partitioned aggregation or a safely
proved narrower sum representation; do not exclude those costs from the
timing report.

The focused tests compare with the general relational executor in scalar
and vector modes, both serially and in parallel. Cases cover dependent
labels, missing joins, filters, empty results, limits and offsets, duplicate
keys, and grouping fields not determined by the unique key. The query
allocation limit is also checked. No table names or generated key formulas
are assumed, and no external algorithm implementation was copied.
