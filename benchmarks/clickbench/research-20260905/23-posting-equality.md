# Experiment 23: equality predicates from actual row postings

Keep the indexed equality path. Find the exact numeric value's posting
list, then binary-search the stripe's physical row range. A sparse slice
can directly produce a selection bitmap, intersect an existing bitmap,
or count matching rows. Dense slices retain the contiguous vector scan.
Missing values produce an empty selection.

This uses the same complete inverse row index as grouping. No result
or predicate-specific count is cached. A stripe records its global row
offset when the immutable auxiliary index is attached.

## Full-data measurement

Engine 575eecf; indexed configuration, eight threads, original SQL.
Minimum client milliseconds of trials two and three; ClickHouse is the
validated checkpoint 1 reference.

| Query | Indexed equality PG | ClickHouse |
| ---: | ---: | ---: |
| 20 | 0.141 | 6.251 |
| 37 | 7.226 | 55.696 |
| 38 | 11.555 | 47.586 |
| 39 | 9.635 | 45.258 |

Q20 becomes a clear win. The other three remain wins but are slower
than the preceding indexed-count subset: the posting lookup is not a
universal improvement over metadata and vector filtering. A later full
run must retain these controls.

All 24 combined-run answers pass validation. Tests cover count, set,
and intersection; missing and extreme values; dense fallback; independent
scalar membership; and unchanged bytes past a partial bitmap tail.

Evidence: posting-distinct-filter.jsonl, its .meta.json, and its gate
in the external research directory. All index costs are recorded in the
metadata. This is an original implementation with no new dependency.
