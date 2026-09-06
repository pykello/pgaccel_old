# Q2 minimum-join iterations

Client warm medians exclude only trial 1. All nine answers match DuckDB.

| Engine checkpoint | Warm median ms | Warm min ms | Warm max ms | Query peak MiB |
|---|---:|---:|---:|---:|
| 409c4ca | 139.068 | 136.994 | 142.044 | 5.346 |
| 6e4e3da | 11.704 | 10.337 | 14.340 | 2.027 |

The [refreshed profile](Q2-PROFILE.md) identifies the outer join and
projection as the largest cost. The retained path handles an ordered
five-table parent and a four-table correlated MIN child with a matching
dimension chain. It verifies consecutive unique dimension keys during
execution, then builds separate exact outer and child filter domains.

Selected part keys define compact MIN slots. One parallel partsupp scan
updates worker minima for child-eligible rows and retains parent-eligible
fact rows. A missing group stays distinct from a zero or negative minimum.
Merging minima is exact. Every candidate equal to its group minimum is
projected via verified dimension lookups and ordinary ORDER/LIMIT handling.

Duplicate fact rows and all minimum-cost ties survive. Failed uniqueness
proofs, incompatible join graphs, unsupported expressions and stricter
child budgets retain general execution. No names, region literals or query
numbers appear in recognition. There is no result cache or persistent index.

All 178 native tests and eight focused sanitizer tests pass. Tests cover
signed widths, INT64_MIN values, duplicate facts, duplicate dimensions,
different filter domains, missing foreign keys and groups, and output slicing.

[Fresh confirmation](Q2.md) passes both scales. This implementation is
original and reuses project helpers without new dependencies; research
references and their relation to the design are in [the profile](Q2-PROFILE.md).
