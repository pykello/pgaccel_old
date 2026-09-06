# Q11 sorted join-sum iterations

Warm client medians exclude only trial 1. Every answer matches DuckDB.

| Engine checkpoint | Warm median ms | Warm min ms | Warm max ms | Query peak MiB |
|---|---:|---:|---:|---:|
| 6e4e3da | 1922.456 | 1908.503 | 2004.264 | 190.042 |
| 8d118bc | 4.407 | 4.289 | 4.498 | 8.013 |

Three contained changes implement this path: `a02548a` normalizes fixed
decimal thresholds with directed rounding; `a530b4c` streams ordered SUM
groups and merges stripe boundaries; `8d118bc` shares a grouped join scan
with a scalar total only after proving exact input equivalence.

Dimension key sequences are verified during each query. Exact membership
domains filter supplier/nation joins. Sorted fact stripe ranges must be
disjoint or touch only at a boundary, and selected keys must be monotonic
inside each stripe. Shuffled physical stripes are allowed. Unsupported
layouts and duplicate dimension keys retain general execution.

Selected rows are gathered into arithmetic blocks. Stripe bounds prove
narrow intermediate and partial sums safe; boundary sums and global totals
use checked 128-bit addition. Only first/last groups can cross stripe
boundaries, so complete interior groups can be checked in parallel. Fixed
decimal comparisons use floor/ceiling thresholds to preserve all six
operators, including negative fractional bounds.

The parent and scalar child must have identical physical inputs, filters,
join predicates and SUM expressions. The total is computed during the
same scan, then its formula resolves the HAVING bound. Differing inputs
retain general execution. No result cache or persistent index is introduced.

All 184 native and six focused sanitizer tests pass. All 22 SF1 answers
match DuckDB, including Q11 with **1048 rows**. Q11 has no result rows at
SF10 or SF100; the nonempty SF1 check and varied synthetic cases validate
its aggregation and threshold logic beyond those empty large-scale results.

[Fresh confirmation](Q11.md) passes both scales. The implementation is
original and uses existing project primitives, without new dependencies.

[All-query goal](ALL-QUERY-GOAL.md)
