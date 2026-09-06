# Q15 shared grouped totals

Status: [fresh SF10 and SF100 confirmation](Q15.md) passes on frozen
`918e691`, raising the individual confirmed count to thirteen queries.

Q15 groups filtered lineitem revenue by supplier, finds the largest total,
and joins its supplier attributes. The general materialized execution
formats the grouped result into strings, constructs a numeric intermediate,
and computes the scalar subquery through its own materialized source.

`639da52` introduces bounded integer-key SUM aggregation. The runtime proves
a small dense key domain and safe signed 64-bit intermediate/partial sums,
then evaluates existing arithmetic blocks into independent worker arrays.
Presence bits distinguish absent groups from sums that equal zero. A checked
128-bit reduction merges workers. Sparse domains, unsupported columns and
unsafe bounds retain general execution. No persistent aggregate cache is
introduced.

`918e691` recognizes a grouped source used by both an equality join and a
scalar MIN or MAX. Both consumers must reference the exact same CTE query
object. The runtime computes totals once, finds the extremum over the full
source, and projects every tied group through verified dimension keys.
Dimension filters apply after the extremum, so they cannot incorrectly
change which source total is largest. Duplicate dimension keys, independent
sources and stricter child resource limits retain general execution.

For example, totals (supplier 1, 10), (supplier 2, 10), (supplier 3, 0)
produce both suppliers 1 and 2 for MAX. If a dimension filter removes
supplier 1, supplier 2 remains; the extremum is still 10. If the only
maximum belongs to a missing dimension key, the inner join returns no rows.
A zero-valued group remains eligible even if all other totals are negative.

All 193 native tests pass, covering signed key widths and extremes,
filtered and dense blocks, duplicate facts, missing groups, zero sums,
minimum/maximum ties, dimension filters, duplicate-key fallback, independent
CTE sources, output slicing and strict child budgets. Eight focused sanitizer tests and all 22 SF1 answer checks also pass.
[SF10 iteration evidence](Q15-ITERATIONS.md) records the measured improvement.

The implementation is original and uses existing project primitives, with
no added dependencies. The design uses the familiar bounded-domain direct
addressing idea; the aggregation background in DuckDB's
[parallel aggregation article](https://duckdb.org/2022/03/07/aggregate-hashtable)
provides context on per-thread states and partitioning. No source code was
copied from that implementation.
