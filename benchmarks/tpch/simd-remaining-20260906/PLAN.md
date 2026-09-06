# Remaining TPC-H CPU work

Continue the same fourfold warm-median target against both engines, with
SF10 iteration before SF100 validation. Preserve all trials, typed answers,
query allocation peaks, and separate engine phases. Each retained change
is a small commit; no prepared answers or query-name dispatch are allowed.

Start with Q4, Q12, and Q19. Q4 needs an existence join and a date-column
comparison. Q12 adds grouped conditional counts and two date comparisons.
Q19 stresses boolean filtering around a join. These expose reusable gaps
beyond the Q1/Q14/Q3 checkpoint. The remaining correlated aggregates and
multiway joins follow; the old full SF100 baseline remains authoritative
until each query is remeasured.

For Q4, form the exact filtered outer key domain before scanning the
inner relation. Probe a bounded bitmap before decoding date predicates,
then mark matching keys and count every surviving outer row once. Outer
duplicates must retain their multiplicity, while inner duplicates must
not multiply EXISTS results. Include NOT EXISTS and empty-domain tests.

Date dictionary IDs cannot be compared between different columns. Map
one dictionary's labels to exact lower/upper-bound thresholds in the
other dictionary, then compare codes in SIMD batches. Keep general
fallback for unsupported storage and expressions.

[DuckDB's optimizer article](https://duckdb.org/2024/11/14/optimizers)
describes reducing intermediate data through filter pushdown and using
build-side join-key ranges to filter probes. The exact candidate bitmap
and date-dictionary thresholds proposed here are original implementations
for PGAccel's representation, not copied DuckDB code. Earlier SIMD/hash
references and the license policy remain in
`../simd-20260906/PLAN.md`. No new dependency is introduced.
