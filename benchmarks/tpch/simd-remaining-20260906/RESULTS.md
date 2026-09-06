# Remaining-query CPU iterations

Baseline: three trials per query and engine, eight workers, native
PGAccel SF10 tables, persistent DuckDB, and ClickHouse MergeTree. All
answers validate. Times are client elapsed milliseconds; warm medians
exclude trial 1. The old full SF100 report remains separate.

| Query | PGAccel baseline (ms) | DuckDB (ms) | ClickHouse (ms) |
|---|---:|---:|---:|
| Q4 | 13256.336 | 211.228 | 352.881 |
| Q12 | 2777.569 | 147.215 | 397.452 |
| Q19 | 33427.765 | 244.487 | 602.110 |

## Q4 existence join

| Version | Change | Warm min (ms) | Warm median (ms) | Warm max (ms) | Correct |
|---|---|---:|---:|---:|---|
| v1 | Exact candidate and hit bitmaps | 46.586 | 49.801 | 51.777 | True |
| v2 | Parallel final grouping | 28.662 | 29.763 | 32.399 | True |
| v3 | Parallel bitmap initialization | 26.293 | 26.523 | 29.036 | True |

The candidate bitmap contains exactly the filtered outer keys. Inner
duplicates only set hit bits; final grouping counts each qualifying outer
row, preserving outer duplicates. NOT EXISTS uses the inverse hit test.
Date comparisons translate between stripe-local dictionaries before SIMD
code comparison. Query temporary storage is budgeted and rebuilt for each
statement. The integer domain is bounded; unsupported shapes fall back.

No new dependency or copied research implementation is introduced. See
[PLAN.md](PLAN.md) for the primary reference and subsequent query stages.
Raw JSONL, frozen binaries, and all trial ranges are retained.


## Q12 grouped conditional counts

| Version | Change | Warm min (ms) | Warm median (ms) | Warm max (ms) | Correct |
|---|---|---:|---:|---:|---|
| v1 | Exact conditional bitmaps and grouped counts | 35.133 | 36.145 | 42.988 | True |
| v2 | Narrow key-offset arithmetic (rejected) | 34.737 | 35.908 | 43.819 | True |
| v3 | SIMD masks and group population counts | 33.592 | 34.177 | 35.122 | True |
| v4 | Shared complements alone (rejected) | 43.349 | 43.643 | 51.272 | True |
| v5 | Branch-free shared complements (rejected) | 33.370 | 36.706 | 41.684 | True |

The v1 phase diagnostic measured about 14 ms constructing build bitmaps
and 19 ms filtering, probing, and grouping. Instrumented phase timings
are diagnostic only and excluded from the latency table. The narrower
key-offset arithmetic had no clear gain; signed-boundary tests remain.
Complement sharing alone saved memory but regressed latency, motivating
a branch-free construction experiment. It still did not beat v3, so both
sharing experiments were rejected. The faster separate bitmaps remain.


## Q19 disjunctive join aggregation

The retained implementation compiles shared native filter leaves and
Boolean bitmap operations, then probes exact build-key membership for
each disjunct. Matching probe rows are unioned before arithmetic so
overlapping branches contribute once. Sequence-key uniqueness is proved
during each statement; unsupported shapes use the general executor.

Nine correct SF10 trials: warm median 21.922 ms,
range 21.681–24.596 ms. Retained; final scale results
and earlier-query regression checks are in [SIX.md](SIX.md).
