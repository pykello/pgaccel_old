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
