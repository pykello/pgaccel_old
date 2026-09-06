# Six-query confirmation at SF10 and SF100

Engine checkpoint: `7c92573`. All six queries pass exact answer checks
and the fourfold warm-median target against both engines at both scales.
These results cover six queries, not the full TPC-H workload.

Times are client elapsed milliseconds, including execution, fetch and
answer conversion. Warm medians exclude only trial 1. SF10 has 15 trials
per query and engine; SF100 has nine. Engines run sequentially with eight
workers. No builds or tests run during timed phases.

| Scale | Query | PGAccel ms | DuckDB ms | ClickHouse ms | DuckDB / PGAccel | ClickHouse / PGAccel |
|---|---|---:|---:|---:|---:|---:|
| SF10 | Q1 | 49.290 | 220.238 | 707.772 | 4.47× | 14.36× |
| SF10 | Q3 | 31.244 | 160.170 | 474.585 | 5.13× | 15.19× |
| SF10 | Q4 | 19.551 | 174.624 | 347.211 | 8.93× | 17.76× |
| SF10 | Q12 | 30.518 | 137.846 | 377.467 | 4.52× | 12.37× |
| SF10 | Q14 | 14.154 | 103.842 | 266.302 | 7.34× | 18.81× |
| SF10 | Q19 | 22.619 | 241.969 | 566.870 | 10.70× | 25.06× |
| SF100 | Q1 | 508.092 | 2359.824 | 6600.856 | 4.64× | 12.99× |
| SF100 | Q3 | 439.728 | 1973.397 | 3899.794 | 4.49× | 8.87× |
| SF100 | Q4 | 218.227 | 1693.183 | 2932.348 | 7.76× | 13.44× |
| SF100 | Q12 | 329.084 | 1456.183 | 3396.404 | 4.42× | 10.32× |
| SF100 | Q14 | 133.189 | 1110.522 | 2224.300 | 8.34× | 16.70× |
| SF100 | Q19 | 221.634 | 2524.301 | 4835.989 | 11.39× | 21.82× |

SF100 services have a 192 GiB memory cap and no swap. PGAccel temporary
query allocations have a 64 GiB cap. Native table loading is separate
from query timing; resource peaks below include loading and charged cache.

| Phase | Service peak GiB | Swap bytes | OOM events |
|---|---:|---:|---:|
| duckdb-six-sf100 | 27.09 | 0 | 0 |
| six-ch-sf100-layout | 0.98 | 0 | 0 |
| clickhouse-six-sf100 | 6.80 | 0 | 0 |
| pgaccel-six-sf100 | 113.87 | 0 | 0 |

PGAccel uses resident native columns; DuckDB uses its persistent database;
ClickHouse uses MergeTree with column statistics. These are engineering
comparisons on the same generated data, not audited TPC benchmark results.
All temporary join directories are rebuilt during execution. Original SQL
is used, with the documented ClickHouse floating-point adapter for Q14.

The frozen binary passed all 154 native and 154 ASan/UBSan tests.
[SIX.json](SIX.json) retains every trial timing, settings, binary hashes
and raw-evidence hashes. Raw JSONL, phase commands and resource samples
are in `/home/hadi/projects/docs/tpch/simd-remaining-20260906/`.

Q4 uses exact existence bitmaps and dictionary date comparisons. Q12
uses exact build-key presence and conditional bitmaps with SIMD probes.
Q19 compiles its disjunction into shared native-filter bitmaps and unions
matching fact rows before aggregation, preserving overlap semantics.

[Iteration history](RESULTS.md) · [Q4 details](Q4.md) ·
[Q12 details](Q12.md) · [Remaining plan](PLAN.md)
