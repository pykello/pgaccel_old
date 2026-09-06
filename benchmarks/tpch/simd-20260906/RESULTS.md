# SF10 Q1 iteration results

Original Q1 SQL, eight workers, five consecutive trials, exact typed
answer checks. Each phase runs alone. Milliseconds include client
execution, fetch, and answer conversion. Warm minimum is trials 2–5;
retain the complete trial sequence to expose noise.

| Engine/checkpoint | Trials (ms) | Warm minimum |
|---|---|---:|
| DuckDB | [385.411, 222.907, 219.754, 225.695, 223.136] | 219.754 |
| ClickHouse | [1173.422, 732.821, 746.265, 747.896, 746.491] | 732.821 |
| PGAccel before | [3116.63, 3313.484, 3270.18, 3229.998, 3312.502] | 3229.998 |
| PGAccel dense v1 | [183.473, 166.921, 168.048, 164.808, 170.404] | 164.808 |

The 4× gate is **54.94 ms** (one quarter of the faster competitor).
The first dense arithmetic kernel does not yet meet that gate. No SF100
claim is made for this new kernel.

ClickHouse uses the existing `tpch_sf10` MergeTree database on port 8123;
DuckDB uses a persistent SF10 database imported from the same Parquet
files. The separate SF100 baseline used a different isolated ClickHouse
service with statistics. Do not conflate these phase configurations.

Raw JSONL evidence is in `/home/hadi/projects/docs/tpch/simd-20260906/`.
The frozen first kernel is `tpch-work/checkpoints/q1-dense-v1-query-runner`.
