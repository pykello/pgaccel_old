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
| PGAccel dense v2 | [189.78, 181.191, 182.078, 186.425, 184.494] | 181.191 |
| PGAccel dense v3 | [183.634, 170.783, 168.395, 172.678, 168.569] | 168.395 |
| PGAccel dense v4 | [119.427, 115.239, 115.149, 117.161, 118.916] | 115.149 |
| PGAccel dense v5 | [118.835, 111.899, 114.949, 112.274, 111.855] | 111.855 |
| PGAccel dense v6 | [97.571, 89.13, 90.628, 105.384, 90.174] | 89.130 |
| PGAccel dense v7 | [116.123, 103.14, 98.314, 96.999, 99.167] | 96.999 |
| PGAccel dense v8 | [92.676, 81.103, 79.095, 79.357, 81.188] | 79.095 |

The 4× gate is **54.94 ms** (one quarter of the faster competitor).
The current retained kernel (v8, 79.095 ms) does not yet meet that gate. No SF100
claim is made for this new kernel.

ClickHouse uses the existing `tpch_sf10` MergeTree database on port 8123;
DuckDB uses a persistent SF10 database imported from the same Parquet
files. The separate SF100 baseline used a different isolated ClickHouse
service with statistics. Do not conflate these phase configurations.

Raw JSONL evidence is in `/home/hadi/projects/docs/tpch/simd-20260906/`.
The frozen first kernel is `tpch-work/checkpoints/q1-dense-v1-query-runner`.

Rejected experiments: v2 reused masks but loaded and stored accumulator
lanes inside the root loop (181 ms). v3 cached masks while keeping a
root accumulator in registers (168 ms). Neither improved on v1 (165 ms),
so both changes were removed. Raw trials remain available.

Retained: v4 binds dictionary metadata (115 ms), v5 skips empty
combinations (112 ms), v6 folds constant scales (89 ms), and v8 vectorizes
code decoding and counts masks (79 ms). The explicit signed multiply
experiment v7 regressed to 97 ms and was removed; its numerical boundary
regression test remains. These are general expression/layout decisions,
not SQL-text recognition.
