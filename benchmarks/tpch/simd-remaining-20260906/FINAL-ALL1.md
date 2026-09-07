# Full TPC-H comparison on one frozen native module

All 22 queries meet their assigned speed target against both DuckDB and
ClickHouse at SF10 and SF100. Q7, Q8, Q9, Q10, Q21 and Q22 require at
least 2x; the other 16 require at least 3x. Every recorded trial passes
answer validation. This replaces the earlier mixed-checkpoint status as
the current full-workload comparison.

PGAccel uses the native Python API for every query at both scales. Complete
client calls include parsing, execution, fetching/converting results and
native result cleanup. These are warm resident-data timings; initial data
loading is reported separately in the raw metadata and is not included.

PGAccel is faster on this workload but retains substantially more resident
memory. At SF100, process high-water marks are 100.2 GiB for PGAccel,
23.6 GiB for DuckDB and 15.7 GiB for ClickHouse. See the recorded memory
tables below for scope and missing measurements.

## Workload totals

Sums of the 22 per-query warm medians, not the elapsed duration of one
continuous workload run. No failed or slow query is excluded.

| Scale | PGAccel s | DuckDB s | ClickHouse s | DuckDB / PG | CH / PG |
|---|---:|---:|---:|---:|---:|
| SF10 | 0.972 | 5.130 | 11.453 | 5.278x | 11.783x |
| SF100 | 10.578 | 51.703 | 97.474 | 4.888x | 9.215x |

## SF10

Client-observed milliseconds; warm medians exclude only trial one.

| Query | Target | PGAccel ms | DuckDB ms | ClickHouse ms | DuckDB / PG | CH / PG |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 3x | 45.440 | 222.700 | 711.502 | 4.901x | 15.658x |
| Q2 | 3x | 11.989 | 78.637 | 299.309 | 6.559x | 24.966x |
| Q3 | 3x | 32.678 | 170.604 | 476.176 | 5.221x | 14.572x |
| Q4 | 3x | 23.599 | 175.600 | 343.856 | 7.441x | 14.571x |
| Q5 | 3x | 26.883 | 177.408 | 589.049 | 6.599x | 21.912x |
| Q6 | 3x | 7.935 | 52.253 | 203.518 | 6.585x | 25.647x |
| Q7 | 2x | 36.648 | 182.643 | 580.760 | 4.984x | 15.847x |
| Q8 | 2x | 38.607 | 230.268 | 619.583 | 5.964x | 16.048x |
| Q9 | 2x | 150.625 | 703.468 | 896.066 | 4.670x | 5.949x |
| Q10 | 2x | 93.810 | 344.288 | 615.572 | 3.670x | 6.562x |
| Q11 | 3x | 7.880 | 41.182 | 181.846 | 5.226x | 23.078x |
| Q12 | 3x | 40.906 | 139.808 | 361.228 | 3.418x | 8.831x |
| Q13 | 3x | 98.380 | 552.679 | 453.065 | 5.618x | 4.605x |
| Q14 | 3x | 14.502 | 104.778 | 260.153 | 7.225x | 17.939x |
| Q15 | 3x | 11.877 | 89.018 | 461.365 | 7.495x | 38.846x |
| Q16 | 3x | 37.884 | 184.070 | 322.182 | 4.859x | 8.504x |
| Q17 | 3x | 17.218 | 181.173 | 578.105 | 10.523x | 33.577x |
| Q18 | 3x | 125.969 | 391.933 | 741.779 | 3.111x | 5.889x |
| Q19 | 3x | 21.750 | 251.815 | 583.625 | 11.578x | 26.833x |
| Q20 | 3x | 20.488 | 132.844 | 433.588 | 6.484x | 21.163x |
| Q21 | 2x | 62.634 | 609.171 | 1539.659 | 9.726x | 24.582x |
| Q22 | 2x | 44.272 | 113.574 | 200.971 | 2.565x | 4.540x |

## SF100

Client-observed milliseconds; warm medians exclude only trial one.

| Query | Target | PGAccel ms | DuckDB ms | ClickHouse ms | DuckDB / PG | CH / PG |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 3x | 440.185 | 2403.602 | 6649.361 | 5.460x | 15.106x |
| Q2 | 3x | 77.479 | 337.937 | 1345.272 | 4.362x | 17.363x |
| Q3 | 3x | 430.604 | 1976.796 | 3939.540 | 4.591x | 9.149x |
| Q4 | 3x | 209.989 | 1685.489 | 2949.705 | 8.027x | 14.047x |
| Q5 | 3x | 374.163 | 1997.345 | 4842.277 | 5.338x | 12.942x |
| Q6 | 3x | 79.652 | 513.654 | 1816.369 | 6.449x | 22.804x |
| Q7 | 2x | 521.101 | 2005.950 | 4598.745 | 3.849x | 8.825x |
| Q8 | 2x | 658.360 | 1998.868 | 5292.148 | 3.036x | 8.038x |
| Q9 | 2x | 2174.809 | 6388.575 | 8905.443 | 2.938x | 4.095x |
| Q10 | 2x | 1160.066 | 3003.827 | 5006.751 | 2.589x | 4.316x |
| Q11 | 3x | 30.496 | 323.997 | 941.140 | 10.624x | 30.861x |
| Q12 | 3x | 341.982 | 1494.902 | 3434.824 | 4.371x | 10.044x |
| Q13 | 3x | 1093.312 | 5595.847 | 4769.721 | 5.118x | 4.363x |
| Q14 | 3x | 132.674 | 1124.596 | 2248.999 | 8.476x | 16.951x |
| Q15 | 3x | 146.978 | 829.427 | 4184.006 | 5.643x | 28.467x |
| Q16 | 3x | 213.195 | 1001.576 | 1378.175 | 4.698x | 6.464x |
| Q17 | 3x | 272.281 | 2563.513 | 4134.933 | 9.415x | 15.186x |
| Q18 | 3x | 733.755 | 4802.045 | 8271.662 | 6.544x | 11.273x |
| Q19 | 3x | 217.095 | 2547.487 | 4909.323 | 11.734x | 22.614x |
| Q20 | 3x | 211.185 | 1371.021 | 3173.749 | 6.492x | 15.028x |
| Q21 | 2x | 682.195 | 6558.207 | 13533.714 | 9.613x | 19.838x |
| Q22 | 2x | 375.958 | 1177.929 | 1147.882 | 3.133x | 3.053x |

## Method and limits

- Fifteen trials per query/engine at SF10, nine at SF100. The first trial
  is excluded only from timing medians; all trials are validated.
- Engines run sequentially with eight workers. Builds and tests do not
  overlap timed phases. The CPU governor and affinity are unchanged.
- SF100 services use a 192 GiB cgroup cap with swap disabled. PGAccel
  uses a 64 GiB query allocation budget. Cgroup peaks include charged
  cache and differ from process RSS or query allocations.
- Integer/decimal values and strings/dates are compared exactly. Floating
  results use max(1e-6 absolute, 1e-12 relative) tolerance. DuckDB supplies
  the typed oracle; its repeated answers are checked for consistency.
- ClickHouse uses MergeTree and the documented dialect adapters for
  floating ratios in Q8/Q14/Q17. Settings and SQL hashes are retained.
- No result cache or optional PGAccel index build is used. All join proofs,
  payload construction, temporary allocations and cleanup remain timed.
- Q11 returns no rows at SF10 and SF100. Its nonempty SF1 case is also
  validated; its scale timings should be interpreted with that result size.
- These are engineering measurements, not audited TPC Benchmark Results.

The SF10 sweep initially missed the former 4x threshold on Q12 and Q18.
The user accepted 3x on 2026-09-07; the original sweep is retained without
substituting faster diagnostic rechecks. All 16 queries assigned 3x pass
at both scales; the other six pass 2x. The original gate and revised gate
remain separately recorded.

## Provenance

Runtime source revision: `a7bccf3015e678fe02c5e5555d20857307f958bf`.

Module SHA256: `77fc9a0cf6f52ec8551536aa030ed53e9e74a45b28b4f13d91ba967d27a0808a`.

[Compact trial evidence](FINAL-ALL1.json) contains metadata, every client
timing, query memory/spill diagnostics, raw-file paths and raw-file hashes.
The full answers remain in the six `*-final-all1-sf*.jsonl` files. SF100
commands and resource samples are in `final-all1-sf100-phases.json` and
`final-all1-sf100-resources.jsonl`. Reproduction scripts and test logs are
in [final-all1-reproduce](final-all1-reproduce/README.md).

[Revised targets](ALL-QUERY-GOAL.md) · [Q8 design](Q8-DESIGN.md) ·
[Q9 design](Q9-DESIGN.md) · [Q10 design](Q10-DESIGN.md) ·
[Q21 design](Q21-DESIGN.md) · [Q22 design](Q22-DESIGN.md)

## Recorded memory usage

These measurements come exclusively from the completed timing sweep;
no query was rerun to collect memory. N/A means not recorded.

- Process RSS includes resident data, runtime state and retained allocator
  memory. Post-query samples can miss temporary peaks during execution.
- VmHWM is the process lifetime high-water mark, not an isolated query
  peak. Later queries inherit earlier peaks. The SF100 ClickHouse process
  includes server startup and preparation before timed queries.
- Service MemoryPeak includes charged file cache and all service processes;
  the reported maximum is from existing periodic samples. It is not
  interchangeable with RSS or the query allocation tracker.
- PGAccel query allocations exclude its resident dataset. Equivalent
  per-query allocation peaks were not recorded for DuckDB or ClickHouse,
  so no three-engine query-memory ranking is claimed.
- ClickHouse SF10 had no server PID sampling; its process memory is N/A.
  PGAccel loads its dataset eagerly, while DuckDB reads on demand, so
  memory immediately after opening is not an equal-work comparison.

| Scale | Engine | RSS after open GiB | Process HWM GiB | Sampled service peak GiB |
|---|---|---:|---:|---:|
| SF10 | PGAccel | 9.695 | 9.942 | N/A |
| SF10 | DuckDB | 0.060 | 3.313 | N/A |
| SF10 | ClickHouse | N/A | N/A | N/A |
| SF100 | PGAccel | 97.984 | 100.197 | 115.933 |
| SF100 | DuckDB | 0.063 | 23.598 | 41.892 |
| SF100 | ClickHouse | 0.735 | 15.715 | 27.775 |

### SF10 memory by query

RSS columns are the largest post-query sample across all trials, in GiB.
PG allocation is the largest tracked query allocation peak, in MiB.

| Query | PG RSS GiB | DuckDB RSS GiB | CH RSS GiB | PG allocation MiB |
|---|---:|---:|---:|---:|
| Q1 | 9.697 | 0.750 | N/A | 1.008 |
| Q2 | 9.699 | 0.929 | N/A | 2.027 |
| Q3 | 9.778 | 1.232 | N/A | 54.061 |
| Q4 | 9.779 | 1.403 | N/A | 14.505 |
| Q5 | 9.779 | 1.463 | N/A | 20.612 |
| Q6 | 9.779 | 1.448 | N/A | 0.591 |
| Q7 | 9.779 | 1.572 | N/A | 52.812 |
| Q8 | 9.780 | 1.603 | N/A | 32.973 |
| Q9 | 9.780 | 2.892 | N/A | 27.939 |
| Q10 | 9.905 | 2.350 | N/A | 213.803 |
| Q11 | 9.905 | 2.034 | N/A | 7.916 |
| Q12 | 9.905 | 1.786 | N/A | 22.113 |
| Q13 | 9.905 | 2.488 | N/A | 45.886 |
| Q14 | 9.905 | 2.434 | N/A | 1.239 |
| Q15 | 9.905 | 2.109 | N/A | 7.737 |
| Q16 | 9.942 | 2.347 | N/A | 37.219 |
| Q17 | 9.921 | 2.172 | N/A | 1.785 |
| Q18 | 9.921 | 2.827 | N/A | 11.915 |
| Q19 | 9.921 | 2.328 | N/A | 3.222 |
| Q20 | 9.920 | 2.135 | N/A | 1.931 |
| Q21 | 9.920 | 2.336 | N/A | 51.796 |
| Q22 | 9.920 | 2.266 | N/A | 3.240 |

### SF100 memory by query

RSS columns are the largest post-query sample across all trials, in GiB.
PG allocation is the largest tracked query allocation peak, in MiB.

| Query | PG RSS GiB | DuckDB RSS GiB | CH RSS GiB | PG allocation MiB |
|---|---:|---:|---:|---:|
| Q1 | 97.986 | 5.078 | 0.724 | 1.008 |
| Q2 | 98.001 | 5.955 | 2.916 | 18.882 |
| Q3 | 98.233 | 8.503 | 2.066 | 476.221 |
| Q4 | 98.232 | 11.523 | 1.787 | 143.251 |
| Q5 | 98.237 | 13.319 | 1.470 | 254.032 |
| Q6 | 98.237 | 13.023 | 1.178 | 0.591 |
| Q7 | 98.200 | 13.710 | 2.329 | 471.105 |
| Q8 | 98.201 | 14.334 | 3.057 | 460.579 |
| Q9 | 98.223 | 17.082 | 12.285 | 273.645 |
| Q10 | 98.201 | 17.829 | 7.982 | 2132.544 |
| Q11 | 98.207 | 17.814 | 3.073 | 76.707 |
| Q12 | 98.207 | 15.562 | 1.221 | 215.232 |
| Q13 | 98.208 | 21.417 | 6.112 | 457.874 |
| Q14 | 98.207 | 20.413 | 4.158 | 3.386 |
| Q15 | 98.222 | 18.672 | 1.800 | 77.367 |
| Q16 | 98.255 | 18.897 | 2.001 | 311.140 |
| Q17 | 98.247 | 18.450 | 0.819 | 21.442 |
| Q18 | 98.247 | 21.437 | 9.717 | 110.170 |
| Q19 | 98.247 | 18.900 | 0.883 | 9.662 |
| Q20 | 98.270 | 18.635 | 1.091 | 17.252 |
| Q21 | 98.353 | 19.824 | 6.108 | 517.640 |
| Q22 | 98.353 | 19.662 | 5.282 | 30.609 |

All recorded SF100 service samples show zero swap and zero OOM events.
All PGAccel trials at both scales report zero spill bytes.
