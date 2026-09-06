# Q7 SF10 iterations

Complete native Python call, nine trials, first trial excluded from warm
medians. Every answer must match the saved DuckDB oracle. These are not
fresh promotion confirmations.

| Checkpoint | Client ms | Engine ms | Peak query bytes |
|---|---:|---:|---:|
| before-native | 10190.153 | 10189.877 | 902447808 |
| separate-v1 | 52.560 | 52.306 | 56092858 |
| profile-v1 | 53.998 | 53.755 | 56092858 |
| simd-v1 | 51.453 | 51.184 | 56092870 |
| profile-v2 | 46.014 | 45.759 | 56092870 |
| batched-v1 | 43.448 | 43.177 | 56092870 |

## Diagnostic phase medians

| Phase | Warm ms |
|---|---:|
| q7phase/proof | 1.688 |
| q7phase/pairs | 0.288 |
| q7build/allocate-4 | 0.001 |
| q7build/build-4 | 0.009 |
| q7build/finish-4 | 0.008 |
| q7build/allocate-0 | 0.017 |
| q7build/build-0 | 0.416 |
| q7build/finish-0 | 0.228 |
| q7build/allocate-5 | 0.008 |
| q7build/build-5 | 0.008 |
| q7build/finish-5 | 0.007 |
| q7build/allocate-3 | 0.113 |
| q7build/build-3 | 1.296 |
| q7build/finish-3 | 0.010 |
| q7build/allocate-2 | 0.398 |
| q7build/build-2 | 27.242 |
| q7build/rank-2 | 3.897 |
| q7build/finish-2 | 1.692 |
| q7phase/payloads | 35.318 |
| q7phase/fact | 14.726 |
| q7phase/output | 0.252 |

The instrumented module is diagnostic only. Source was restored before
building retained candidates. Table 2 is orders, table 3 is customer, table
0 is supplier, and tables 4/5 are the nation aliases. Order propagation is
the dominant build cost, motivating SIMD gathers from narrow payload arrays.

[Design](Q7-DESIGN.md) · [All trial diagnostics](Q7-ITERATIONS.json)
