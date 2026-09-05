# Experiment 8: compact single-column count tables — keep

Replace individually allocated grouped-count nodes with the original flat
lookup table and contiguous payloads. Raw numerical keys use eight bytes;
string keys retain logical string views. Dictionary caches now contain a
worker-owner identifier and stable payload index instead of a pointer that
vector growth would invalidate. Merge exact counts and release source arrays.

All 44 native tests pass, including dictionary cache growth, signed extremes,
filters, date grouping, and spill fallback. All 24 benchmark trials pass the
subset gate; Q39's raw tied-row mismatch passes independent tie validation.

| Query | Baseline PG warm ms | Experiment warm ms | Recorded CH warm ms |
| --- | ---: | ---: | ---: |
| Q8 | 8.497 | 8.419 | 25.819 |
| Q13 | 3502.709 | 1032.395 | 493.533 |
| Q16 | 9663.634 | 1378.656 | 328.508 |
| Q34 | 12053.828 | 4856.235 | 2345.208 |
| Q35 | 11964.197 | 4917.029 | 2316.499 |
| Q37 | 107.451 | 42.269 | 58.922 |
| Q38 | 18.871 | 8.125 | 50.352 |
| Q39 | 9.188 | 7.195 | 46.255 |

Q37 is a new win. Q13, Q16, Q34, and Q35 improve but still trail ClickHouse.
No structural preprocessing, query result cache, or new dependency. Keep the
change while researching compact exact distinct state and shared dictionary
identities. A fresh full-suite comparison remains required for the goal.

Raw files: `../docs/clickbench/research-20260905/flat-count.jsonl` and
`flat-count-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/flat-count-query-runner`.
