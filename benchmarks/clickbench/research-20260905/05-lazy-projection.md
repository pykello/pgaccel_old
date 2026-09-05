# Experiment 6: reserved distinct input and lazy projection — keep

Reserve the exact unfiltered input size before materializing radix distinct
values. This removes vector growth and lowers Q5's tracked allocation peak
from 1,873,861,608 to 1,600,099,760 bytes.

For eligible projection queries, filter stripes across eight workers and
maintain a bounded candidate heap in each worker. Construct projected cells
only when candidate ordering needs them. Merge these candidates with the same
ordering and offset rules. Preserve the existing input-prefix behavior for
unordered LIMIT queries. Computed strings own their storage across worker
completion; the regression tests include hidden ordering and OFFSET.

All 40 native tests pass. All 18 full-data trial answers pass the explicit
subset gate, including independent tie validation for Q25.

| Query | Baseline PG warm ms | Experiment warm ms | Recorded CH warm ms |
| --- | ---: | ---: | ---: |
| Q5 | 9029.440 | 833.906 | 483.858 |
| Q20 | 32.287 | 6.333 | 5.949 |
| Q24 | 1953.528 | 258.373 | 130.245 |
| Q25 | 1226.700 | 44.804 | 80.205 |
| Q26 | 1794.540 | 164.410 | 236.562 |
| Q27 | 1234.628 | 45.506 | 61.510 |

Q25, Q26, and Q27 beat the recorded ClickHouse comparison. Q20 and Q24
still need work. These are selected-query measurements, not a fresh overall
score. Each engine uses eight threads; elapsed client time includes response
transport. No query result cache or additional preprocessing was introduced.

Original implementation; no additional dependencies. Raw evidence lives in
`../docs/clickbench/research-20260905/lazy-projection.jsonl` and
`lazy-projection-gate.json`. The frozen binary is
`../clickbench-work/research-20260905/bin/lazy-projection-query-runner`.
