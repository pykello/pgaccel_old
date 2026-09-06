# CPU SIMD optimization results

The final frozen binary passes the fourfold warm-median target against
both DuckDB and ClickHouse for Q1, Q14, and Q3 at SF10 and SF100.
Every measured trial validates. Q1 is an aggregation; Q14 and Q3 exercise
the new join pipelines. This is a three-query checkpoint, not a claim
about all TPC-H or ClickBench queries.

| Report | Contents |
|---|---|
| [Current SF100 comparison](Q3-SF100.md) | Same binary, all three engines, nine trials each, memory peaks |
| [Fresh SF10 confirmation](SF10-CONFIRM.md) | Fifteen trials each, medians and early-trial limitations |
| [Algorithms](ALGORITHMS.md) | Dense arithmetic, positional joins, bitmap rank, SIMD probes, exactness |
| [Q3 iteration history](Q3-SF10.md) | Retained changes and rejected experiments |
| [Q1 iteration history](RESULTS.md) | Dense grouping and SIMD arithmetic experiments |
| [Research and next stages](PLAN.md) | Primary sources, license policy, broader join work |
| [Earlier Q1 SF100](Q1-SF100.md) | Faster Q1-only checkpoint, a different frozen binary |
| [Earlier Q14 SF100](Q14-SF100.md) | Prior fused join checkpoint and Q1 recheck |

PGAccel engine revision: `68b24ea`, on branch
`feature/tpch-coverage-20260905`. Each retained engine change is a
separate commit with a wrapped explanatory body. The final native suite
passes 146 tests; the full sanitizer suite passed before the final
empty-state guard, whose focused sanitizer checks also pass.

New implementations are original and add no dependencies. Fast paths
require exact data-layout and arithmetic proofs; unsupported shapes keep
the general executor. Temporary indexes and all result processing are
included in query time. No answers or join directories persist between
queries. Data loading is separate and its memory footprint is reported.

These reports are mirrored under `/home/hadi/projects/docs/tpch/` in
`simd-20260906/`, where raw JSONL, profiles, service logs, and rerun scripts
are also retained. The previous full-workload baseline remains in
`/home/hadi/projects/docs/tpch/sf100-20260905/RESULTS.md`.
