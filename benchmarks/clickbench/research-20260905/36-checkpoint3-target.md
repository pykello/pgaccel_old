# Checkpoint 3: the requested warm comparison target is met

On the complete 99,997,497-row, 105-column ClickBench dataset, PGAccel wins
**39/43 queries (90.70%)** against a fresh ClickHouse 26.8.2.7 run.
The paired geometric mean PGAccel/ClickHouse latency ratio, with a 10 ms
shift, is **0.368585417**, or an inverse score of
**2.713×**. The sum of per-query warm minima is
**17.982 seconds** versus **21.694 seconds**.
These two overall summaries both favor PGAccel.

This meets the research stopping criterion. It is an **experimental indexed,
resident warm comparison on this machine**, with explicit preprocessing costs.
It is not evidence that the default unindexed engine, cold loading, concurrent
service, or every workload beats ClickHouse, nor an official leaderboard result.

## Protocol and correctness

- Frozen engine source: `6e704b67b4e3abf5f79596f949f24514fef2d6a9` on
  `research/clickbench-20260905`; subsequent report commits do not change it.
- Runner SHA-256: `b667946aef7c876db825960ba23e69d7d5dcb4969ebdd1cedc47f214861177fe`.
- Pinned query-file SHA-256: `a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219`.
- All 43 unchanged queries, eight threads, three trials per query, a 32 GiB
  query allocation budget, and no query-result cache.
- Client latency measures request through complete response. The warm value
  is `min(trial_2, trial_3)`. Raw files also preserve executor timings.
- Engines ran sequentially without concurrent compilation or benchmark work.
  PGAccel started at 2026-09-05T11:43:01.532147+00:00; ClickHouse at
  2026-09-05T11:47:52.356710+00:00.
- All **129 PGAccel trial answers** and **129 ClickHouse trial answers** pass
  their validation gates. For ClickHouse, 107 match the saved oracle directly;
  22 tied-order alternatives are independently checked against the source
  Parquet using DuckDB, including complete candidate aggregates and ranks.
- **84/84 native tests and 84/84 ASan+UBSan tests pass** for this engine.

The declared Q4 policy is material: PGAccel computes `AVG(UserID)` with a
128-bit sum and returns approximately `2.528953029789716e18`. ClickHouse's
native Int64 accumulation overflows on this dataset and returns approximately
`-55945124888.916`. PGAccel is validated against an independent wide-average
oracle; ClickHouse against its native result. The timed SQL is unchanged, but
this is not a claim of bit-identical arithmetic for Q4. Tied ORDER BY results
are checked for valid membership and aggregates rather than arbitrary identical
boundary rows. See [reproduction and gates](reproduce.md).

## Preprocessing and memory costs

The runner explicitly enables generic global dictionaries and inverse row
postings with `PGACCEL_BUILD_DICTIONARIES=1` and a 64 GiB index budget.
The indexes store actual values, IDs, and row positions; they do not store
query answers. Default loading does not enable this experimental configuration.

| Measurement | Final checkpoint |
| --- | ---: |
| Native data loading | 48.137 s |
| Index construction | 136.132 s |
| Combined load | 184.270 s |
| Indexed columns | 50 |
| Persistent auxiliary index bytes | 28,105,839,348 (26.176 GiB) |
| Peak index allocation | 28,105,842,912 bytes |
| Process peak RSS after loading/indexing | 60.116 GiB |
| Maximum recorded process peak RSS through queries | 63.977 GiB |

RSS includes loaded data and allocator/process overhead; index bytes are not
whole-process memory. Query allocations and resident indexes have separate
budgets today. Native conversion/import is outside this warm comparison; these
numbers do not establish an ingestion or end-to-end cold-start advantage.
Hardware is a Xeon Gold 6152 host with 22 physical cores, 44 logical CPUs,
251 GiB RAM and AVX-512; each engine is limited to eight query threads.

## All query timings

Milliseconds of warm client latency. Lower wins; ratios are PGAccel/ClickHouse
without the shift, while the aggregate score above uses the 10 ms shift.

| Query | PGAccel ms | ClickHouse ms | Ratio | Winner |
| --- | ---: | ---: | ---: | --- |
| Q1 | 0.030 | 3.556 | 0.009 | PGAccel |
| Q2 | 1.111 | 3.707 | 0.300 | PGAccel |
| Q3 | 5.021 | 69.538 | 0.072 | PGAccel |
| Q4 | 26.728 | 73.221 | 0.365 | PGAccel |
| Q5 | 0.052 | 510.529 | 0.000 | PGAccel |
| Q6 | 0.052 | 411.751 | 0.000 | PGAccel |
| Q7 | 0.104 | 19.312 | 0.005 | PGAccel |
| Q8 | 6.390 | 23.753 | 0.269 | PGAccel |
| Q9 | 325.924 | 672.937 | 0.484 | PGAccel |
| Q10 | 604.752 | 738.672 | 0.819 | PGAccel |
| Q11 | 102.413 | 185.670 | 0.552 | PGAccel |
| Q12 | 106.749 | 217.463 | 0.491 | PGAccel |
| Q13 | 53.816 | 482.862 | 0.111 | PGAccel |
| Q14 | 361.762 | 635.356 | 0.569 | PGAccel |
| Q15 | 177.362 | 595.014 | 0.298 | PGAccel |
| Q16 | 39.371 | 363.485 | 0.108 | PGAccel |
| Q17 | 1006.174 | 1320.925 | 0.762 | PGAccel |
| Q18 | 8.442 | 451.513 | 0.019 | PGAccel |
| Q19 | 1346.386 | 2419.727 | 0.556 | PGAccel |
| Q20 | 0.190 | 6.087 | 0.031 | PGAccel |
| Q21 | 165.489 | 321.574 | 0.515 | PGAccel |
| Q22 | 168.090 | 105.183 | 1.598 | ClickHouse |
| Q23 | 268.931 | 556.302 | 0.483 | PGAccel |
| Q24 | 165.344 | 114.553 | 1.443 | ClickHouse |
| Q25 | 45.433 | 74.477 | 0.610 | PGAccel |
| Q26 | 166.293 | 240.797 | 0.691 | PGAccel |
| Q27 | 46.150 | 62.061 | 0.744 | PGAccel |
| Q28 | 876.656 | 162.139 | 5.407 | ClickHouse |
| Q29 | 10569.211 | 1948.511 | 5.424 | ClickHouse |
| Q30 | 3.611 | 88.120 | 0.041 | PGAccel |
| Q31 | 243.968 | 315.602 | 0.773 | PGAccel |
| Q32 | 78.520 | 427.361 | 0.184 | PGAccel |
| Q33 | 464.192 | 2784.942 | 0.167 | PGAccel |
| Q34 | 217.539 | 2299.778 | 0.095 | PGAccel |
| Q35 | 221.240 | 2303.025 | 0.096 | PGAccel |
| Q36 | 22.696 | 316.531 | 0.072 | PGAccel |
| Q37 | 3.576 | 64.389 | 0.056 | PGAccel |
| Q38 | 5.890 | 49.394 | 0.119 | PGAccel |
| Q39 | 5.417 | 46.924 | 0.115 | PGAccel |
| Q40 | 43.855 | 129.359 | 0.339 | PGAccel |
| Q41 | 8.342 | 28.404 | 0.294 | PGAccel |
| Q42 | 11.138 | 26.865 | 0.415 | PGAccel |
| Q43 | 7.149 | 22.820 | 0.313 | PGAccel |

## What changed and what remains

The earlier unindexed checkpoint won 18/43 (ratio 1.205092), and indexed
checkpoint 2 won 32/43 (0.468085). Those are historical engine checkpoints,
not fresh measurements of the current engine with indexing disabled.

The last seven additional wins came from reducing repeated memory work and
representing exact groups more compactly: parallel overwrite-only buffers,
bounded numeric domains, inverse postings for distinct counts, partitioned
numeric aggregation, date/time bucket keys, and conditional composite-key
layouts. Filtered radix reservation avoids copying growing input buffers,
while charging reservations against the remaining allocation budget.
[Reports 27–35](README.md) document individual measurements and correctness.

ClickHouse still wins Q22, Q24, Q28 and Q29. Q29 alone accounts for most of
PGAccel's remaining total time. The [follow-up plan](37-follow-up-plan.md)
prioritizes these losses and defines the work needed for a useful bounded
service. No further performance claim is made for unimplemented ideas.

The global substring-preparation experiment was rejected after slowing the
representative queries. The bitmap proposal has a retained standalone probe,
but the measured tradeoffs did not justify making it a default column format.
No new external library dependency is retained. Original implementations and
the public-domain SplitMix64 finalizer satisfy the license constraint; see the
[license audit](dependencies.md) and [primary references](references.md).

## Evidence and reproduction

Raw artifacts live beside the mirrored report in
`~/projects/docs/clickbench/research-20260905/`:

- [Machine-readable comparison](checkpoint3-indexed-comparison.json)
- [PGAccel trials](pg-checkpoint3-indexed.jsonl),
  [metadata and per-column construction](pg-checkpoint3-indexed.meta.json),
  [129-answer gate](pg-checkpoint3-indexed-gate.json)
- [Fresh ClickHouse trials](ch-checkpoint3.jsonl),
  [metadata](ch-checkpoint3.meta.json), [129-answer gate](ch-checkpoint3-gate.json)
- [Native test log](native-checkpoint3.log) and
  [sanitizer test log](sanitize-checkpoint3.log)

The frozen binary is
`~/projects/clickbench-work/research-20260905/bin/checkpoint3-indexed-query-runner`.
Its checksum matches both the benchmark metadata and the current build binary.
[Reproduction instructions](reproduce.md) include loading, benchmarking,
validation and the exact scoring formula. Earlier raw measurements remain
available; this report does not replace their historical evidence.
