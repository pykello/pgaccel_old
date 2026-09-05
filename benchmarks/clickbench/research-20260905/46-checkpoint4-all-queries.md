# Checkpoint 4: all 43 queries are faster than ClickHouse

PGAccel wins **43/43 queries** in the fresh paired resident warm comparison
on the complete **99,997,497-row, 105-column** ClickBench fixture. Every query's
warm client latency is strictly lower than ClickHouse's in this run.
The extended stopping target is met.

| Complete-suite metric | Result |
| --- | ---: |
| PGAccel query wins | 43/43 (100%) |
| Paired geometric PG/CH ratio with 10 ms shift | 0.267877054 |
| Inverse shifted score | 3.733× |
| Unshifted paired geometric ratio | 0.113187970 |
| Sum of PGAccel warm minima | 7.310909 s |
| Sum of ClickHouse warm minima | 21.525867 s |

These are **experimental indexed, resident warm measurements on one host**.
They establish the requested comparison, not a default unindexed, cold-start,
concurrent-service, or universal workload advantage. This is not an official
leaderboard submission. Preprocessing is disclosed below, and Q4's declared
arithmetic policy remains explicit.

## Frozen engine and comparison protocol

- Branch: `research/clickbench-20260905`.
- Engine source: `e7dd24716ebbb6d592fb8138481c2011cbe10c88`. Later commits only document the result.
- Runner SHA-256: `f998c644be37c68fe605fef1d7a2a887b1828938671f3129678f1937990ef218`.
- Query file SHA-256: `a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219`.
- ClickHouse version: `26.8.2.7`.
- PGAccel run started `2026-09-05T18:58:55.434233+00:00`; the fresh ClickHouse run started
  `2026-09-05T19:02:48.663735+00:00`.
- All 43 pinned queries are unchanged. Both engines use eight threads, three
  trials per query and a 32 GiB query allocation limit. Result caching is off.
- Client timing covers request through complete response. Warm means
  `min(trial_2, trial_3)`; raw files retain every client and executor sample.
- Engines, compilation and profiling run sequentially. No profiled executions
  are used in this checkpoint's timings, and no subset minima are substituted.
- Hardware: Xeon Gold 6152, 22 physical cores / 44 logical CPUs, approximately
  251 GiB RAM, AVX-512. Prepared toolchain and commands are in
  [reproduce.md](reproduce.md).

## Correctness and arithmetic policy

All **129 PGAccel trial answers** and **129 fresh ClickHouse trial answers**
pass their gates. ClickHouse has 106 direct saved-oracle matches and
23 source-verified tied-order alternatives. DuckDB checks actual
source Parquet rows, candidate aggregates and boundary ranks where SQL leaves
ordering ties unspecified. Unsupported differences remain failures.
All **95 native tests and 95 ASan/UBSan tests pass** for this engine.

Q4 is not an identical-arithmetic comparison. PGAccel's 128-bit accumulation
returns the mathematically correct `AVG(UserID)`, approximately
`2.528953029789716e18`. ClickHouse's native Int64 accumulation overflows on
this dataset and returns approximately `-55945124888.916`. The timed SQL is
unchanged; PGAccel is checked against the independent wide-average oracle and
ClickHouse against its native result, following the declared research policy.
The 43/43 claim uses that policy rather than hiding the difference.

## All query timings

Milliseconds of warm client latency. Per-query ratios are PGAccel/ClickHouse
without a shift; every value is below one.

| Query | PGAccel ms | ClickHouse ms | Ratio |
| --- | ---: | ---: | ---: |
| Q1 | 0.033 | 3.659 | 0.009 |
| Q2 | 1.112 | 3.666 | 0.303 |
| Q3 | 5.050 | 67.279 | 0.075 |
| Q4 | 28.764 | 66.377 | 0.433 |
| Q5 | 0.057 | 407.699 | 0.000 |
| Q6 | 0.055 | 434.601 | 0.000 |
| Q7 | 0.104 | 24.165 | 0.004 |
| Q8 | 6.205 | 27.010 | 0.230 |
| Q9 | 347.337 | 686.037 | 0.506 |
| Q10 | 596.742 | 757.850 | 0.787 |
| Q11 | 104.017 | 189.987 | 0.547 |
| Q12 | 106.754 | 219.051 | 0.487 |
| Q13 | 53.687 | 495.486 | 0.108 |
| Q14 | 364.063 | 629.701 | 0.578 |
| Q15 | 176.444 | 580.378 | 0.304 |
| Q16 | 39.526 | 383.298 | 0.103 |
| Q17 | 1026.186 | 1334.791 | 0.769 |
| Q18 | 9.024 | 464.297 | 0.019 |
| Q19 | 1356.368 | 2405.289 | 0.564 |
| Q20 | 0.160 | 5.952 | 0.027 |
| Q21 | 121.625 | 326.290 | 0.373 |
| Q22 | 40.947 | 109.637 | 0.373 |
| Q23 | 50.863 | 542.875 | 0.094 |
| Q24 | 16.434 | 106.447 | 0.154 |
| Q25 | 2.906 | 76.720 | 0.038 |
| Q26 | 3.932 | 256.710 | 0.015 |
| Q27 | 2.775 | 61.601 | 0.045 |
| Q28 | 143.676 | 158.444 | 0.907 |
| Q29 | 1205.125 | 1914.472 | 0.629 |
| Q30 | 3.643 | 91.754 | 0.040 |
| Q31 | 254.554 | 308.986 | 0.824 |
| Q32 | 83.122 | 403.665 | 0.206 |
| Q33 | 503.354 | 2700.151 | 0.186 |
| Q34 | 275.301 | 2285.756 | 0.120 |
| Q35 | 274.226 | 2316.387 | 0.118 |
| Q36 | 22.193 | 311.046 | 0.071 |
| Q37 | 3.476 | 76.184 | 0.046 |
| Q38 | 5.553 | 50.215 | 0.111 |
| Q39 | 5.449 | 45.455 | 0.120 |
| Q40 | 45.497 | 121.197 | 0.375 |
| Q41 | 5.613 | 23.504 | 0.239 |
| Q42 | 11.397 | 29.924 | 0.381 |
| Q43 | 7.563 | 21.873 | 0.346 |

## Closing the four losses from checkpoint 3

| Query | Main change | Report |
| --- | --- | --- |
| Q28 | Dense exact string-length aggregates by global group ID | [38](38-dense-string-length.md) |
| Q22 | Cheap comparisons first; evaluate only labels reached by sparse selections | [43](43-sparse-predicates.md) |
| Q24 | Push inclusive local top-K boundaries ahead of expensive filters | [44](44-projection-boundaries.md) |
| Q29 | Weighted dictionary evaluation and a restricted exact delimiter-capture plan | [39](39-weighted-dictionary.md), [42](42-capture-prefix.md), [45](45-delimited-capture.md) |

Local signature layout and vectorized rejection also improved the string
filter path ([40](40-local-signatures.md), [41](41-simd-signatures.md)).
Each implementation was committed separately with an explanatory body.
The restricted regex compiler uses literal alternatives and delimiter scans;
it recognizes a grammar, not SQL query IDs, benchmark column names or hosts.
Unsupported expressions retain the generic executor or RE2 fallback.

## Setup and memory costs

`PGACCEL_BUILD_DICTIONARIES=1` enables the experimental generic dictionaries
and inverse row indexes with a 64 GiB index budget. The indexes retain actual
values, IDs, signatures and row positions, not query answers. Default loading
does not enable this configuration. Per-query weights, filters, heaps and
aggregate tables are constructed inside timed execution.

| Measurement | Checkpoint 4 |
| --- | ---: |
| Native loading | 49.161 s |
| Index construction | 142.189 s |
| Combined loading/indexing | 191.350 s |
| Indexed columns | 50 |
| Persistent auxiliary index bytes | 29,615,085,204 (27.581 GiB) |
| Peak index allocation | 29,615,088,768 bytes |
| Process peak RSS after load/indexing | 61.521 GiB |
| Maximum recorded process peak RSS through queries | 64.586 GiB |

The extra local signature layout adds about 1.406 GiB relative to checkpoint 3.
RSS also includes loaded native columns, allocator retention and other process
overhead; index bytes are not whole-process memory. Resident and per-query
budgets are still separate. Native conversion, original ingestion, restart,
concurrent clients and index persistence are outside this warm comparison.

No new external dependency was retained. The continued code is original,
using the already-audited RE2 fallback and public-domain hash finalizer.
See [dependencies](dependencies.md) and [primary research references](references.md).

## Evidence and next steps

The external mirror in `~/projects/docs/clickbench/research-20260905/` contains:

- [Machine-readable comparison](checkpoint4-indexed-comparison.json)
- [PGAccel trials](pg-checkpoint4-indexed.jsonl), [metadata](pg-checkpoint4-indexed.meta.json),
  [129-answer gate](pg-checkpoint4-indexed-gate.json)
- [Fresh ClickHouse trials](ch-checkpoint4.jsonl), [metadata](ch-checkpoint4.meta.json),
  [129-answer gate](ch-checkpoint4-gate.json)
- [Native tests](native-checkpoint4.log) and [sanitizer tests](sanitize-checkpoint4.log)

Frozen binary:
`~/projects/clickbench-work/research-20260905/bin/checkpoint4-indexed-query-runner`.
Its checksum matches both metadata and the current build.

[Checkpoint 3](36-checkpoint3-target.md) remains the historical 39/43 result;
its evidence is preserved. The [staged follow-up plan](47-follow-up-plan.md)
now focuses on robustness, total memory, persistence and a limited useful
service rather than remaining ClickBench losses. [Reproduction instructions](reproduce.md)
include the exact loading, validation and scoring commands.
