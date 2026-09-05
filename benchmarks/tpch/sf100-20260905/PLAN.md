# SF100 three-engine comparison

The user requested a roughly 100 GB TPC-H dataset, then asked to optimize
memory before comparing PGAccel, ClickHouse, and DuckDB. SF100 is the chosen
scale; its actual physical storage and resident sizes will be measured.

## Completed prerequisites

- All 22 unmodified DuckDB 1.4.3 TPC-H queries pass against the complete SF1
  Parquet oracle, three trials each, after compact join keys (`294c36b`).
- Q13 and Q15 each pass three complete SF10 trials. A full SF10 corpus gate
  is still distinct from these focused checks.
- SF10 Q21 query allocation peak fell from 12.81 to 4.18 GiB, with identical
  answers; Q3 fell 52.2% and Q13 fell 40.6%. See the coverage checkpoint.
- Native and ASan/UBSan suites both pass 129/129 tests. UBSan exposed a
  temporary descriptor's uninitialized layout enum; `ffc3b3c` initializes
  it before moving the descriptor, without changing stored layouts.
- The three-engine driver passes all 22 queries across three trials on
  the same small SF0.001 fixture in all three engines. This rehearsal is
  for correctness, not performance: preparation work ran concurrently.

Frozen PGAccel binary:
`/home/hadi/projects/tpch-work/checkpoints/sf100-query-runner`

SHA-256:
`be6fd84981b9300c255ef6b2b60f5fb31299fc8dc311b22ae15d8522d442ab3b`

The first PGAccel attempt exposed a runner configuration bug: the root
received 64 GiB, but nested plans retained their default 8 GiB limit. Q2
stopped at that child cap while total process RSS was about 106 GiB.
`5eb5fc4` applies statement limits throughout the parsed plan; runtime
remaining-parent accounting still bounds combined allocations. The full
PGAccel phase is restarted from Q1 with this corrected binary. The partial
attempt is archived as `inherited-child-limit-pgaccel.jsonl` and its log.
The completed DuckDB phase remains valid and supplies the saved oracle.

## Host and resource contract

The host reports 251.35 GiB RAM, one Xeon Gold 6152 socket, 22 physical cores
and 44 logical CPUs. The user requested a 192 GiB budget, so benchmark
engine services use `MemoryMax=192G` and `MemorySwapMax=0` through the user
systemd manager. Generation uses a smaller 128 GiB service cap and a
64 GiB DuckDB buffer limit.

Only one SF100 engine runs its query phase at a time. DuckDB writes saved
oracle answers first and exits; PGAccel and ClickHouse read those answers
without running DuckDB. The existing ClickHouse installation remains
untouched. Its older small-scale tables are outside the benchmark process
cap and will be reported as background host memory.

| Engine | Source representation | Query settings |
|---|---|---|
| PGAccel | All eight native tables loaded; auxiliary indexes disabled | 64 GiB query allocations plus resident inputs; fast paths enable eight workers, relational joins remain serial; 512 GiB spill quota where supported |
| DuckDB 1.4.3 | Generated persistent native tables, normal buffer manager | Eight threads, 176 GiB memory limit, 512 GiB temporary spill limit |
| ClickHouse 26.8.2.7 | Memory tables loaded from matching Parquet | Eight threads, 64 GiB query limit, query cache off, `join_use_nulls=1`, `aggregate_functions_null_for_empty=1` |

ClickHouse uses a dedicated instance on HTTP 18123/native 19000. Its server
memory threshold is 184 GiB inside the 192 GiB service cap. Insertion is
allowed 160 GiB because Memory tables may retain allocations throughout
an INSERT; measured queries override that setting with 64 GiB. The full
ClickHouse Memory dataset is loaded only for its phase and is released
when that isolated service stops. Its source Parquet remains available.

The NULL settings align semantics: the small rehearsal found empty SUMs
in Q17 and Q19 that ClickHouse otherwise returned as zero. SQL statements
are unchanged. No materialized answers or workload-specific indexes are
introduced.

## Timing and correctness

Run Q1–Q22 in order, three consecutive trials per query, with a 1,800-second
limit per trial. Report all trials, first-trial time, and warm minimum over
trials 2–3. Report failures, timeouts, and unavailable oracles explicitly.
Do not compute an overall winning total by silently dropping failures.

The primary comparison is client-observed SQL execution plus fetching and
answer conversion. PGAccel execution-only and ClickHouse server timing
are separate diagnostics. DuckDB's first query may read native disk blocks;
its open time is not an all-table preload time. PGAccel and ClickHouse load
costs and physical/resident sizes must be reported separately. Warm data
and this resident ClickHouse layout are not a cold-storage comparison or
an exhaustive search for each engine's best configuration.

Check every returned row: exact integers/decimals and strings/dates;
floating tolerance max(1e-6 absolute, 1e-12 relative). DuckDB trials must
agree with each other; other engines are checked against the saved typed
DuckDB answers. Record the exact SQL hash and frozen runner hash. A missing
oracle leaves validation unavailable rather than inventing a pass.

`VmRSS` and `VmHWM` refer to the engine process; HWM is cumulative since
process start, including loading, not a per-query delta. PGAccel also
reports budgeted query allocation peaks. Service memory includes charged
file cache, so its cap is not interchangeable with RSS or the engine's
allocator counter.

## Staged execution

1. Generate the persistent DuckDB database and eight nonnullable Parquet
   tables with `benchmarks/tpch/generate.py`. Preserve counts, types, sizes,
   versions, and hashes in the data manifest.
2. Import native snapshots one table at a time with `prepare_native.py`,
   verifying every source row count before publishing completed snapshots.
3. Finish all builds and setup before timed queries. Run `compare_scale.py
   --engine duckdb` inside a 192 GiB service, saving the typed answers.
4. Run `compare_scale.py --engine pgaccel` in a separate 192 GiB service
   with the frozen binary and saved DuckDB oracle; release the process.
5. Load ClickHouse Memory tables with `load_clickhouse.py`, verify counts,
   then run `compare_scale.py --engine clickhouse` with the saved oracle.
   Archive the configuration, load report, and results before stopping the
   isolated instance.
6. Write a per-query comparison and measured memory report in this folder,
   including any failures and specific optimization work suggested by the
   measurements. Do not claim earlier ClickBench wins for this binary
   without repeating that workload.

Work files are under `/home/hadi/projects/tpch-work/sf100/`. Raw results and
reports belong in `/home/hadi/projects/docs/tpch/sf100-20260905/`, with a
versioned compact report under `benchmarks/tpch/sf100-20260905/`.

These are engineering measurements, not audited TPC Benchmark Results.
The generated data and TPC query corpus remain external. All new PGAccel
algorithms are original implementations with no added engine dependency.
