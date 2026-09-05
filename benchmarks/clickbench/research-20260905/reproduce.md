# Reproduce the indexed research comparison

Run from the pgaccel checkout on the prepared host. Initial installation,
dataset download, and ClickHouse setup remain in ../docs/tpch/ and
../docs/clickbench/README.md. This measures complete resident warm queries;
it does not produce an official cold-cache leaderboard submission.

Checkpoint 4 source is e7dd247 on research/clickbench-20260905. The paths below
already exist. Use new output names to preserve earlier evidence.

## Build and correctness tests

```bash
cmake -S . -B build -G Ninja
cmake --build build --target query_runner run_tests -j8
ctest --test-dir build --output-on-failure
python3 tests/audit_gate_test.py
python3 tests/count_tie_validation_test.py
```

The prepared build uses GCC 13.3, C++20, -O3, AVX-512F/BW, Arrow/Parquet,
TBB, and RE2. The host is a Xeon Gold 6152 with 22 physical cores,
44 logical CPUs, and approximately 251 GiB RAM. Execution uses eight workers.
The research introduced no retained library dependency.

## Complete PGAccel run

```bash
PGACCEL_BUILD_DICTIONARIES=1 \
PGACCEL_INDEX_MEMORY_BYTES=68719476736 \
python3 benchmarks/clickbench/warm.py \
  --engine pgaccel --threads 8 --repeats 3 --timeout 3600 \
  --native /home/hadi/projects/clickbench-work/hits-stage3.pgaccel \
  --oracle /home/hadi/projects/clickbench-work/ci-fixtures/clickhouse.jsonl \
  --output /home/hadi/projects/docs/clickbench/research-20260905/pg-reproduction.jsonl
```

The harness checks all 99,997,497 rows and 105 columns are loaded.
It sets a 32 GiB query allocation budget. The index limit is separate.
Index construction and loading appear in metadata outside query timings;
query-local buffers are built within the timed query.

Set PGACCEL_BUILD_DICTIONARIES=0 for a separately labeled unindexed run.
Do not substitute an unindexed or mixed-subset result into an indexed score.

Validate all three trials, including the declared wide-average policy and
independent deterministic tie refinements:

```bash
python3 benchmarks/clickbench/validate_trials.py \
  --results /home/hadi/projects/docs/clickbench/research-20260905/pg-reproduction.jsonl \
  --wide-oracle /home/hadi/projects/clickbench-work/ci-fixtures/wide-average.json \
  --refinements /home/hadi/projects/clickbench-work/ci-fixtures/refinements.jsonl \
  --output /home/hadi/projects/docs/clickbench/research-20260905/pg-reproduction-gate.json
```

## Fresh ClickHouse run and source verification

Run after PGAccel exits. Stop compilation and other benchmarking during timing.

```bash
TZ=UTC python3 benchmarks/clickbench/warm.py \
  --engine clickhouse --threads 8 --repeats 3 \
  --database clickbench_audit \
  --oracle /home/hadi/projects/clickbench-work/ci-fixtures/clickhouse.jsonl \
  --output /home/hadi/projects/docs/clickbench/research-20260905/ch-reproduction.jsonl

/home/hadi/projects/tpch-work/venv/bin/python \
  benchmarks/clickbench/validate_clickhouse_trials.py \
  --results /home/hadi/projects/docs/clickbench/research-20260905/ch-reproduction.jsonl \
  --oracle /home/hadi/projects/clickbench-work/ci-fixtures/clickhouse.jsonl \
  --parquet /home/hadi/projects/clickbench-work/hits.parquet \
  --output /home/hadi/projects/docs/clickbench/research-20260905/ch-reproduction-gate.json
```

ClickHouse uses the complete MergeTree table, eight threads, a 32 GiB query
memory limit, and disabled query-result caching. DuckDB source verification
runs after timing. A raw answer_mismatch may be a valid boundary tie; the
full gate decides. Unsupported differences remain failures to investigate.

## Score only complete validated runs

For each query, use min(trial 2, trial 3) of client_ms, represented by ms
in the driver output. A query win requires PG time strictly below CH time.
The agreed overall ratio is:

```text
exp(mean(log((PG_ms + 10) / (CH_ms + 10))))
```

The extended goal requires **43 of 43 wins** and a ratio below 1.
Checkpoint 4 satisfies that target; checkpoint 3 satisfied the earlier 39/43 target. Also report the unshifted ratio,
sum of warm times, raw trials, setup costs, and memory. Never replace a
complete checkpoint with best times picked from separate subsets.

Preserve each .jsonl, .meta.json, stderr file, gate, and frozen runner binary.
Metadata records source revision, query hash, binary hash, engine version,
and load/index costs. Copy the frozen runner before changing the build.

## Scope

Results establish this host's resident warm behavior for the unchanged
43-query corpus and disclosed indexed configuration. Cold restart/import,
multi-client service operation, joins, and new data distributions require
separate evaluation. The Q4 wide-AVG distinction remains explicit.
See protocol.md in the parent source benchmark directory for submission gaps.
