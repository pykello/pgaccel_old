# Reproduce the SF100 comparison

Use a fresh work directory and fresh result paths: preparation and benchmark
scripts deliberately reject existing output. The commands below describe
this host's installed environment; earlier TPC-H setup guides cover package
installation. Run from `/home/hadi/projects/pgaccel_old`.

```bash
TPCH_WORK=/home/hadi/projects/tpch-work/sf100
TPCH_RESULTS=/home/hadi/projects/docs/tpch/sf100-20260905
TPCH_PYTHON=/home/hadi/projects/tpch-work/venv/bin/python
TPCH_QUERIES=/home/hadi/projects/tpch-work/queries-duckdb-1.4.3
TPCH_RUNNER=/home/hadi/projects/tpch-work/checkpoints/sf100-query-runner
```

The named paths above belong to the recorded run. Choose different names
for a new run. Engine services are capped at 192 GiB with swap disabled;
this is enforced by the user systemd manager, not merely a benchmark label.

## Prepare matching inputs

```bash
systemd-run --user --unit=pgaccel-tpch-sf100-generate \
  --property=MemoryMax=128G --property=MemorySwapMax=0 \
  --working-directory="$PWD" --wait --pipe \
  "$TPCH_PYTHON" benchmarks/tpch/generate.py --sf 100 \
  --output "$TPCH_WORK/data" --database "$TPCH_WORK/duckdb/tpch.duckdb"

systemd-run --user --unit=pgaccel-tpch-sf100-native \
  --property=MemoryMax=192G --property=MemorySwapMax=0 \
  --working-directory="$PWD" --setenv=TZ=UTC --wait --pipe \
  "$TPCH_PYTHON" benchmarks/tpch/prepare_native.py \
  --data "$TPCH_WORK/data" --native "$TPCH_WORK/native" \
  --runner "$TPCH_RUNNER"
```

Generation creates a persistent DuckDB database, then exports matching
Parquet tables. Decimal storage is explicitly INT64, with no nulls. Native
preparation verifies counts and publishes one snapshot at a time. Finish
all preparation, builds, and tests before starting timed phases.

## Run DuckDB and save the oracle

```bash
systemd-run --user --unit=pgaccel-tpch-sf100-duckdb \
  --property=MemoryMax=192G --property=MemorySwapMax=0 \
  --working-directory="$PWD" --setenv=TZ=UTC --wait --pipe \
  "$TPCH_PYTHON" benchmarks/tpch/compare_scale.py --engine duckdb \
  --database "$TPCH_WORK/duckdb/tpch.duckdb" \
  --temp-directory "$TPCH_WORK/duckdb/benchmark-temp" \
  --queries "$TPCH_QUERIES" --output "$TPCH_RESULTS/duckdb.jsonl"
```

The process exits and releases its buffer pool before the next phase.

## Run PGAccel against saved answers

```bash
systemd-run --user --unit=pgaccel-tpch-sf100-pgaccel \
  --property=MemoryMax=192G --property=MemorySwapMax=0 \
  --working-directory="$PWD" --setenv=TZ=UTC --wait --pipe \
  "$TPCH_PYTHON" benchmarks/tpch/compare_scale.py --engine pgaccel \
  --runner "$TPCH_RUNNER" --native "$TPCH_WORK/native" \
  --queries "$TPCH_QUERIES" --oracle "$TPCH_RESULTS/duckdb.jsonl" \
  --output "$TPCH_RESULTS/pgaccel.jsonl"
```

## Prepare and run the isolated ClickHouse phase

Configuration may be prepared earlier; load MergeTree tables and collect
column statistics before this phase. Ports 18123 and 19000 must be free. A separate root
and unit preserve the existing server on 8123/9000.

```bash
"$TPCH_PYTHON" benchmarks/tpch/configure_clickhouse.py \
  --root "$TPCH_WORK/clickhouse"

systemd-run --user --unit=pgaccel-tpch-sf100-clickhouse \
  --property=MemoryMax=192G --property=MemorySwapMax=0 --setenv=TZ=UTC \
  /usr/bin/clickhouse-server \
  --config-file="$TPCH_WORK/clickhouse/config.xml" \
  --pid-file="$TPCH_WORK/clickhouse/server.pid"

curl -fsS http://127.0.0.1:18123/ping

"$TPCH_PYTHON" benchmarks/tpch/load_clickhouse.py \
  --data "$TPCH_WORK/data" --database tpch_sf100_mergetree --engine MergeTree --statistics \
  --port 19000 --report "$TPCH_RESULTS/clickhouse-load.json"

"$TPCH_PYTHON" benchmarks/tpch/adapt_clickhouse_queries.py \
  --source "$TPCH_QUERIES" \
  --output "$TPCH_WORK/clickhouse-query-overrides"

"$TPCH_PYTHON" benchmarks/tpch/prepare_clickhouse.py \
  --database tpch_sf100_mergetree --queries "$TPCH_QUERIES" \
  --report "$TPCH_RESULTS/clickhouse-mergetree-layout.json"

"$TPCH_PYTHON" benchmarks/tpch/compare_scale.py --engine clickhouse \
  --database tpch_sf100_mergetree --queries "$TPCH_QUERIES" \
  --clickhouse-query-overrides "$TPCH_WORK/clickhouse-query-overrides" \
  --server-pid-file "$TPCH_WORK/clickhouse/server.pid" \
  --oracle "$TPCH_RESULTS/duckdb.jsonl" \
  --output "$TPCH_RESULTS/clickhouse.jsonl"
```

The server's systemd unit enforces the cap; the small HTTP driver need not
run in that unit. Archive the load report, server configuration, and
results before stopping the dedicated instance:

```bash
systemctl --user stop pgaccel-tpch-sf100-clickhouse.service
```

The MergeTree files persist when the server stops. Parquet, DuckDB, and
PGAccel native snapshots also remain available.

## Render the complete report

```bash
"$TPCH_PYTHON" benchmarks/tpch/report_scale.py \
  --duckdb "$TPCH_RESULTS/duckdb.jsonl" \
  --pgaccel "$TPCH_RESULTS/pgaccel.jsonl" \
  --clickhouse "$TPCH_RESULTS/clickhouse.jsonl" \
  --output "$TPCH_RESULTS/RESULTS.md"
```

The output includes every query and a compact checkpoint with raw hashes,
settings, trial times, and memory peaks. Read PLAN.md for timing boundaries,
NULL semantics, and the distinction between complete and failed phases.

## Separate Q20 capacity check

The 64 GiB PGAccel phase rejected Q20 at its query budget while the service
remained below 192 GiB. After the complete three-engine baseline and the
isolated ClickHouse shutdown, rerun Q20 in a fresh PGAccel service with an
80 GiB query allowance. Preserve this as a separate checkpoint; do not
replace the baseline failure or silently include it in the baseline total.

```bash
systemd-run --user --unit=pgaccel-tpch-sf100-q20-capacity \
  --property=MemoryMax=192G --property=MemorySwapMax=0 \
  --working-directory="$PWD" --setenv=TZ=UTC --wait --pipe \
  "$TPCH_PYTHON" benchmarks/tpch/compare_scale.py --engine pgaccel \
  --runner "$TPCH_RUNNER" --native "$TPCH_WORK/native" \
  --queries "$TPCH_QUERIES" --oracle "$TPCH_RESULTS/duckdb.jsonl" \
  --only 20 --repeats 3 --query-memory-gib 80 \
  --output "$TPCH_RESULTS/q20-capacity-80g.jsonl"
```
