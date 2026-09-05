# Date execution and ClickBench stages 0–3

Build with CMake, Arrow/Parquet, TBB, readline, PAPI, and an AVX-512-capable
CPU. C++20 is required. The ordinary build runs local GoogleTests; optional
Touca service tests require `-DPGACCEL_BUILD_TOUCA_TESTS=ON`.

```sh
cmake -S . -B build -G Ninja
cmake --build build -j 8
ctest --test-dir build --output-on-failure
# Optional integration tests, with pyarrow installed:
python tests/parquet_schema_test.py build/pgaccel
```

The branch supports typed numeric expressions; COUNT, SUM, AVG, MIN, MAX;
row projection; one variable grouping key plus constants; output aliases;
ORDER BY selected expressions or aliases; LIMIT; and exact COUNT(DISTINCT).
Multiple variable grouping keys, joins, OR, LIKE, general SQL functions,
OFFSET, and HAVING remain unsupported. Unsupported SQL returns an error.

Numeric SUM/AVG use checked 128-bit accumulators. SUM retains decimal scale;
AVG formats the final wide sum divided by the count. Arithmetic division
uses a fixed decimal scale of at least six digits and truncates excess
fractional digits. Empty SUM/AVG/MIN/MAX return NULL; empty counts return zero.
Input NULLs are rejected rather than silently removed. String comparison uses
byte ordering. Dates use civil calendar days; timestamps use UTC epoch seconds.
Subsecond timestamps are rejected.

## Load ClickBench

Use the unchanged, full `hits.parquet` and the declared schema in this directory.
The upstream Date column has an integer Parquet annotation and its timestamp
columns have unannotated INT64 values. The declaration is therefore required.
The schema maps CHAR/VARCHAR input values to text and widens SMALLINT storage
to logical Int32 while checking its declared range.

```text
load_parquet hits /absolute/path/hits.parquet * benchmarks/clickbench/hits.schema;
save hits /absolute/path/hits.pgaccel;
set memory_mb 32768;
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
```

The loader validates physical compatibility, timestamp units, missing values,
and declared small-integer ranges. Eight import workers preserve source row-group
order. Native loading validates column lengths, layouts, dictionary codes, and
pruning bounds. The native format remains platform-dependent and unversioned.

The default query budget is 8 GiB. The full-data audit explicitly uses 32 GiB.
The budget conservatively accounts for query-owned groups, exact sets, scratch,
and output; it excludes the resident input table and allocator/runtime overhead.
It is not an operating-system RSS limit. There is no spill-to-disk fallback.
Budget exhaustion returns an error and never substitutes an approximate count.

## Reproduce the audit

The pinned upstream SQL, original measurements, new raw results, and detailed
report live in `../docs/clickbench/` relative to the repository root. The input
contains 99,997,497 rows and 105 columns. Its SHA-256 is
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.

```sh
PGACCEL_QUERY_MEMORY_BYTES=34359738368 build/query_runner \
  native /absolute/path/hits.pgaccel hits - 1 1 3 \
  < ../docs/clickbench/upstream/clickhouse-queries.sql > pg-results.jsonl
python benchmarks/clickbench/compare.py \
  --queries ../docs/clickbench/upstream/clickhouse-queries.sql \
  --output ch-results.jsonl --database clickbench_audit --threads 8
python benchmarks/clickbench/audit.py --runner build/query_runner \
  --native /absolute/path/hits.pgaccel \
  --queries ../docs/clickbench/upstream/clickhouse-queries.sql \
  --oracle ch-results.jsonl --output coverage.jsonl
```

`query_runner` reports loading separately, then executes every statement without
rewriting SQL. Its parallel flag selects eight workers; zero selects one worker.
`audit.py` applies a per-query deadline and reports parse failure, execution
failure, timeout, answer mismatch, unverified result, or success. Ordered
results compare in order; differing valid ties require separate review.

Q4 needs explicit interpretation: the installed ClickHouse 26.8.2.7 returns
`-55945124888.916016` for unchanged `AVG(UserID)`. PgAccel returns approximately
`2.528953029789716e18`, matching DuckDB and ClickHouse with an explicit Int128
input. The exact sum is `252888973009538030487413479`. The audit deliberately
reports the unchanged-query mismatch; it does not hide this numeric-policy
difference with a tolerance or silently rewrite the benchmark.

## Date performance

`tpch/date-queries.sql` includes selective and broad date grouping, complete
GROUP BY date, an empty date range, and the TPC-H-derived discount/date count.
Compare full answers with DuckDB and both ClickHouse Memory and MergeTree tables
using `tpch/compare_dates.py`. The detailed SF10 before/after results are in
`../docs/tpch/results/date-stage3/` and the accompanying report.

Filtering now checks both range endpoints before narrowing physical literals.
Date grouping uses selected bitmap bits, compact dense count/sum arrays, and
only active dictionary labels. High-cardinality grouping and distinct use
bounded hash state; their merge and allocation costs still need optimization.
