#!/usr/bin/env bash
set -euo pipefail
build_dir=${PGACCEL_BUILD_DIR:-build}
python_bin=${PGACCEL_PYTHON:-python3}
cmake -S . -B "$build_dir" -G Ninja
cmake --build "$build_dir" --target pgaccel run_tests query_runner -j 8
ctest --test-dir "$build_dir" --output-on-failure
"$python_bin" tests/audit_gate_test.py
"$python_bin" tests/parquet_schema_test.py "$build_dir/pgaccel"
if [[ ${1:-} == --full ]]; then
    : "${PGACCEL_FIXTURES:?Set the directory containing the full audit fixtures}"
    output_dir=${PGACCEL_CI_OUTPUT:-build/ci-results}
    mkdir -p "$output_dir"
    "$python_bin" benchmarks/clickbench/audit.py \
        --runner "$build_dir/query_runner" \
        --native "$PGACCEL_FIXTURES/hits.pgaccel" \
        --queries benchmarks/clickbench/queries.sql \
        --oracle "$PGACCEL_FIXTURES/clickhouse.jsonl" \
        --output "$output_dir/audit.jsonl"
    "$python_bin" benchmarks/clickbench/gate.py \
        --results "$output_dir/audit.jsonl" \
        --wide-oracle "$PGACCEL_FIXTURES/wide-average.json" \
        --output "$output_dir/gate.json"
fi
