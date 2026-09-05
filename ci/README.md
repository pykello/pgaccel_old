# Correctness jobs

The workflow uses provisioned AVX-512 runners with the project's build
dependencies. `pgaccel-avx512` runs unit, audit-policy, and generated-Parquet
integration tests. Set `PGACCEL_PYTHON` to a Python executable with pyarrow.

The separate `pgaccel-clickbench` job is enabled when the repository variable
`PGACCEL_FIXTURES` names a fixture directory containing:

- `hits.pgaccel` and `hits.pgaccel.metadata`: full 99,997,497-row snapshot;
- `clickhouse.jsonl`: independent oracle results for the supported queries;
- `wide-average.json`: independent `count` and exact `sum_userid` for Q4.

The pinned SQL and required coverage live in the repository. Missing outcomes,
regressions, wrong answers, and unexpected unverified coverage fail the gate.
Q4 only passes the explicit wide policy when its value agrees with the exact
sum divided by the count. Its unchanged ClickHouse mismatch stays visible.

Run the same jobs locally with `bash ci/check.sh` or `bash ci/check.sh --full`.
The full audit has per-query deadlines and retains raw outcomes as artifacts.
Configuring this workflow does not provision or register GitHub runners.

New snapshots have version and ABI declarations plus matching identities in
the data and metadata files. Readers reject unsupported versions and mismatched
pairs, and still read legacy snapshots. Version 1 payloads remain native-endian;
the header detects incompatibility rather than promising cross-platform input.
The identity is a pairing guard, not a content checksum or atomic publication.

The default full-query gate now requires the 31-query stage-4 subset. Its
ClickHouse fixture must include those queries. Supply `refinements.jsonl`
from `benchmarks/clickbench/validate_ties.py` for tied LIMIT outputs; the gate
checks the original SQL, refinement, and exact typed rows before accepting
SQL-equivalent alternatives. Keep unchanged-query mismatches in the raw audit.

String predicates also require RE2 (`libre2-dev` on Ubuntu) and pkg-config.
