# Q9 native Python confirmation

The same frozen module at runtime checkpoint `c2321f0` is compared against
fresh DuckDB and ClickHouse runs. The acceptance target is 2x at both scales.
Times are warm-median complete client calls, excluding only trial one.

| Scale | PGAccel native ms | DuckDB ms | ClickHouse ms | DuckDB / PG | CH / PG | 2x gate |
|---|---:|---:|---:|---:|---:|---|
| SF10 | 222.066 | 662.206 | 897.094 | 2.98x | 4.04x | Pass |
| SF100 | 3011.559 | 6203.938 | 8848.011 | 2.06x | 2.94x | Pass |

All 15 SF10 and nine SF100 trials pass answer validation: exact integer,
decimal, string and date values; floating values use the existing tolerance
of max(1e-6 absolute, 1e-12 relative). Engines run sequentially with eight
workers, and builds/tests do not overlap timed phases. SF100 services are
capped at 192 GiB with swap disabled; PGAccel uses a 64 GiB query budget.
Native timings include parsing, execution, result conversion and cleanup.

SF100 peak query allocation across trials: 6,541,312,242 bytes. Resource samples
and phase commands are retained in `q09-confirm1-sf100-resources.jsonl` and
`q09-confirm1-sf100-phases.json`. No optional index or result cache is used.

All 22 SF1 queries match the saved answers on the same module. The 244-test
native suite and 10 focused ASan/UBSan tests pass. See [design](Q9-DESIGN.md)
for applicability checks and regression coverage.

Module SHA256: `74258e2cf1fe79ef3d3a78fbb4ff00fafa7b983b285031636d308cd91a42e5ab`.

These are individual-query confirmations, not a current all-query sweep.
[Complete trial diagnostics](Q9.json) · [Active targets](ALL-QUERY-GOAL.md)
