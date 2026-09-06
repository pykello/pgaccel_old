# Q16: native Python confirmation

The optional native Python API at `90e5747` passes both scales with the
same frozen module. The CLI attempts remain failed gates in
[Q16-CONFIRMATIONS.md](Q16-CONFIRMATIONS.md). These interfaces are reported
separately; this result does not revise any CLI timing.

| Scale | PGAccel native ms | DuckDB ms | ClickHouse ms | DuckDB / PGAccel | CH / PGAccel |
|---|---:|---:|---:|---:|---:|
| SF10 | 37.607 | 185.473 | 338.014 | 4.93x | 8.99x |
| SF100 | 227.373 | 964.195 | 1360.260 | 4.24x | 5.98x |

All 15 SF10 and nine SF100 trials match the saved DuckDB oracle. Medians
exclude only trial one. Eight workers, sequential engines, 64 GiB PGAccel
query allocation limit, 512 GiB spill cap, and 192 GiB SF100 process limits
with zero swap apply. The complete native `execute()` call is timed, including
parsing, output conversion to Python lists/strings, and native result cleanup.
DuckDB uses its Python API and ClickHouse uses HTTP. Engine-only timing is
diagnostic. No query-result cache or optional index build is enabled.

All 22 SF1 answers match through this module in
`pgaccel-native-q16-v1-sf1.jsonl`. Native integration tests cover Unicode and
escaped strings, aliases, parse errors, concurrent calls, and close lifetime.

Module SHA256: `04c28293f6fdf20671cfba14d0a2f4f4352b6a6a9dc3e4780fc354d7ef840013`.

This is the fifteenth individual query confirmation. Earlier confirmations
use CLI transport and different runtime checkpoints. A final all22 sweep must
use one frozen module and one consistent interface at both scales.

## Reproduction and implementation

See the repository `python/README.md` for installation and API instructions.
The harness adds `--engine pgaccel-native --module /path/to/pgaccel_native.so`.
The SF100 Python process itself is capped by systemd. Native cancellation is
not exposed; the comparison driver runs under an external process deadline.

The native module uses permissively licensed
[pybind11 3.0.1](https://github.com/pybind/pybind11/blob/v3.0.1/LICENSE).
No third-party join implementation was copied. Query optimization details are
in [Q16-DESIGN.md](Q16-DESIGN.md), with rejected and retained experiments in
[Q16-ITERATIONS.md](Q16-ITERATIONS.md).

[Compact trial evidence](Q16-NATIVE.json) · [Remaining work](ALL-QUERY-GOAL.md)
