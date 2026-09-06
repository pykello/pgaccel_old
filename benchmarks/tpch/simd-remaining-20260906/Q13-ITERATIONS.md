# Q13 count-histogram and literal-search iterations

Warm client medians exclude only trial 1. All nine answers in each run
match DuckDB. Interrupted baseline files are excluded.

| Runner | Warm median ms | Warm min ms | Warm max ms | Query peak MiB |
|---|---:|---:|---:|---:|
| 918e691 | 35636.022 | 35479.626 | 35738.670 | 1822.792 |
| f6bb124 without ordered byte search | 2590.224 | 2574.534 | 2615.324 | 45.886 |
| f6bb124 | 203.786 | 200.451 | 208.579 | 45.886 |
| 919c396 | 149.696 | 145.678 | 155.430 | 45.886 |
| 5190383 | 91.527 | 90.392 | 94.993 | 45.886 |

The separate instrumented profile of the ordered-search version measured
warm median 198.348 ms in the scan and 2.915 ms in histogram construction.
Worker medians, excluding the first query, were 165.240 ms filtering and
26.240 ms counting. These are diagnostic phases, not promotion timings.

`919c396` rejects missing literal bytes before validating candidate
encoding. `5190383` compares first/last literal bytes across bounded SIMD
positions, then verifies every survivor. Binary inputs, earliest matches,
unaligned starts and protected-page tails are covered. All 200 native
tests and five focused sanitizer tests pass for the final candidate.

The optional signature extension `d367cfd` passed its tests but was
reverted in `34f00ff`: the TPC-H harness disables those dictionary indexes,
so it cannot accelerate this workload. No performance gain is attributed
to that unexercised path.

For the regex variant, the runner was built from `f6bb124` using
`executor_filter_values.cc` from `d81feb1`, then the working source was
restored. Both variants share the key-verification fix. Binary hashes
and every trial are preserved in [Q13-ITERATIONS.json](Q13-ITERATIONS.json).

[Design, rejected experiment and validation](Q13-DESIGN.md). Fresh
15-trial SF10 and same-binary nine-trial SF100 confirmation remain separate
from these iterations.

A preliminary 13-trial comparison was rejected by the automation because
the protocol requires 15. Its complete evidence is retained separately;
the subsequent 15-trial run is the confirmation. All 22 SF1 answers also
match DuckDB on final frozen runner `5190383`.
