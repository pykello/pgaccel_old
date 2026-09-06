# Q6 filtered arithmetic iterations

Eight workers; warm client medians exclude only trial 1. All nine answers
match the saved DuckDB oracle in each iteration.

| Checkpoint | Warm median ms | Warm min ms | Warm max ms | Query peak MiB |
|---|---:|---:|---:|---:|
| q6-before (`409c4ca`) | 17.502 | 16.938 | 20.680 | 0.003 |
| q6-filtered-v1 (`b12ce48`) | 8.500 | 8.358 | 8.747 | 0.591 |

The baseline interprets the expression separately for each qualifying row.
The new path applies the existing native bitmap filters, gathers selected
rows into blocks of at most 256, evaluates the arithmetic program once
per block, and reduces SIMD lanes. Dense selections use sequential decoding.

For each stripe, raw column bounds prove that every arithmetic intermediate
and every possible reduction subset fits signed 64 bits. Worker totals merge
using checked 128-bit addition. Wide or unsupported expressions retain the
general executor. Empty selections preserve SQL NULL; output limits and
offsets retain ordinary result handling. No persistent precomputation is used.

All 176 native tests and six focused ASan/UBSan tests pass. Tests cover
decimal scale alignment, signed physical widths, sparse/dense selections,
partial blocks, empty sums, cancellation with wide subsets and memory caps.
This original implementation reuses project helpers without new dependencies.

The fresh SF10 confirmation passes at 8.584 ms against DuckDB 53.349 ms
and ClickHouse 199.319 ms. SF100 also passes at 79.023 ms versus DuckDB 507.214 ms and ClickHouse
1802.468 ms; [full confirmation](Q6.md).

[All-query goal](ALL-QUERY-GOAL.md)
