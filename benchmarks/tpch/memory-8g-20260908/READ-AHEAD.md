# Bounded direct read-ahead with serial decoding

Commit `44edc07` overlaps one 8 MiB O_DIRECT pread with decoding from the
current 8 MiB buffer. Each DirectInput owns 16 MiB total. The scanner still
loads and decodes columns serially; selectors and transforms stay on the
caller. This avoids the rejected concurrent-decoding design's Q13 regression.

On refill, the reader waits for the pending read. If its aligned offset
matches the demanded block, it swaps the two buffers; otherwise it discards
the prefetched block and reads the demanded offset directly. It then starts
one read of the following block, if inside the snapshot. Seeks inside the
current block keep it; seeks elsewhere are resolved at refill. Range and
short-read checks remain mandatory, with no buffered payload fallback.

The prototype uses std::async with a background pread, not io_uring. There
is at most one pending read per stream, with an additional I/O helper thread
per active stream; the existing execution arena still has eight workers.
Thread creation and stack residency are included in the end-to-end capped
runs. No additional dependency or separately licensed algorithm is added.

Statistics drain pending reads and count their bytes exactly once, even
when speculative. Destruction waits before freeing buffers or closing the
file, including early-stop and error paths. Buffer reporting charges both
8 MiB blocks. The external total-memory cap additionally includes thread
stacks, allocator retention, results, query state and file-cache charge.

All 289 native tests, Python integration and all 22 SF1 answers pass.
Reader tests cover sequential block/tail contents, unaligned seeks,
invalid seeks, repeated counter reads and early destruction with I/O pending.

| Query/build | First call s | Warm median ms | Total peak GiB | Recorded load GiB |
|---|---:|---:|---:|---:|
| Q1 byte-usage | 11.465 | 419.480 | 6.904 | 6.852 |
| Q1 read-ahead | 8.626 | 419.640 | 6.969 | 6.914 |
| Q13 byte-usage | 29.776 | 548.904 | 1.866 | 8.798 |
| Q13 read-ahead | 28.727 | 549.078 | 1.811 | 8.829 |

Each fresh-process run performs nine calls under MemoryMax=8G and
MemorySwapMax=0. All 18 candidate answers pass the typed SF100 oracle.
Controls are the preceding byte-flag runs; each comparison is one run per
build, not a confidence interval. First calls improve about 24.8% for Q1
and 3.5% for Q13. Warm medians are kept as measured without substitution.
Recorded load bytes are the existing query-preparation metric, including
speculative reads drained by the scan; it is not a device-wide traffic counter.

Evidence: [Q1](native-direct-read-ahead-q1-sf100.json),
[Q13](native-direct-read-ahead-q13-sf100.json),
[SF1 gate](auto-projection-sf1-read-ahead-gate.json).

This establishes a benefit from read overlap for Q1. It does not prove
io_uring is necessary, eliminate preparation, or replace the complete
[all-query warm comparison](TRANSITION-CHAINS.md). A later backend can reduce
thread-launch overhead while preserving the same bounded buffer contract.
First-call latency and cross-query reuse remain part of the open 8 GiB goal.

## Transition check

The [Q1 → Q21 → Q1 run](native-transitions-read-ahead-sf100.json) passes all
27 typed answers in one database. Total peak is **6.971 GiB**, with zero
sampled file charge and no memory-limit events under 8 GiB and zero swap.

| Step | Query | First call s | Warm median ms |
|---|---|---:|---:|
| 1 | Q1 | 8.790 | 425.263 |
| 2 | Q21 | 12.805 | 620.792 |
| 3 | Q1 | 9.068 | 419.002 |

Changing SQL still frees and reconstructs statement-specific input. These
timings include that work and do not establish resident-data latency.
