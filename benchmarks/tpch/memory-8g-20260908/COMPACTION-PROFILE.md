# Q1 compaction by column

The temporary [instrumentation](column-compaction-profile.patch) applies to
`8b3e0f8`. Each column task accumulates elapsed wall time in its own slot;
columns run concurrently, so these values overlap and must not be summed
as preparation latency. The profile includes task descheduling and allocator
waits, not just CPU execution. No instrumentation remains in product source.

| Column | Accumulated task time s |
|---|---:|
| l_quantity | 1.701 |
| l_extendedprice | 7.898 |
| l_discount | 3.735 |
| l_tax | 3.804 |
| l_returnflag | 11.625 |
| l_linestatus | 11.587 |
| l_shipdate | 14.539 |

The three dictionary columns dominate. Their compaction implementation asks
for input code width in both row passes and output code width in the second
pass. These virtual methods depend only on dictionary size, which is fixed
throughout each pass. Resolving them once is the next experiment; pruning,
code validation and remapping semantics must remain intact.

The instrumented Q1 passes all nine typed SF100 answers under an enforced
8 GiB total cap with zero swap. [Evidence](native-direct-compact-profile-q1-sf100.json)
records the frozen module hash, first and warm timings, live memory peak and
raw answer location. Its timings do not replace the uninstrumented sweep.

## Retained: resolve input and output code widths once

`a773a5a` caches input width before discovering retained codes, and output
width after building the pruned dictionary. Both copy passes use those fixed
widths. Dictionary ordering and code mapping are unchanged, including the
transition from two-byte input codes to one-byte output codes. Existing tests
cover that transition, invalid selections, dates and large parallel stripes.
All 281 tests, 22 SF1 answers and Python integration pass.

| Query | First call s | Warm median ms | Peak total GiB |
|---|---:|---:|---:|
| Q1 | 18.298 | 391.606 | 6.887 |
| Q12 | 10.158 | 191.784 | 1.287 |

Both queries pass all nine typed SF100 answers under 8 GiB with zero swap.
Q1 first preparation is about 29% faster than the preceding uninstrumented
25.824-second run. Its warm median is slower than that run's 360.103 ms;
retain this observation instead of replacing it with a favorable repeat.
It remains below the original saved 440.185 ms. Q12 also stays below its saved
341.982 ms. This is a preparation change, not a demonstrated warm-kernel gain.

Evidence: [Q1](native-direct-compact-width-q1-sf100.json),
[Q12](native-direct-compact-width-q12-sf100.json),
[SF1 gate](auto-projection-sf1-compact-width-gate.json).
The full workload report still uses the earlier frozen module. After remaining
work, validate all 22 queries and transitions in a single final capped run.

## Rejected: stored-width bounds during numeric compaction

The [experiment](compact-bounds-experiment.patch) keeps min/max accumulators
at the stored signed width and widens them after copying, rather than widening
every selected value. This resembles the retained contiguous native-load
bounds optimization, but compaction gathers selected rows and has different
execution costs. The experiment passes 282 tests, including new selected
signed-extreme and tail cases, all 22 SF1 answers, and Python integration.

[SF100 Q1](native-direct-compact-bounds-q1-sf100.json) passes nine answers under
8 GiB with zero swap, but first-call time increases from 18.298 to 20.319
seconds and warm median from 391.606 to 394.715 ms. There is no measured gain.
The source and experimental test were restored; the complete patch, SF1 gate
and SF100 measurements remain available. Do not repeat this variant merely
because the related contiguous reduction was successful.
