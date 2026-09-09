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
