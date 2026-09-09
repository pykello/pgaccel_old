# Remaining gap: query preparation and input volume

The [latest full sweep](TRANSITION-STRINGS.md) proves all 22 prepared-input
warm medians meet the saved pgaccel times in one 8 GiB database. It does not
prove those latencies when new SQL requires reading and preparing its input.
All 198 answers and the total-memory measurements are preserved.

The table below divides each first call's recorded preparation read bytes by
**4,000,000,000 bytes/second**, using the user's stated drive bandwidth as a
scenario. This is not a measured hardware ceiling. It assumes that rate is
the aggregate sustained limit and ignores decoding/execution costs. Reported
bytes omit some schema work, so this is not device-wide traffic accounting.
At that assumed rate, moving the recorded bytes alone already exceeds the
old resident-query time for these examples.

| Query | Recorded read GiB | Transfer-only s at assumed rate | Saved resident ms | Current first call s |
|---|---:|---:|---:|---:|
| Q1 | 6.898 | 1.852 | 440.185 | 8.455 |
| Q6 | 5.172 | 1.388 | 79.652 | 2.090 |
| Q9 | 13.359 | 3.586 | 2174.809 | 15.964 |
| Q13 | 8.814 | 2.366 | 1093.312 | 12.953 |
| Q21 | 7.711 | 2.070 | 682.195 | 12.362 |

Further overlap and faster decoding can reduce preparation, but the same
read volume cannot meet these times under that bandwidth assumption. The
next work needs to reduce bytes read, safely reuse immutable input across
SQL changes, or use general physical summaries/indexes where their semantics
cover the requested operation. It must not hide query-specific joins or
aggregates outside the measured call.

## Next investigation

Inventory which physical columns each TPC-H query loads, their native sizes,
and whether preparation retains all rows and original values. Distinguish
original columns from predicate representatives, date transforms and compacted
fact rows. Only original, snapshot-identified data is eligible for general
cross-query reuse. Use explicit cache byte admission and release old entries
before allocating replacements; benchmark real SQL transitions within 8 GiB.

In parallel with that design work, identify the largest on-disk inputs where
a compact representation or index can reduce actual transfers. Compare stored
bytes and end-to-end execution, including any decoding and index setup. The
scan must retain direct input and exact validation. Avoid another small loop
optimization until it addresses a measured part of this remaining gap.

For the stated TPC-H goal, completion evidence must cover the required 22
queries, their timing boundaries, correctness and total memory. General
arbitrary-workload admission is a separate product concern; it is not an
additional workload silently added to the benchmark goal. The unresolved
issue here is specifically SQL-dependent preparation excluded by warm reuse.
