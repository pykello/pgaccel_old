# Findings at the requested stop — 2026-09-09

Work stopped at the user's request. No implementation or benchmark is pending.
Branch: `feature/tpch-coverage-20260905`. Runtime/cache integration was committed
through `a48f24c`; measurement and documentation followed through `e0ec348`.

## What is established

All 22 TPC-H SF100 queries pass in one long-lived database under an enforced
8 GiB total cgroup limit, with swap disabled. The latest default-configuration
sweep runs nine calls per query and validates all 198 answers against typed
DuckDB reference output. Peak total memory is **6.988 GiB**. Sampled file-cache
charge is zero, and no memory-limit or OOM events are recorded.

Every warm median meets the saved pgaccel baseline and is below the saved
ClickHouse and DuckDB times. These peer figures are historical measurements,
not new equivalent cold-input runs. [Full comparison](TRANSITION-CACHE-CONTROL.md).

**The end-to-end same-performance goal remains unproven.** A changed SQL text
releases the previous prepared input and performs projection, filtering and
compaction again. Warm calls reuse this statement-specific input, but reparse
and execute the original SQL; results and aggregates are not cached. First calls
include preparation and client conversion. Their total is **148.678 seconds**,
versus **6.289 seconds** for the sum of warm medians. Do not present warm numbers
as changed-SQL end-to-end times.

| Query | Warm client ms | First call including preparation s |
|---|---:|---:|
| Q1 | 377.521 | 8.466 |
| Q2 | 69.353 | 1.951 |
| Q3 | 227.005 | 7.478 |
| Q4 | 159.279 | 9.943 |
| Q5 | 178.861 | 7.696 |
| Q6 | 5.801 | 2.071 |
| Q7 | 377.239 | 7.816 |
| Q8 | 296.969 | 7.656 |
| Q9 | 1421.228 | 16.691 |
| Q10 | 374.576 | 10.875 |
| Q11 | 19.485 | 2.812 |
| Q12 | 169.928 | 5.696 |
| Q13 | 506.340 | 13.018 |
| Q14 | 98.719 | 2.349 |
| Q15 | 65.555 | 2.657 |
| Q16 | 201.917 | 1.105 |
| Q17 | 208.411 | 4.886 |
| Q18 | 529.843 | 6.978 |
| Q19 | 103.409 | 8.168 |
| Q20 | 127.534 | 5.944 |
| Q21 | 560.546 | 12.380 |
| Q22 | 209.649 | 2.043 |

## Direct input and retained optimizations

Payload input uses O_DIRECT, with explicit alignment/error handling and no
buffered fallback. Each active payload stream has two 8 MiB buffers with one
background pread; headers use 4 KiB. io_uring has not been implemented or
benchmarked. The user's stated 4 GB/s drive throughput is not a measured ceiling
for this reader.

Retained preparation work includes dense-stripe retention, cheaper dictionary
code validation, read-ahead, smaller header buffers, string allocation reuse,
one stream guard per decoded string dictionary, and parallel ordered-literal
matching where applicable. The preceding complete sweep reduced first-call sum
from 168.749 to 146.768 seconds. Later control variance is preserved rather than
replacing individual measurements with favorable samples.
[Preparation evidence](STRING-PREPARATION.md) and
[preceding full sweep](TRANSITION-STRINGS.md).

## Original-block cache: implemented, not enabled by default

The optional cache uses validated snapshot UUID/file identity, byte admission,
LRU eviction and retained ending offsets for variable-length stripes. Sources
are copied before reaching mutable consumer APIs, preserving original strings
across predicate transformations and SQL changes. Legacy snapshots remain
uncached. Its admission charge does not bound query-held data or total RSS.
All 299 native tests and Python integration pass.

A same-build comparison found:

| Measurement | Cache disabled | 512 MiB cache |
|---|---:|---:|
| Correct answers | 198/198 | 198/198 |
| First-call sum, s | 148.678 | 153.500 |
| Warm median sum, s | 6.289 | 6.469 |
| Total peak memory, GiB | 6.988 | 7.385 |
| Direct reads, GiB | 146.878 | 145.996 |

The cache produced 5,341 stripe hits, but saved only 0.60% of reads. Additional
per-column read-ahead on misses offsets part of the savings; mutable-output
copies add work and memory. The two sequential runs establish no benefit that
justifies enabling it. Keep it an opt-in prototype, disabled by default.
[Implementation, full comparison and decision](CACHE-SCANS.md).

## What the input inventory implies

All 22 queries require a union of 31.381 GiB of original native column spans.
The full snapshot is about 62.022 GiB. Seventeen small repeatedly used columns
total 283.619 MiB on disk, but their decoded block charges total 479.175 MiB.
Disk size is not resident size. `o_comment` alone is 7.589 GiB and used only by
Q13; a small dimension cache cannot remove its first-use cost.
[Inventory](INPUT-INVENTORY.md), [admission measurement](COLUMN-ADMISSION.md),
and [read-volume limits](READ-VOLUME-GAP.md).

## Unfinished ideas, not measured improvements

The next investigation was going to examine Q13 string decoding and compact or
searchable comment representations. This inspection made no code changes and
started no new benchmark before the stop request. Avoiding full original string
materialization for proven predicate-only uses might reduce allocation and
copying, but requires preserving sorted/unique dictionary validation, valid
codes, dictionary coverage, padding checks, UTF-8/LIKE semantics and row alignment.
No performance claim or implementation is established for that idea.

Further cache work would first need to address miss read amplification and
mutable-output copying. io_uring remains an option only if measurements show an
I/O submission/queue-depth bottleneck. Neither is necessary to describe the
already verified warm-only milestone, and neither has closed the preparation gap.

All timing evidence, raw-answer hashes and frozen module identity are linked
from the reports above. Do not rerun merely to reconstruct missing historical
memory information or to replace unfavorable samples.
