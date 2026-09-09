# SF100 under 8 GiB: direct-I/O working-set experiments

The target is the previous complete TPC-H SF100 performance with an 8 GiB total memory limit and no payload page-cache dependency. **The target is not achieved.** This checkpoint establishes direct input, streaming buffers, and the benefit and limits of projected residency.

## Latest: all 22 queries pass in one 8 GiB database

[TRANSITION-READ-AHEAD.md](TRANSITION-READ-AHEAD.md) is the latest complete
comparison against saved pgaccel, ClickHouse and DuckDB times. It uses one
frozen module and one long-lived database, Q1 through Q22, nine calls each.
All **198 answers pass** at **6.968 GiB** peak total memory, with zero sampled
file-cache charge, zero swap and no memory-limit events. Every warm median
is below its saved pgaccel, ClickHouse and DuckDB time.

First calls take **1.191–28.888 seconds** and include SQL-specific preparation.
Their sum falls from 256.221 to 168.749 seconds versus the preceding complete
chain-domain sweep, combining dense retention, byte dictionary flags and
read-ahead. This is not an isolated I/O comparison. Warm reuse is not
equivalent to executing changing SQL at the old fully resident latency.
Internal admission for arbitrary workloads is also unfinished.

The reader now overlaps O_DIRECT reads using two bounded 8 MiB buffers per
stream while decoding remains serial. It uses a background pread; io_uring
has not been added. All 289 native tests, 22 SF1 answers and Python integration
pass for this module. [Next steps](NEXT-STEPS.md) track the remaining gap.

The earlier [chain-domain sweep](TRANSITION-CHAINS.md),
[1 MiB reader sweep](TRANSITION-SWEEP.md),
[8 MiB sweep](TRANSITION-NARROW.md),
[dense-group sweep](TRANSITION-DENSE.md),
[preparation sweep](TRANSITION-COMPACTION.md),
[HAVING sweep](TRANSITION-HAVING.md) and
[run-reduction sweep](TRANSITION-RUNS.md) remain separate evidence. No faster
individual measurement replaces a slower value in a complete sweep.

## Latest follow-up: small direct header buffers

[Header validation](SMALL-HEADERS.md) now uses 4 KiB with no read-ahead.
The matching Q2 control improves from 1.783 to 1.475 seconds; Q13 improves
from 28.727 to 27.682 seconds. Q1 is slightly slower in its recorded sample.
All 291 tests and all 22 SF1 checks pass. The complete comparison above
predates this follow-up and remains unchanged.

## Preparation follow-up: retain dense stripes

[Dense retention](DENSE-RETENTION.md) reduces Q1 first-call time from 20.505
to 12.444 seconds. Warm time rises from 358.502 to 407.976 ms, still below
the saved baseline, with 6.904 GiB peak. All 287 tests and SF1 checks pass.
This follow-up is separate from the completed all-query warm milestone.

## Preparation follow-up: dictionary validation

[Byte usage flags](NATIVE-VALIDATION.md) reduce first-call time from 12.444
to 11.465 seconds for Q1 and from 30.724 to 29.776 seconds for Q13.
The candidate peaks at 6.904 and 1.866 GiB respectively. All 288 native
tests, Python integration, 22 SF1 answers and the SF100 checks pass.
Warm medians and controls are preserved in the report; this does not
replace the complete warm sweep or establish changing-query latency.

## Latest preparation follow-up: bounded read-ahead

[Serial decoding with direct read-ahead](READ-AHEAD.md) reduces Q1 first-call
time from 11.465 to 8.626 seconds and Q13 from 29.776 to 28.727 seconds.
All 289 native tests and all 22 SF1 answers pass. A Q1 → Q21 → Q1 transition
check passes all 27 answers at 6.971 GiB peak with zero sampled file charge.
The reader owns two 8 MiB buffers per stream, with one background pread.

## Rejected follow-up: concurrent column loading

[The experiment](PARALLEL-LOAD-EXPERIMENT.md) passes correctness and memory
checks but increases Q13 first-call latency from 29.776 to 76.008 seconds.
It was reverted; the implementation patch and slower measurements are saved.

## Q11 preparation: shared scalar domain proof

[Shared-domain preparation](SHARED-DOMAIN-PREPARATION.md) now proves when
uncorrelated scalar scopes require the same inner-join rows. Bounded
different-key propagation now carries those domains through dimensions.
Q11 runs in 8.844 ms warm at 0.098 GiB total peak; Q3 and Q5 also pass under
8 GiB. All 286 tests, SF1 answers and Python integration pass. This follow-up
is included in the complete comparison above.

## Q17 follow-up: two-level bitmap probing

[Q17 and Q18 profiling](Q17-Q18-PROFILE.md) identifies their dominant scans.
A budgeted summary of nonempty membership words reduces Q17 warm time from
275.683 to 209.781 ms in paired SF100 runs. Both pass nine answers under
8 GiB; all 283 tests, 22 SF1 answers and Python integration pass. This
change is included in the complete comparison above. Subsequent
contiguous-run reduction improves Q18 from 751.764 to 581.259 ms, and Q11
passes at 27.782 ms. All 284 tests and SF1 checks pass for that final module.

## Q11 follow-up: bounded HAVING pruning

[Adaptive HAVING pruning](HAVING-PRUNING-PLAN.md) reduces Q11 query-state peak
from 49.4 to 1.68 MiB by proving that completed groups cannot pass the final
threshold. The paired SF100 warm result improves modestly from 28.571 to
28.181 ms. All 282 tests, 22 SF1 answers and Python integration pass. This
change is included in the complete comparison above. Its remaining Q11
warm target is now met after shared-domain preparation; earlier regressions
remain in their original reports.

## Preparation profiling and parallel compaction

[PREPARATION-PROFILE.md](PREPARATION-PROFILE.md) measures direct-read wall time
and preparation phases. Q1 spends about 20 seconds compacting columns; Q13
has a different loading/transformation bottleneck. Parallel column copies
reduce Q1 first-call time modestly to 25.615 seconds at 6.869 GiB peak in an
individual run. All 281 tests, 22 SF1 answers and Python integration pass.
Direct dictionary construction subsequently reduces Q13 first-call time
from 35.907 to 30.708 seconds, with nine checked SF100 answers under 8 GiB.
[Per-column compaction profiling](COMPACTION-PROFILE.md) then identifies repeated
virtual code-width lookups. Resolving those once reduces Q1 first-call time
to 18.298 seconds at 6.887 GiB peak; its 391.606 ms warm result is preserved.
These retained changes are included in the latest complete comparison above.

## Nested date filters reduce Q15 and Q20 residency

`4cac40f` intersects necessary literal filters across every physical-table use.
A single-table scan inside a CTE or subquery can discard failing rows only if
all other uses require the same filter. Unfiltered aliases or multi-table uses
disable this rule. The original query, including correlation and aggregation,
still runs on the retained rows.

| Query | Previous pgaccel ms | Warm client ms | First call s | Peak cgroup GiB |
|---|---:|---:|---:|---:|
| Q15 | 146.978 | 66.276 | 12.305 | 0.479 |
| Q20 | 211.185 | 135.243 | 20.539 | 2.002 |

Both queries pass all nine SF100 answers in fresh 8 GiB zero-swap processes.
The change passes 275 native tests, all 22 SF1 answers and Python integration.
Eight warm medians in that individual-run comparison exceed their saved baseline. The full
objective remains open; first-call preparation is not a warm scan benchmark.

## Full sweep, follow-ups and representative transitions

[TRANSITIONS.md](TRANSITIONS.md) adds a five-step test in one long-lived database.
All 45 answers pass before and after returning unused allocator pages on SQL
changes. Q9's ending residency falls from 7.88 to 2.79 GiB; reloads can take
longer. The new run records no file-cache charge or memory-limit events.
The complete transition sweep linked above supersedes this smaller coverage test.

[FULL-SWEEP.md](FULL-SWEEP.md) compares all 22 queries with saved ClickHouse,
DuckDB and previous pgaccel times. One frozen build passed 21 queries and was
OOM-killed on Q19. A separately identified fix (`683ad20`) made Q19 pass at
96.720 ms warm and 2.057 GiB peak, with a 17.272-second first call. It extracts
literal filters required by every OR branch before retaining fact rows. The
original SQL still checks the complete predicate. All 274 native tests, all 22
SF1 answers and Python integration pass after this fix.

These results do not constitute an all-query success on one final build.
The updated table includes the separately identified Q15 and Q20 follow-ups.
First preparation remains substantial and a full query-transition sweep is
a separate measurement reported above.
The report preserves the failed run and distinguishes Q19's follow-up build.

## Earlier checkpoint: inner-join membership brings Q5, Q8 and Q9 under the cap

`d07628f` exposes validated snapshot stripe counts during binding. `cdd4cec`
uses them to choose the largest SUM input, loads its dimensions first, and builds
integer-key membership from their literal filters. Equality chains carry these
necessary conditions through intermediate joins. Fact rows that cannot match
are discarded before their payloads are retained. Dimensions keep every row and
key sequence; the original executor still expands duplicate matches and checks
all predicates. Outer/existence joins, child scopes and repeated candidate-table
aliases decline this preparation step. Bitmap payloads have a 256 MiB allowance;
sparse extreme key domains skip optional pruning rather than allocating an
unbounded dense index.

| Query | Previous client ms | Warm client ms | First call s | Peak cgroup GiB |
|---|---:|---:|---:|---:|
| Q5 | 374.163 | 306.261 | 19.577 | 2.960 |
| Q8 | 658.360 | 398.008 | 22.200 | 2.195 |
| Q9 | 2174.809 | 1592.271 | 31.328 | 2.766 |

All 27 answers pass their typed oracle. Each query uses a separate 8 GiB,
zero-swap cgroup with a 512 MiB executor allowance. First-call reading, membership
construction and compaction are separate from warm execution. All 273 native
tests, 22 SF1 answers and Python integration pass. Tests include transitive keys,
duplicate dimension matches, empty key sets, outer-join exclusion and signed
extreme domains. Source snapshots and the SQL statement remain unchanged.

Every SF100 query now has an individual capped success across these checkpoints.
A complete fixed-build sweep is reported in [FULL-SWEEP.md](FULL-SWEEP.md), with
saved peer comparisons and current regressions. Neither that sweep nor the
individual results demonstrate a bounded cache across transitions between
queries. Query-specific preparation remains substantial, and matching every
recorded warm time is still unfinished.

## Earlier checkpoint: paired date predicates bring Q21 under the cap

`9fae20f` extends statement-specific predicate storage to date pairs. It checks
all scopes before compressing columns used only in the same comparison between
two dates in one source row. True and false outcomes retain representative pairs
of actual dates, encoded with one-byte local codes. Both columns use the same
outcome and row selection. Outputs, literal constraints, competing comparisons
and cross-alias comparisons disable the transformation. All row and join-key
sequences remain intact, including Q21's three uses of lineitem.

| Query | Previous client ms | Warm client ms | First call s | Peak cgroup GiB |
|---|---:|---:|---:|---:|
| Q21 | 682.195 | 682.736 | 19.447 | 6.844 |
| Q4 | 209.989 | 182.054 | 15.676 | 4.535 |

Both runs pass all nine typed-oracle checks and verify the live 8 GiB total cap
with zero swap. The query allowance is 768 MiB: Q21's recorded query-state peak
was slightly above 512 MiB. This does not raise the total limit. Q21's warm median
is effectively unchanged rather than a demonstrated speedup; Q4 is faster.
First-call direct reading, predicate evaluation and compaction remain visible.
The final suite passes 271 tests, all 22 SF1 answers, and Python integration.

Nineteen SF100 queries have now completed under the cap across these checkpoints.
Q5, Q8 and Q9 still lack successful capped runs. The full objective remains
unfinished: Q20 and other recorded regressions require work, as does bounded
residency across a full sequence of different queries.

The next preparation step should use inner-join membership to filter large fact
inputs. Build eligible-key membership from a dimension's literal filters, retain
the dimension's complete key sequence, and discard fact rows that cannot match.
This may reduce Q5/Q8 lineitem inputs through filtered orders, and Q9 through
filtered part keys. The transformation must preserve duplicate join multiplicity,
limit itself to proved inner-join scopes, and include membership construction in
first-call time and total memory. It has not yet been implemented or measured.

## Earlier checkpoint: predicate-only strings bring Q13 and Q20 under the cap

`9196971` checks every bound use of a physical string column, across aliases,
scalar/membership subqueries and materialized scopes. Compression is allowed
only when every use observes the same literal predicate. Outputs, grouping,
join/correlation keys, computed expressions and different predicates disable it.
During direct preparation each stripe retains at most two actual labels: one
matching and one nonmatching representative. One-byte row codes preserve every
predicate outcome, row count and join-key sequence. The original SQL is still
executed against these statement-specific buffers; source snapshots are unchanged.

For example, with `NOT LIKE '%special%requests%'`, ordinary comments can share
one ordinary representative, while matching comments share a matching one.
Their spelling can be discarded only because no permitted query use observes
it. A later statement that selects the comment reloads the original column.
This preserves Q13's left join without deleting orders or building a result cache.

| Query | Previous client ms | Warm client ms | First call s | Peak cgroup GiB | Result |
|---|---:|---:|---:|---:|---|
| Q13 | 1093.312 | 547.416 | 38.081 | 1.851 | Faster warm; all answers pass |
| Q20 | 211.185 | 241.926 | 21.181 | 7.887 | Memory passes; warm regression |

Both runs verify the live 8 GiB total limit and zero swap, use a 512 MiB query
allowance, and pass all nine typed-oracle comparisons. Warm medians exclude only
the first call; preparation includes reading and evaluating original strings.
The final suite passes 270 native tests, all 22 SF1 answers, and Python integration.
No peer engines were rerun. The measured module is frozen as
`pgaccel_native-predicate-strings.cpython-312-x86_64-linux-gnu.so` in the checkpoint
directory, with its hash in each report.

Eighteen SF100 queries now have a successful memory-capped run across these
checkpoints. Q5, Q8, Q9 and Q21 still lack one. This is memory/correctness coverage,
not achievement of the full performance goal: Q20 is about 15% slower, other
previously reported regressions remain, and a complete query-transition cache
policy has not been demonstrated. Next work must address those four working
sets and the warm regressions while keeping cold preparation visible.

## Earlier checkpoint: Q7 and Q10 fit with faster warm execution

Q7 now compacts its lineitem input even though nation is used under two aliases.
Only repeated uses of the table being compacted prevent this scope-local
selection; other dimensions remain complete (`d0571fd`).

For Q10, the phase probe measured 7,603,412,992 bytes resident in the live cgroup
immediately after preparation. Narrowing each worker's group sums alone was
insufficient: replicated arrays still exhausted the remaining total budget.
`5af58ed` establishes bounds for any positive or negative partial sum, without
relying on cancellation. `f1146b9` uses one shared int64 sum array and a presence
bitmap when that proof succeeds. Workers use relaxed atomic addition/bit updates,
and results are read after all tasks join. Unproved ranges retain separate wide
sums. Tests include signed extremes and simultaneous updates to one group.

| Query | Previous client ms | Warm client ms | First call s | Peak cgroup GiB | Query-state MiB |
|---|---:|---:|---:|---:|---:|
| Q7 | 521.101 | 449.134 | 19.581 | 4.365 | 407.105 |
| Q10 | 1160.066 | 598.865 | 26.026 | 7.596 | 314.472 |

Both native Python runs pass all nine typed-oracle comparisons with an enforced
8 GiB total cap, zero swap and a 512 MiB query allowance. Warm medians exclude
only the first call; preparation is query-specific and explicitly timed. No
peer-engine reruns or successful-sample substitutions were used. Earlier Q7/Q10
failures below are retained historical evidence and are superseded by these
successful runs. The final native suite passes 268 tests and Python integration; all 22 SF1
answers also pass the final shared-state validation gate.

Across these checkpoints, sixteen SF100 queries have a successful capped run.
Q5, Q8, Q9, Q13, Q20 and Q21 still lack one. This is not a new all-query sweep,
a full cache-transition test, or achievement of the stated goal; some existing
warm timings also remain slower than the historical baseline.

Next priorities are filter-only string storage (notably Q13), sparse dimension
representations for the larger joins, and total memory admission across query
transitions. Filter-only dictionary labels may be replaceable by representatives
of identical predicate outcomes, but only after proving that no output, key,
ordering or other scope observes the original string values. This would preserve
row/key sequences while avoiding retention of large comment dictionaries.

## Earlier checkpoint: compacted aggregate inputs

Direct preparation now applies eligible simple filters one stripe at a time,
then retains only qualifying rows. Numeric min/max bounds are recomputed;
local dictionaries are pruned and recoded, including a two-byte to one-byte
transition where possible. All columns use the same ordered selection. No
auxiliary row indexes survive compaction. `c11288e` adds the primitive,
`edea6e8` enables it for direct queries, and `21f4e3b` recognizes fact values
inside CASE expressions separately from dimension conditions.

The policy preserves dimension tables to keep consecutive-key join proofs.
It declines pushdown across child scopes, repeated candidate-table aliases, outer joins and
existence joins. An initial experiment compacting dimensions passed all 22 SF1
answers but sent several joins down much slower paths; that policy was replaced.
Its one-pass correctness evidence is retained, not used as a timing comparison.
The retained policy passes all 22 SF1 answers, all 266 native tests, and Python
integration. The SQL is still reparsed and executed on every call.

These are nine-call native Python experiments with an enforced 8 GiB total cap
and no swap. Warm medians exclude the first call, which now includes filtering
and compaction as well as direct reads. Prepared data are specific to the current
SQL; this is not a cold-query speed claim or a demonstrated whole-workload cache.
All successful trials pass the saved typed oracle.

| Query | Previous client ms | Prepared warm client ms | First call s | Peak cgroup GiB |
|---|---:|---:|---:|---:|
| Q1 | 440.185 | 424.602 | 28.547 | 6.884 |
| Q3 | 430.604 | 395.620 | 17.643 | 5.725 |
| Q6 | 79.652 | 4.512 | 9.340 | 0.157 |
| Q14 | 132.674 | 122.000 | 11.665 | 0.206 |

Q3 adds a successful capped query to the previous thirteen-query coverage.
Q6 and Q14 show substantial residency reductions. Q1's slight timing change
is not a substantial memory improvement: its broad filter retains most rows.
No ClickHouse or DuckDB timings were rerun.

Q7 still hits the total 8 GiB cap. Q10 first fails the 512 MiB executor budget;
its historical query-state peak was 2,236,134,864 bytes. Allowing 3 GiB of query
state under the same total cap then produces an OOM kill. Both failures are
retained. No post-exit tiny systemd peak is treated as a live measurement.

The Q10 follow-up, now implemented above, was to reduce the per-worker grouped SUM arrays in
`executor_chain_sum.cc`: eight 128-bit arrays over a large dimension can consume
roughly two gigabytes. A proved int64 accumulation path may halve that state;
it must establish bounds before execution and retain a wide fallback. Broader
coverage still needs sparse dimension directories, filter-only string disposal,
and a total shared memory admission/cache policy. The all-query target remains
unmet.

## Measured projected working sets

Each query is a separate process with explicit required columns loaded using O_DIRECT. Native version-1 fixtures are unchanged. A systemd cgroup enforces `MemoryMax=8G` and `MemorySwapMax=0`; its limit and `memory.peak` are read inside the process. Nine query trials follow loading. Medians exclude only trial one. All successful trials validate against the saved DuckDB SF100 oracle.

The old numbers are full native Python client medians; new numbers are C++ ExecuteQuery medians and exclude JSON serialization. Both exclude initial loading. These diagnostics are not a same-interface final performance claim. No peer engine was rerun. Current probe query budgets are 256 MiB for Q1/Q6 and 512 MiB for the multi-table sweep; the total process cap is always 8 GiB.

| Query | Previous client ms | Projected executor ms | New/old | Peak cgroup GiB | Direct load s | Status |
|---|---:|---:|---:|---:|---:|---|
| Q1 | 440.185 | 446.483 | 1.014 | 6.872 | 17.026 | Answers pass |
| Q2 | 77.479 | 88.330 | 1.140 | 1.328 | 3.035 | Answers pass |
| Q3 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q4 | 209.989 | 219.451 | 1.045 | 5.884 | 13.720 | Answers pass |
| Q5 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q6 | 79.652 | 80.168 | 1.006 | 5.179 | 12.119 | Answers pass |
| Q7 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q8 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q9 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q10 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q11 | 30.496 | 40.239 | 1.319 | 1.107 | 2.509 | Answers pass |
| Q12 | 341.982 | 345.365 | 1.010 | 7.452 | 17.591 | Answers pass |
| Q13 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q14 | 132.674 | 137.799 | 1.039 | 6.397 | 14.403 | Answers pass |
| Q15 | 146.978 | 151.096 | 1.028 | 6.527 | 15.547 | Answers pass |
| Q16 | 213.195 | 212.720 | 0.998 | 1.196 | 1.970 | Answers pass |
| Q17 | 272.281 | 281.216 | 1.033 | 5.756 | 13.133 | Answers pass |
| Q18 | 733.755 | 807.403 | 1.100 | 6.457 | 14.956 | Answers pass |
| Q19 | 217.095 | 238.193 | 1.097 | 7.450 | 18.529 | Answers pass |
| Q20 | — | — | — | — | — | Not run: oversized projected column inventory |
| Q21 | 682.195 | — | — | 8.0 limit hit | — | OOM killed; retained failure |
| Q22 | 375.958 | 382.107 | 1.016 | 1.185 | 3.003 | Answers pass |

Thirteen queries complete within the cap. Nine measure within 5% of the historical numbers, an observation rather than a revised acceptance gate. Q2/Q11/Q18/Q19 have larger slowdowns. Q21 is killed at the total cgroup limit. No numbers were substituted for failures.

## Native Python comparison with automatic projection

This follow-up uses `Database(..., direct=True)` at `6a2c297`, the same native
Python client boundary as the historical benchmark. The first of nine calls
includes automatic binding and direct preparation. The remaining eight reuse
the projected data and execute again; their median is shown below. Each query
gets a fresh 8 GiB, zero-swap cgroup and a 512 MiB executor budget. All 117
answers pass the typed oracle, and every worker verifies its live kernel limits.
No peer engines were rerun. Historical timing variance remains a limitation.

| Query | Previous client ms | Direct warm client ms | New/old | First call s | Peak cgroup GiB |
|---|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 466.291 | 1.059 | 18.318 | 6.878 |
| Q2 | 77.479 | 88.956 | 1.148 | 3.342 | 1.334 |
| Q4 | 209.989 | 211.104 | 1.005 | 14.131 | 5.890 |
| Q6 | 79.652 | 78.125 | 0.981 | 12.808 | 5.185 |
| Q11 | 30.496 | 43.715 | 1.433 | 2.561 | 1.114 |
| Q12 | 341.982 | 333.421 | 0.975 | 18.099 | 7.458 |
| Q14 | 132.674 | 137.883 | 1.039 | 15.227 | 6.403 |
| Q15 | 146.978 | 153.607 | 1.045 | 15.297 | 6.533 |
| Q16 | 213.195 | 225.458 | 1.058 | 2.325 | 1.223 |
| Q17 | 272.281 | 280.652 | 1.031 | 13.407 | 5.762 |
| Q18 | 733.755 | 756.095 | 1.030 | 15.942 | 6.462 |
| Q19 | 217.095 | 227.017 | 1.046 | 18.949 | 7.456 |
| Q22 | 375.958 | 381.308 | 1.014 | 3.297 | 1.190 |

The previous coverage limits still apply: these are the thirteen queries whose
projected data and execution completed, not all 22 queries or a full workload
cache transition test. Cold calls take seconds and several warm queries regress.
The stated goal remains unmet. `run_native_direct.py` retains commands, module
hashes, all samples, direct read counts and correctness checks. The measured
module is frozen at `~/projects/tpch-work/checkpoints/direct-memory-20260908/pgaccel_native-6a2c297.cpython-312-x86_64-linux-gnu.so`.

## Streaming diagnostic

The synchronous single-reader filtered-SUM diagnostic executes SF100 Q6 in 10.855, 10.843, and 10.943 seconds. It reads 5,498,732,544 payload bytes per run and returns the exact saved answer. A resource-confirmation run takes 10.841 seconds, with cgroup peak 6,860,800 bytes and process high-water 10,208 KiB. The difference includes shared library accounting; these measures are not interchangeable.

This diagnostic includes loading, native validation, and expression evaluation on each scan. It is not the old resident native API. Around seven seconds of user CPU time indicate that asynchronous I/O alone will not close the gap. No io_uring dependency has been introduced.

The systemd terminal summary on this host sometimes reports a misleading tiny memory peak after process exit. The confirmation and resident probes instead read memory.peak and the actual limits inside the live cgroup. Initial terminal summaries remain in raw evidence and are not used as memory measurements.

A subsequent width-specialized min/max validation change (`496451f`) reduces
these scans to 9.083, 9.310 and 9.227 seconds (median 9.227 s, about 15% below the
original 10.855 s median). All three answers are exact, and live cgroup peaks
remain below 6.9 MB. The first trial records 5.03 s user CPU and 1.23 s system
CPU. This improvement is worth retaining, but does not approach warm execution.
See `direct-q6-sf100-vector-bounds.json`; all 263 native tests pass, including
signed extremes, all stored widths, vector tails and invalid bounds.

## What changed

- `e1277fd`: optional version-2 aligned value vectors and padding validation; version-1 writing remains default. The direct backend can also read unchanged version-1 snapshots through aligned bounce buffers.
- `95fa282`: seekable O_DIRECT input with one 1 MiB aligned buffer, explicit short-read/errors, no buffered payload fallback, and I/O counters.
- `840e2a8`: projected direct stripe callbacks, reusing native validation and retaining only one stripe at a time unless the consumer explicitly pins more.
- `3ba3cbe`: projected resident direct loading for the existing executors. This owns the chosen columns; it is not yet an LRU cache or automatic admission policy.

- `6fbe03a`: metadata-only binding and physical column dependency discovery across child scopes, followed by rebinding against projected data.
- `69f703e`: `query_runner direct-dir` exposes automatic projection and preparation counters.
- `6a2c297`: `Database(..., direct=True)` retains one projected working set, reparses and executes every call, and releases columns when SQL changes. This is not a result cache or a shared memory admission policy.

## Next stages

1. **Done:** bind against metadata-only schemas, collect dependencies in every query scope, then load/rebind projected tables. All 22 SF1 queries pass. Both the CLI and native Python API expose opt-in direct projection with preparation visible in timing.
2. Add explicit admission and an 8 GiB shared budget for cached columns, pinned batches, dictionaries, query state, and results. Do not evict buffers referenced by cached group keys, join directories, or result views. A per-query allocation limit alone is insufficient.
3. Make large fact scans consume bounded pinned batches. Keep small dimensions/key proofs resident where useful. Add an indexed chunk directory and predicate-first reads so a scan can avoid unrelated chunks instead of reading all named columns.
4. Replace repeated heap string dictionaries and wide numeric payloads with compact/persisted encodings. Q13 comments and Q9 multi-column working sets cannot meet the target through column projection alone.
5. Measure overlap of direct reads, validation/decode, filtering, and aggregation. Use bounded asynchronous submission, potentially io_uring, only after lifetimes/cancellation and queue memory are explicit. Preserve a synchronous fallback that still uses O_DIRECT.
6. Run a same-interface all-22 SF100 comparison under the cap. Include first query, warm repeats, transitions between queries, load/eviction work, disk bytes, memory peaks, failures, and answer validation. Do not equate individually loaded query subsets with a demonstrated whole-workload cache policy.

## Reproduction and evidence

`audit_columns.py` derives a static once-per-column byte inventory from existing native metadata and SQL identifiers. It includes serialized headers/dictionaries and ignores selectivity and repeated passes. Its 4 GB/s figures are estimates using the supplied bandwidth, not measured drive throughput.

Build probe sources with the same flags/libraries as the engine (C++20, O3, AVX-512F/BW, pthread, no strict aliasing), `-Isrc -Ibenchmarks`, `build/libpgaccel_static.a`, and `-lre2 -ltbb -larrow -lparquet`. The runner scripts retain exact commands, binary hashes, all timing samples, and correctness outcomes. Frozen diagnostic executables are under `~/projects/tpch-work/checkpoints/direct-memory-20260908/`.

Complete response rows are under `~/projects/docs/tpch/memory-8g-20260908/raw/`. Compact records beside this report preserve raw-file paths/hashes and per-trial answer hashes/counts. No benchmarks were rerun to compact evidence.

Native verification: all 260 tests passed after the streaming change; the added resident-projection test and all five direct/aligned tests also passed. The complete suite passes all 262 tests after dependency binding. All 22 SF1 queries pass through automatic projection; Python integration also passes with direct reload/reuse, failed preparation, and owned result lifetime.

Linux documents the direct-I/O alignment/filesystem requirements in [open(2)](https://man7.org/linux/man-pages/man2/open.2.html). The [io_uring interface](https://man7.org/linux/man-pages/man7/io_uring.7.html) supports asynchronous submission/completion; it does not replace ownership or memory budgeting. Metadata reads remain small ordinary reads; native payloads use O_DIRECT. Snapshots must remain immutable while any reader uses them.

## Memory work after projection

The next useful executor change is predicate-first materialization. For an
inner-join query, a base-table filter can discard rows before retaining payload
columns. A compacted stripe must preserve row correspondence across all retained
columns, rebuild local dictionary codes and bounds, and invalidate auxiliary row
directories. Applying a filter in one scope must not remove rows needed by a
second alias or a child query. Left joins, antijoins and shared-table subqueries
require separate scope-specific selections; conservatively declining pushdown is
necessary until those semantics are represented explicitly.

For filter-only strings, retain a predicate bitmap after evaluating the filter,
then discard the string payload if no other scope or output needs it. Q13's
comment filter is a useful target, but it needs a bound predicate replacement;
simply deleting the column would make later binding or evaluation incorrect.
For numeric joins, pin the compact dimension keys and process fact stripes with
bounded row-reference lifetimes. Existing directories contain raw column
pointers and cannot survive eviction without an explicit ownership change.

Direct I/O is necessary for honest file-cache accounting here, but does not make
the full fact input resident in 8 GiB. Even ideal 4 GB/s reads take roughly 1.37
seconds for Q6's measured 5.50 GB scan, before decoding and execution. Matching an
80 ms warm query requires retaining a suitable representation, avoiding most
reads through indexing/pruning, or both. Asynchronous reads should be measured
after reducing validation overhead and fixing batch ownership, with a fixed
queue and buffer budget.
