# Recovering warm performance under 8 GiB

These are individual SF100 follow-ups, each with nine typed-oracle-checked
answers in a fresh 8 GiB zero-swap cgroup. Warm medians exclude first
preparation. Executor worker counts remain unchanged. The complete transition
comparison is a separate measurement; these rows do not substitute for it.

| Query / build | Warm client ms | Query-state peak MiB | Decision |
|---|---:|---:|---|
| Q2, 8 MiB reader | 82.755 | 18.882 | Baseline for this change |
| Q2, shared minimum domains | 71.248 | 18.763 | Keep |
| Q11, 8 MiB reader | 34.188 | 76.618 | Baseline for these changes |
| Q11, 256-group reservation | 33.547 | 76.618 | Superseded |
| Q11, selected-count reservation | 31.019 | 49.522 | Keep |
| Q11, narrow-key SIMD membership | 28.668 | 49.411 | Keep |

The saved original pgaccel medians are 77.479 ms for Q2 and 30.496 ms for
Q11. Query-state peaks exclude retained input columns; total cgroup limits
are independently enforced and recorded.

## Q2: share a proved equivalent dimension domain

`b66f773` changes `src/executor_correlated_min.cc`. The fused correlated-MIN
executor previously built the supplier/nation/region key domains separately
for the parent and scalar child. It now compares the remapped dimension filters
by column, operator and literal values. Exact matches share one set of domains.
Different filters still build independent sets. Fact predicates are independent
even when dimensions match. The original minimum reduction and projection keep
all equal-cost ties and duplicate fact rows.

Existing tests cover matching and differing regions, different child fact
filters, absent keys, duplicate dimensions, signed values and scalar/SIMD modes.
All 275 native tests and Python integration passed at this checkpoint. The
subsequent all-query SF1 gates also include this change.

## Q11: reduce allocation and vectorize compressed keys

A temporary phase probe found warm diagnostic medians of 27.479 ms in the
grouped fact scan, 2.964 ms building domains, 2.514 ms finishing results and
0.348 ms proving key sequences. These are instrumented phase measurements,
not promotion timings. The [instrumentation patch](q11-phase-diagnostic.patch)
and [diagnostic record](native-direct-phase-diagnostic-q11-sf100.json) preserve
the experiment; phase logging is absent from the retained executor.

`8516f53` changes `src/executor_sorted_sum.hpp`. Each retained group previously
grew a vector from an empty capacity. Counting selected bitmap bits gives an
upper bound on the number of groups. The initial reservation is bounded by
that count, the stripe's integer-key range, and 4096 entries. It is enabled
only when all initial reservations together fit in one eighth of the query
allowance. Stripes that need more groups still grow normally. Selective HAVING
paths retain their existing incremental pruning. This removes reallocations
and avoids rounding every typical Q11 stripe up to a larger final capacity.
The smaller 256-entry trial barely helped and was superseded; its
[patch](sorted-reserve256-diagnostic.patch) applies to `b66f773`.

`581fc52` changes `src/executor_membership.hpp`. The bitmap membership kernel
already used masked AVX-512 gathers for 32-bit native keys, but narrower keys
fell back to scalar `Read` and membership checks. It now sign-extends sixteen
8-bit or 16-bit keys into 32-bit lanes and uses the same bounded bitmap probe.
Out-of-domain lanes remain masked out before gathering; anti-membership keeps
the complement within the previous selection mask. Short tails and unsupported
representations retain scalar execution.

The latest build passes 276 native tests, all 22 SF1 answers and Python
integration. Added tests compare SIMD and scalar paths against independently
computed membership for negative values, masks, tails and anti-membership.
The nonempty SF1 Q11 answer is included, since its SF100 answer is empty.
All implementations are original and introduce no new dependencies.

Evidence: [Q2 before](native-direct-buffer8-q2-sf100.json),
[Q2 after](native-direct-shared-min-q2-sf100.json),
[Q11 before](native-direct-buffer8-q11-sf100.json),
[small reservation](native-direct-sorted-reserve-q11-sf100.json),
[selected-count reservation](native-direct-sorted-reserve4k-q11-sf100.json),
[narrow-key SIMD](native-direct-narrow-member-q11-sf100.json).

## Q22: avoid substring construction for a proved ASCII prefix test

The complete narrow-membership sweep recorded 377.226 ms for Q22, versus
375.958 ms in the original baseline. A subsequent change in
`src/executor_prefix_antijoin.cc` eliminates generic substring construction
for every phone dictionary label. The plan already proves a leading
two-character slice and ASCII two-byte literals. A match therefore requires
two leading ASCII bytes, which directly address the existing lookup table.
Valid UTF-8 suffixes remain supported, and the whole string is still validated
before that shortcut; an invalid suffix cannot be hidden by an ASCII prefix.

The [follow-up](native-direct-prefix-bytes-q22-sf100.json) passes nine SF100
answers at 219.790 ms warm, with a 2.225-second first call under 8 GiB.
All 277 native tests, all 22 SF1 answers and Python integration pass. Existing
tests cover short strings, non-ASCII leading characters and UTF-8 suffixes;
the added test verifies rejection of an invalid suffix. Keep this change for
its substantial execution-time margin. It does not replace Q22's measurement
in the preceding complete sweep.

## Sorted arithmetic gathers: retain a scoped policy

The [global experiment](global-gather-experiment.patch) gathered eight signed
32-bit or 64-bit values explicitly with vector instructions. It passed 278
native tests and all SF1 answers. Paired fresh-process SF100 measurements
showed a mixed result: Q11 improved from 30.369 to 29.257 ms, while Q3 rose
from 347.983 to 355.643 ms. Both queries passed all nine answers under 8 GiB.
The global policy was not retained.

`d8e6591` makes vector gathering an explicit template option and enables it
only for sorted aggregation, including arithmetic input decoding and grouping
keys. Other join paths retain their original policy. Tests cover signed
extremes, repeated indices, nonzero offsets, unaligned outputs and short tails.
The final build passes 279 tests, all 22 SF1 answers and Python integration.
[Q11](native-direct-scoped-count-q11-sf100.json) runs in 28.640 ms warm;
the [Q3 control](native-direct-scoped-count-q3-sf100.json) is 344.759 ms.
This is a measured choice for sorted aggregation, not a claim that explicit
gathers improve every kernel.

## Q12: identify the grouped input of a conditional count

Q12's SUM expressions contribute constant CASE values. The old preparation
rule therefore found no numeric measure column and retained the whole fact
input, despite its literal date and shipping-mode filters. `a20dcf7` uses the
single string/date grouping input for a two-table conditional-count shape.
Only that input loses rows; the other table retains its key sequence. The
existing inner-scope and unique-physical-table guards still apply. The original
query checks its date comparisons and evaluates both conditional counts.

The [Q12 follow-up](native-direct-scoped-count-q12-sf100.json) passes all nine
answers at **204.854 ms warm and 1.294 GiB peak**, versus the saved 341.982 ms
and the earlier direct run's 7.458 GiB peak. First preparation is 10.946 seconds.
The new test checks selected fact rows, intact dimensions and exclusion of
outer joins using the parser's supported aggregate shapes. All 279 tests and
the [final 22-query SF1 gate](auto-projection-sf1-scoped-count-final-gate.json)
pass. These follow-ups have not yet been incorporated into a complete sweep.
