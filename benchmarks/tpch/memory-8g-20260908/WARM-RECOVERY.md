# Recovering Q2 and Q11 warm performance under 8 GiB

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
