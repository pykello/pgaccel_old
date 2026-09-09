# Candidate: prune grouped sums using a growing scalar lower bound

The staged proposal below is now implemented by `840bb3a` and `9e436af`.
The final proof and measured limits are recorded after the original stages. `src/executor_sorted_join_sum.cc` already proves that the grouped parent
and scalar child use the same fact expression and join/filter domain. It scans
once, retains groups in `src/executor_sorted_sum.hpp`, obtains the scalar total,
then evaluates HAVING. Q11's SF100 result is empty, but its SF1 result is not;
an optimization must prove rejection from current data, never from query ID,
scale factor, saved answers or a guessed group bound.

A possible single-pass policy applies to SUM(value) > a * total, or >=, only
when a is nonnegative, values are nonnegative, and exact scaling is monotone.
The sum of completed stripes is then a lower bound on the final total. A
completed interior group that already fails the predicate against that lower
bound cannot pass against the final, larger bound. Discarding it avoids growing
and later reading the retained group vector.

Implementation stages:

1. Add an explicit opt-in policy to the sorted scanner. Prove supported scalar
   formula, comparison, nonnegative expression bounds and checked total range.
   Preserve the existing route when the proof fails. Do not reinterpret a
   negative or nullable input as zero or silently narrow wide arithmetic.
2. Publish completed stripe totals with bounded synchronization. Snapshot the
   lower bound once per stripe, not with an atomic operation for every row.
   Delayed publication is conservative: it may retain extra groups but must
   not discard a group that could pass the final predicate. Treat publication
   overflow as a failed fast-path proof, with normal fallback.
3. Keep first and last groups intact until cross-stripe merging. A partial
   boundary sum cannot justify rejecting the completed group. Continue using
   the final exact scalar bound for all groups that survive the early filter.
4. Validate nonempty results, GT versus GE at equality, zero multiplier,
   negative inputs, unsupported formulas, decimal rounding, wide totals,
   empty selections and keys spanning several stripes. Compare serial and
   parallel modes against an independent executor route. Race freedom must
   not rely on a particular stripe completion order.
5. Measure Q11 at SF1 and SF100, total query-state allocation, and first-call
   and warm timing under the 8 GiB zero-swap cap. Check other callers of the
   shared scanner. Retain only a measured gain with exact answers; save failed
   experiments rather than re-running until a preferred sample appears.

An alternative two-pass scheme computes total and maximum group sum first,
then rereads only if some group can pass. It is easier to justify but risks
slowing nonempty results. Prefer evaluating the bounded single-pass policy
first; it can preserve the existing output path and avoid a compulsory second
fact scan. Neither proposal caches answers or skips the original membership
and arithmetic proofs on a new execution.


## Retained implementation and measurements

The scanner creates a retention predicate once per stripe and publishes the
stripe sum after it finishes. The join route opts in for strict/inclusive lower
HAVING comparisons with a shared SUM or SUM multiplied by a nonnegative
numeric constant. It compiles each stripe's arithmetic bounds first to prove
all values nonnegative. Negative inputs and unsupported scalar formulas use
the existing scanner. No query ID, scale factor or answer is consulted.

Completed stripe sums are added to an atomic signed 64-bit lower bound. The
counter saturates at INT64_MAX, which remains conservative when the actual
nonnegative total is wider. This refines stage 2: saturation avoids treating
representable wide totals as failures. Relaxed snapshots can be stale but
cannot exceed the final total. Each stripe computes its exact scaled cutoff
once. A completed interior group can be dropped only if it already fails
against that cutoff. The first and current last groups are retained for exact
cross-stripe merging. The final scalar total still comes from the exact wide
sum of all stripe sums, including pruned groups.

All 282 tests, 22 SF1 answers and Python integration pass. New tests include
nonempty outputs, zero/negative multipliers, strict/inclusive comparisons,
exact and fractional thresholds, a key spanning three stripes, and final totals
beyond signed 64-bit range in both serial and parallel modes. Existing tests
retain negative-value, different-domain, duplicate-key and fallback coverage.
SF1 Q11 is nonempty and checked against the saved typed DuckDB oracle.

| SF100 Q11 | Warm median ms | Query-state peak MiB | Total peak GiB |
|---|---:|---:|---:|
| Control | 28.571 | 49.415 | 1.111 |
| Growing lower bound | 28.181 | 1.676 | 1.067 |

Both sequentially measured fresh-process runs pass all nine answers under
8 GiB with zero swap. The large improvement is memory; warm timing improves
only about 1.4%. Neither individual timing replaces the previous complete
sweep's Q11 regression. The final frozen module must still be checked in the
full workload before claiming the all-query warm target is met. Preparation
latency and general admission remain separate unfinished work.

Evidence: [control](native-direct-growing-bound-control-q11-sf100.json),
[candidate](native-direct-growing-bound-q11-sf100.json),
[SF1 gate](auto-projection-sf1-growing-bound-gate.json).


## Complete workload validation

[TRANSITION-HAVING.md](TRANSITION-HAVING.md) runs all 22 queries and 198 calls
with the retained module in one database. Every answer passes at 6.882 GiB
peak, with zero swap, zero sampled file charge and no memory-limit events.
Q11 is 29.873 ms versus its saved 30.496 ms. Q17 and Q18 now exceed their
saved times; this report retains those regressions. The full goal remains
open, and the next execution work targets those queries.
