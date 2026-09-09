# Candidate: prune grouped sums using a growing scalar lower bound

This is an implementation proposal, not a retained optimization or benchmark
claim. `src/executor_sorted_join_sum.cc` already proves that the grouped parent
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
