# Measured next steps after correlated-domain reductions

Checkpoint `6cba692` passes 160 native and sanitizer tests. SF10 warm
medians are Q17 122.720 ms, Q2 182.752 ms, and Q20 867.227 ms. All nine
answers per query validate. These three still miss the fourfold target.

Temporary timing instrumentation measured the following approximate
phases at SF10. These are diagnostics, not benchmark timings; parent
scalar/membership phases include child execution and must not be added
to the corresponding child phases.

| Query | Diagnostic cost | Next action |
|---|---|---|
| Q17 | Child filtering/row retention 22–26 ms; child aggregation 18 ms; outer join/aggregation 40–42 ms | Fuse selected fact grouping and threshold evaluation with typed states; avoid scalar result string conversion and repeated key allocation |
| Q2 | Child join/aggregation 22 ms; outer join/aggregation 106 ms | Compile repeated bound predicates and reduce scalar lookup overhead before changing join directory layout |
| Q20 | Part-name membership child about 186 ms, evaluated twice; child aggregate output about 100 ms | Prune literal-prefix dictionary ranges, then share membership results within a statement and retain typed grouped results |

1. Exploit sorted native string dictionaries for a literal-prefix LIKE.
   Locate its contiguous candidate label range with binary searches. Run
   the existing exact regex only on candidates, preserving NOT LIKE and
   invalid UTF-8 behavior. Sparse input selections retain their existing
   selective-label path. Measure Q20; do not infer a join speedup from it.
2. Share required membership domains between an outer query and its
   correlated child. Preserve statement lifetime and explicit memory
   ownership; never cache a result or directory across statements. Avoid
   treating a child budget as extra memory beyond its parent budget.
3. Introduce typed single-key grouped scalar state for Q17. Preserve AVG
   rounding/scale and empty groups, and save only selected fact row IDs
   when that avoids a second full scan. Prove build uniqueness or retain
   duplicate-preserving general execution. Compare against adversarial
   threshold-equality cases as well as generated TPC-H answers.
4. Reduce generic predicate and key-allocation overhead for Q2. Profile
   again after each retained change; do not specialize on TPC-H literal
   values, table names, or previously computed results.
5. Require fresh warm comparisons before claiming the fourfold SF10
   target, then validate the same frozen binary at SF100. Q20's capacity
   check is already complete but is separate from its latency target.

The diagnostic binary and log are retained externally as
`q20-domain-profile-query-runner` and `pgaccel-domain-profile-sf10.log`.
Instrumentation was removed; the rebuilt normal binary exactly matches
SHA-256 `0e89c534ee2c98d21dd6555f4a1fb4d0d65d37cd540a70d92e673fadc28d90b4`.
No diagnostic timing is mixed into the performance comparison.

The earlier [correlated-query plan](CORRELATED-PLAN.md) links the primary
research reference. These next steps follow the local phase measurements;
no external algorithm implementation or new dependency is copied.
