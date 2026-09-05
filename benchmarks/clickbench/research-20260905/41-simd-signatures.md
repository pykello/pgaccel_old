# Experiment 41: vectorized local signature rejection

Implementation: `55873e1`. Four consecutive 128-bit signatures fit in one
AVX-512 register. Compare each pair of 64-bit lanes with the required bits,
then enumerate only labels whose two lanes both pass. Those candidates still
receive the exact string predicate. Negated predicates begin with admitted
labels and clear exact matches; scalar execution and partial-vector tails
retain the existing behavior.

| Query | Scalar signature-loop warm ms | SIMD signature-loop warm ms |
| --- | ---: | ---: |
| Q21 | 138.284 | 122.877 |
| Q22 | 144.044 | 129.051 |
| Q23 | 217.954 | 192.118 |
| Q24 | 144.182 | 122.566 |

Here “scalar signature-loop” refers to the preceding implementation; both
benchmarks enabled AVX row-membership expansion. All 12 full-data answers pass
the gate, including Q22 tied boundaries. All 87 native tests pass, including
random substring checks, exact negation and independent local label layouts.
No additional persistent memory or build step was introduced by this change.

Decision: keep the consistent filter improvement. Q22 and Q24 remain slower
than the previous complete ClickHouse comparison (105.183 and 114.553 ms).
Further work must address the remaining predicate cost, not merely count this
subset as meeting the extended goal.

Artifacts in the external mirror: `pg-simd-local-signatures.jsonl`,
`pg-simd-local-signatures.meta.json`, and `pg-simd-local-signatures-gate.json`.
