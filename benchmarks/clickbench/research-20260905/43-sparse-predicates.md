# Experiment 43: cheap comparisons before sparse dictionary predicates

Implementation: `c2b946b`. The previous filter order followed column position,
so Q22 evaluated URL substring matching before the cheap encoded comparison
`SearchPhrase <> ''`. Move direct comparisons before value predicates, retaining
same-column ordering and numeric range fusion within the cheap class.

For an intersection selecting at most a quarter of a stripe, and fewer than
half its dictionary labels, enumerate only local labels reached by selected
rows. Evaluate their signatures and exact predicates, then apply membership
to the surviving row bits. Small dictionaries and dense selections keep the
existing vectorized whole-dictionary path. This works for LIKE, NOT LIKE and
other value predicates without depending on benchmark column names.

| Query | Previous SIMD-signature warm ms | Sparse-predicate warm ms |
| --- | ---: | ---: |
| Q21 | 122.877 | 118.129 |
| Q22 | 129.051 | 40.188 |
| Q23 | 192.118 | 50.326 |
| Q24 | 122.566 | 121.319 |

Q22 is now a provisional win against the previous ClickHouse checkpoint's
105.183 ms. Q24 still loses to 114.553 ms. Q28 remains 142.849 ms; Q37, Q38 and
Q39 controls are 3.363, 5.815 and 5.353 ms. All 24 full-data trial answers pass
the gate, including deterministic refinements for Q22 and Q39 boundary ties.
All 91 native tests pass, including sparse positive and negative predicates,
independent label spaces and a partial final bitmap word.

Decision: keep. No extra persistent index data is introduced. The sparse path
uses fixed stripe-local needed-label and result bitmaps; no cross-query cache
or query-specific index is involved.

Artifacts: `pg-sparse-predicates.jsonl`, its metadata and gate in the external
docs mirror. A fresh complete comparison remains necessary before promoting
the extended 43/43 target.
