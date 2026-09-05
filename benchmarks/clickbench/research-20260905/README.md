# ClickBench improvement research

Started 2026-09-05 06:42:57 UTC. Initial research window: eight hours.
The subsequently requested stopping goal takes precedence over that window:
beat ClickHouse on at least 39/43 queries and in paired geometric mean latency
with the ClickBench 10 ms shift, on the complete 99,997,497-row fixture.

## Measurement and acceptance

Baseline source: `b273cfd0094a33619fad4dabd29bbc881631d89c`.
Preserved binary SHA-256:
`96278b43bbd835172cdb3be23ef0dce05c19ec07fd8f1b36c983f03464fc0725`.
The binary is in `~/projects/clickbench-work/research-20260905/bin/`.
Prior complete three-trial baseline and ClickHouse results are in
`../docs/clickbench/results/staged-work/item5/`.

Use the unchanged pinned queries, complete results, eight threads, and a
32 GiB query allocation budget. Engines and compilation run sequentially during
timing. Warm latency is the minimum of trials two and three; retain all samples.
Final acceptance requires all 129 trial answers to pass the independent gate,
including the declared Q4 wide-AVG policy and independently verified tied-order
alternatives. Report client and executor timing separately. These are resident
warm measurements; indexes or other preprocessing must disclose build time and
memory, and no query-result cache may substitute for executing timed queries.

Keep each change a contained commit with an explanatory body and lines <=72
characters. Record hypotheses, implementation, correctness, timings, and the
keep/reject decision. Preserve rejected implementations through commit history
and preserve raw benchmark artifacts outside the source tree. Only original
implementations or dependencies with verified MIT/BSD/Apache-style permissive
licenses may be incorporated; record dependency licenses before adoption.

## Initial experiment queue

1. Close the observed Int32 literal narrowing hole before new filter work.
2. Profile representative high-cardinality queries and identify allocation,
   expression, hashing, merge, and finalization costs.
3. Specialize numeric exact-distinct storage and eliminate redundant set probes.
4. Test compact composite aggregate state and hash layouts that avoid per-group
   aggregate-vector allocations.
5. Evaluate dictionary-level expression evaluation for regex/length grouping,
   retaining exact typed equality and owned string lifetimes.
6. Evaluate expression binding/caching and compact encoded grouping keys while
   preserving independent dictionary identities across stripes.
7. Prototype the user's bit-sliced/range-bitmap proposal against current SIMD
   comparisons on random and clustered, low- and high-cardinality columns.
8. Improve filtered and ordered projection, including late materialization.
9. Consider generic uniqueness/statistics and sorted-index optimizations only
   with complete correctness and honest preprocessing accounting.
10. Run full before/after trials, fresh ClickHouse comparisons, date regression
    checks, memory and sanitizer checks; reassess the remaining losses.

The queue is adaptive: evidence determines the next experiment. Completing the
queue does not by itself satisfy the performance objective.
