# Research status

The extended target is met: **43/43 queries are faster than ClickHouse** in
the fresh full-data resident warm comparison. The shifted geometric PG/CH
ratio is **0.267877**; summed warm minima are **7.311 s vs 21.526 s**.

- [All 43 timings, setup costs and evidence](46-checkpoint4-all-queries.md)
- [Reproduction instructions](reproduce.md)
- [Current staged follow-up plan](47-follow-up-plan.md)

Both engines have **129 validated trial answers**. All **95 native tests and
95 ASan/UBSan tests pass**. The report explicitly preserves the declared Q4
wide-average policy and source verification of valid ordering ties.

This is the experimental indexed configuration on 99,997,497 rows and 105
columns, using eight threads. It retains **27.581 GiB** of auxiliary indexes,
with **142.189 s** of construction after **49.161 s** of native loading.
These are warm query results, not a default unindexed, cold-start, concurrent
service or official leaderboard claim. No query-result cache or new external
dependency is retained.

Final engine source is `e7dd24716ebbb6d592fb8138481c2011cbe10c88` on
`research/clickbench-20260905`; later commits document the result. Raw results
beside the external docs mirror are `checkpoint4-indexed-comparison.json`,
`pg-checkpoint4-indexed.jsonl` and `ch-checkpoint4.jsonl`, with metadata and gates.

The four losses from checkpoint 3 were closed with dense string-length
aggregation (Q28), sparse predicate evaluation (Q22), inclusive top-K bounds
(Q24), and weighted dictionary evaluation plus a restricted exact regex plan
(Q29). Reports 38–45 retain individual experiments and their evidence.

Historical checkpoints remain available: [unindexed checkpoint 1](12-checkpoint1.md)
(18/43), [indexed checkpoint 2](26-checkpoint2-indexed.md) (32/43), and
[indexed checkpoint 3](36-checkpoint3-target.md) (39/43). The follow-up plan
addresses remaining product work rather than unresolved ClickBench losses.
