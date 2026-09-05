# Research status

The goal has been extended: continue until **43/43 queries** are faster than
a fresh paired ClickHouse run. Work is active on Q22, Q24, Q28 and Q29.
The complete checkpoint below remains the latest validated full comparison;
individual experiments do not replace it.

The requested resident warm comparison target is met: **39/43 query wins
(90.70%)** and a **0.368585** paired geometric PGAccel/ClickHouse latency
ratio with a 10 ms shift. Summed warm minima are **17.982 s vs 21.694 s**.

See the [final 43-query comparison](36-checkpoint3-target.md),
[reproduction guide](reproduce.md), and [staged follow-up plan](37-follow-up-plan.md).
Both engines have 129 validated trial answers; 84 native and 84 sanitizer
tests pass. The report explains Q4's declared wide-average policy.

This is the experimental indexed configuration on the complete 99,997,497-row,
105-column dataset, using eight threads. It adds **26.176 GiB** of indexes
and **136.132 seconds** of construction after **48.137 seconds** of native
loading. It does not establish a default unindexed or cold-start advantage.
No query-result cache or new external algorithm dependency is retained.

Final engine source is `6e704b67b4e3abf5f79596f949f24514fef2d6a9` on
`research/clickbench-20260905`; later commits document the result.
Raw results beside the external docs mirror are
`checkpoint3-indexed-comparison.json`, `pg-checkpoint3-indexed.jsonl`, and
`ch-checkpoint3.jsonl`, with corresponding metadata and correctness gates.

ClickHouse still wins Q22, Q24, Q28 and Q29. The follow-up plan addresses
those losses and the memory, persistence and semantic gates needed for a
bounded read-only analytics service.

Historical results remain available: [unindexed checkpoint 1](12-checkpoint1.md)
won 18/43 at 1.205092; [indexed checkpoint 2](26-checkpoint2-indexed.md)
won 32/43 at 0.468085. Numbered experiment reports preserve accepted and
rejected ideas and their individual measurements.
