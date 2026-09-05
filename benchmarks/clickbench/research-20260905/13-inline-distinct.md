# Experiment 12: inline grouped integer distinct — keep selective dispatch

Move one direct numeric COUNT DISTINCT into the contiguous group payload.
Keep the first distinct identity inline and allocate a flat backing set only
for additional identities. Dictionary group caches retain stable owner/index
locations. This removes per-group state and shared-pointer allocations from
high-cardinality single-column grouping.

| Query | General compact state ms | Unrestricted inline ms | Selective dispatch ms |
| --- | ---: | ---: | ---: |
| Q9 | 1663.678 | 1401.448 | 1462.655 |
| Q11 | 634.842 | 727.410 | 646.418 |
| Q12 | 378.341 | 406.005 | 403.660 |
| Q14 | 12718.610 | 1245.520 | 1206.076 |

Unrestricted dispatch regressed the small Q11/Q12 groupings. Keep the general
state for composite keys and string dictionaries whose maximum local label
count is at most 4,096. Retain inline state for eligible raw numeric groups
and larger single-column string groups. Q14 benefits substantially. The
restored paths do not completely reproduce earlier best timings; no gain is
claimed for Q11/Q12. The threshold is a measured heuristic, not a semantic
condition or a benchmark column-name check.

All 12 initial trials and all 15 selective/expression trials validate.
Native tests cover singleton allocation, signed identities, exact unions,
and cached indexes through growth across independent dictionaries. There are
51 passing native tests at the selective/expression checkpoint. No new
persistent index or dependency was added by this experiment.

Raw evidence: `inline-distinct.jsonl`, `inline-distinct-gate.json`,
`dictionary-expression.jsonl`, and `dictionary-expression-gate.json` in
`../docs/clickbench/research-20260905/`.
