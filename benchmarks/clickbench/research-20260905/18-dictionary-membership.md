# Experiment 18: vectorized dictionary membership

Keep the original AVX-512 implementation. Represent a stripe's matching
labels as an 8 KiB bitmap. For sixteen row codes at a time, gather the
containing words, shift by each code's bit position, and write the sixteen
membership bits. Intersections skip empty chunks. The scalar path walks
selected bits and preserves partial output bytes.

This changes row membership evaluation after exact label matching; it does
not cache query answers or add a persistent index. Both 8-bit and 16-bit
codes use the same logical representation. The label-matching semantics of
LIKE, NOT LIKE, IN, and NOT IN remain unchanged.

## Full-data subset measurement

Engine `4f6c7cb`, dictionaries disabled, eight threads, unchanged queries,
three trials each. All twelve answers pass the full validation gate,
including the canonical Q22 tie refinement. Times are minimum warm client
milliseconds; ClickHouse is the independently validated checkpoint 1 run.

| Query | Checkpoint 1 PG | Membership PG | ClickHouse |
| ---: | ---: | ---: | ---: |
| 21 | 274.015 | 212.544 | 336.286 |
| 22 | 270.629 | 217.762 | 96.774 |
| 23 | 426.411 | 343.636 | 537.647 |
| 24 | 271.047 | 214.561 | 89.395 |

No new query win is claimed from this subset. The cheaper row scan improves
all four measured cases, but exact dictionary-string matching remains a
major cost. A separate experiment will test conservative string signatures.

## Reproduction and evidence

Raw files in `../docs/clickbench/research-20260905/`:
`dictionary-membership.jsonl`, its `.meta.json`, and
`dictionary-membership-gate.json`. The frozen executable is
`../clickbench-work/research-20260905/bin/dictionary-membership-query-runner`.

The 63-test suite passed. The new regression covers both execution modes,
65,536 labels, scalar and AVX paths, intersections, and byte-tail boundaries.
This implementation is original and introduces no dependency.
