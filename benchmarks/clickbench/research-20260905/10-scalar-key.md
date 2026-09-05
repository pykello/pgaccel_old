# Experiment 11: scalar identities inside representative keys — keep

When a single stored numeric column determines a group, the existing 64-bit
key word can retain that exact value. Hash it for partitioning and table
lookup, then compare the identity directly. This avoids rereading resident
rows when a group is found again or when worker tables merge. The resident
row reference remains available for output, and external sorting continues
to compare logical values. The key payload does not grow.

Q36 improves from 1765.814 ms to 1292.869 ms warm client time. Its three trials
are 1362.562, 1327.066, and 1292.869 ms, and all answers validate. All 47 native
tests pass, including signed extremes, wide affine output, date grouping,
and external spill checks. The refreshed ClickHouse Q36 is 330.542 ms, so this
remains a loss despite the improvement.

Original implementation; no new dependency or preprocessing. Raw evidence:
`../docs/clickbench/research-20260905/scalar-key.jsonl` and
`scalar-key-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/scalar-key-query-runner`.

The fresh full ClickHouse checkpoint is `ch-checkpoint1.jsonl` in the same raw
results directory. All 43 records match the existing ClickHouse oracle. Its
shifted geometric mean is 0.9857 times the previous CH baseline; individual
queries vary, so future overall comparisons should use this fresh checkpoint.
