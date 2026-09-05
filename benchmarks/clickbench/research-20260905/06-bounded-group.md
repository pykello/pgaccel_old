# Experiment 7: redundant identities and bounded group limits — keep

Q36 groups by ClientIP and three affine expressions of ClientIP. The explicit
integer base determines these additional keys. Hash and compare the base
only, retaining normal projection of the derived values. Only signed stored
integers plus/minus signed 64-bit literals qualify: they cannot overflow the
checked 128-bit domain. General multiplication remains checked.

For an unordered GROUP BY with LIMIT, retain each worker's smallest K keys,
where K includes OFFSET. Any globally selected key must belong to each
contributing worker's smallest K; therefore its aggregate remains complete.
As boundaries shrink, prune later stripes using a safe upper bound on the
first grouping column. Require the visible output to begin with the grouping
columns, preserving the existing deterministic selection. Exclude HAVING and
computed expressions whose evaluation could otherwise be skipped.

| Query | Previous experiment warm ms | New warm ms | Recorded CH warm ms |
| --- | ---: | ---: | ---: |
| Q18 | 2840.1 | 9.300 | 456.740 |
| Q36 | 6600.4 | 1765.814 | 317.880 |

All six full-data trial answers validate. Q18 now wins; Q36 still loses.
All 42 native tests pass, including complete counts across eight workers,
OFFSET, excluded HAVING, signed extremes, and unsafe expression errors.
Q18 uses 31,696 tracked query allocation bytes. No preprocessing or result
cache was added. Both implementations are original and add no dependency.

Raw evidence: `../docs/clickbench/research-20260905/bounded-group.jsonl`
and `bounded-group-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/bounded-group-query-runner`.
These measurements do not replace a full-suite comparison.
