# Experiment 10: compact grouped exact distinct state — keep

Represent an empty or singleton integer set without a backing hash allocation.
For larger integer sets, reuse the original flat integer set. If a string or
number outside signed 64-bit range arrives, build a general logical-value set
before replacing the compact state. Promotion retains all prior values and
string ownership; allocation failure leaves the prior logical set readable.
Worker unions remain exact. This changes aggregate state, not query answers.

All 47 native tests pass in release and under AddressSanitizer plus
UndefinedBehaviorSanitizer. New checks include signed boundaries, wide
expression promotion, failed promotion, and cross-worker exact unions. The
sanitizer log is `../docs/clickbench/research-20260905/sanitize-b3e922a.log`.

All 21 selected full-data trial answers validate. Q22's tied-row mismatch
passes the independent tie gate. Controls without distinct are retained.

| Query | Baseline PG warm ms | Experiment warm ms | Recorded CH warm ms |
| --- | ---: | ---: | ---: |
| Q9 | 12241.457 | 1663.678 | 694.477 |
| Q10 | 12687.377 | 2171.889 | 775.952 |
| Q11 | 1351.826 | 634.842 | 187.475 |
| Q12 | 1227.440 | 378.341 | 232.250 |
| Q14 | 23387.430 | 12718.610 | 627.224 |
| Q22 | 272.767 | 290.731 | 120.515 |
| Q23 | 426.906 | 404.372 | 593.064 |

Keep the broad distinct improvement, but Q9–Q14 still trail ClickHouse.
Q22 does not improve; Q23 retains its prior win. Per-group allocations and
repeated global string identity checks remain important costs, motivating
inline distinct payloads and shared dictionary identity research next.

Original implementation; no new dependency, index preprocessing, or result
cache. Raw files: `../docs/clickbench/research-20260905/exact-set.jsonl` and
`exact-set-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/exact-set-query-runner`.
