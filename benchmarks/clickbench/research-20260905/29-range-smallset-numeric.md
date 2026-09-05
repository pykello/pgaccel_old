# Experiments 29–31: narrow domains, small distinct sets, numeric radix records

These three contained implementation commits share one nine-query measurement.
They are retained, with attribution limits stated below. Original queries,
full data, eight threads, minimum client milliseconds of trials two and three.
All 27 trial answers validate; the 75-test native suite passes.

## Actual numeric ranges — 4a1aa55

Pack unindexed numeric identities using the complete validated minimum and
maximum, rather than the physical storage width. Subtract the minimum in
unsigned arithmetic, retaining exact identities even across signed extremes.
Constant columns require no bits. Global dictionary IDs already have bounded
domains and keep their existing encoding.

For example, values INT64_MIN and INT64_MIN+1 need one bit, as do
INT64_MAX-1 and INT64_MAX. Their composite needs two identity bits while
preserving all original values for output. The new regression checks this
case and an additional constant negative key.

This can admit more composite key shapes. It does not produce a demonstrated
standalone win in this subset; Q17's identities already use global IDs.

## Inline per-value distinct groups — a7d49c5

While visiting one value's row postings, retain the first eight encountered
group identities inline. Repeated identities need no sorting. Additional
identities use the existing original exact integer set. A group is incremented
only once for that input value, including repetitions crossing stripe boundaries.

Q9 and Q10 have unchanged globally indexed group domains, so the following
improvements are evidence for replacing per-value row sorting. Q12 also benefits
from narrower numeric group domains, so its two effects are not isolated.

## Numeric radix records — 9bb51f9

For packed composite grouping with up to three direct SUM/AVG inputs, copy
the narrow source numbers beside each key during the scan. Scatter complete
records into disjoint hash-prefix partitions, then aggregate exact wide sums
there. Share one row count across aggregates. This avoids partial-table unions
and later random source-column reads. Nearly unique groups keep their faster
existing path.

Tests cover one, two, and three numeric payloads, signed wide sums, nonzero
decimal scale, filtered composite groups, empty selections, and pagination.

## Results before the buffer-initialization follow-up

| Query | Indexed checkpoint 2 | New subset PG | Fresh ClickHouse |
| ---: | ---: | ---: | ---: |
| 9 | 569.150 | 469.521 | 670.641 |
| 10 | 840.358 | 722.902 | 762.971 |
| 11 | 286.918 | 279.841 | 173.796 |
| 12 | 313.576 | 307.295 | 219.490 |
| 14 | 527.992 | 542.777 | 618.364 |
| 17 | 1682.662 | 1724.775 | 1364.565 |
| 31 | 615.776 | 471.183 | 292.493 |
| 32 | 79.100 | 79.575 | 422.209 |
| 33 | 444.147 | 478.981 | 2730.306 |

Q10 becomes a win. Q31 improves but remains a loss. Q11 and Q12 remain
losses. Q32 and Q33 remain wins through the preserved nearly unique path.

Measured engine code 9bb51f9; metadata HEAD b49c5a6 also includes the Q22
validation-script extension. Evidence: range-numeric-smallset.jsonl, its
.meta.json and -gate.json. Frozen binary:
../clickbench-work/research-20260905/bin/range-numeric-smallset-query-runner.
All implementations are original and add no dependency.
