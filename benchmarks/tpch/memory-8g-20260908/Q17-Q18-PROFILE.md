# Q17 and Q18 execution profiles

Temporary [instrumentation](q17-q18-profile.patch) applies to `94da4c8` and
has been removed from production source. Both queries pass all nine typed
SF100 answers under 8 GiB with zero swap. Times below exclude the first trial;
instrumentation perturbs timing, so these diagnose phases rather than replace
the complete uninstrumented comparison.

Q17 uses ExecuteCorrelatedAvg. Median phase wall times:

| Phase | ms |
|---|---:|
| dimension | 8.736 |
| fact_scan | 251.762 |
| thresholds | 2.910 |
| selected_values | 12.107 |

Q18 uses ExecuteSortedSum for its grouped membership subquery and
ExecuteStarSum for the outer query. The sorted scan median is
594.103 ms, compared with 760.998 ms for the complete client call.
The difference includes outer execution, finishing and client overhead;
this first profile does not separate those remaining components.

Next experiment: Q17's bitmap membership scan dominates. A compact summary
of nonempty membership words could reject most sparse probes before fetching
from the larger bitmap. It must preserve exact membership, masked range
checks, all tails and the scalar fallback, and count its storage in the query
budget. Q18 needs a separate grouped-scan optimization after Q17.

Evidence: [Q17](native-direct-q17-q18-profile-q17-sf100.json),
[Q18](native-direct-q17-q18-profile-q18-sf100.json).


## Retained Q17 change: summarize nonempty bitmap words

`7ad4e48` adds an opt-in second-level bitmap to the correlated-average probe.
One summary bit represents a nonempty 32-key word of the exact membership
bitmap. The SIMD probe first gathers summary words and clears lanes whose
membership word is empty. Remaining lanes perform the original exact
membership lookup. No false positives survive that second lookup, and a zero
summary bit cannot reject a present key. Invalid key lanes are masked before
both gathers; tails use the existing scalar membership test.

The summary is built on every execution and charged to the query budget.
It is enabled only with AVX, nonnegative signed-32-bit key bounds, a domain
larger than 2^20 keys, and selected cardinality less than 1/128 of domain
width. The summary requires roughly one extra bit per 32 domain keys, or
about 78 KiB for 20 million keys. Dense/small domains and other executor
callers retain the previous probe. No new dependency or third-party code was
introduced.

All 283 tests, 22 SF1 answers and Python integration pass. The new probe test
covers empty and nonempty words, both summary boundaries, masked out-of-range
keys including signed extremes, nonzero offsets, and scalar tails. The SF100
check exercises the automatic large-domain policy in the actual executor.

| Q17 run | Warm median ms | First call s | Total peak GiB |
|---|---:|---:|---:|
| Control | 275.683 | 7.219 | 5.765 |
| Two-level bitmap | 209.781 | 6.986 | 5.765 |

Both sequential fresh-process runs pass nine typed answers under 8 GiB with
zero swap. The warm reduction is about 24%, providing margin below the saved
272.281 ms. This isolated result does not replace the previous complete
sweep. Q18's measured sorted aggregation is the next target.

Evidence: [control](native-direct-summary-bitmap-control-q17-sf100.json),
[candidate](native-direct-summary-bitmap-q17-sf100.json),
[SF1 gate](auto-projection-sf1-summary-bitmap-gate.json).


## Retained Q18 change: combine contiguous equal-key runs

`b943b96` changes the all-row sorted scan to accumulate each equal-key run
within the decoded block before updating retained group storage. Previously
it tested the vector's last key and updated its stored sum for every row.
Runs crossing a block boundary still merge into the existing last group;
first and last stripe groups still use the exact wide cross-stripe merge.
The existing expression bounds prove block/run/stripe arithmetic fits before
using signed 64-bit partial sums. Unsorted input retains the fallback check.

Gathered scans keep the row loop, including Q11's adaptive HAVING path. The
change does not assume any fixed run length or use query-specific metadata.
All 284 tests, 22 SF1 answers and Python integration pass. New tests combine
single-row and varying-length runs, block/stripe boundaries, signed values,
filters and HAVING comparisons against the independent modern executor.

| Run | Warm median ms | First call s | Total peak GiB |
|---|---:|---:|---:|
| Q18 control | 751.764 | 9.048 | 6.676 |
| Q18 run reduction | 581.259 | 8.890 | 6.676 |
| Q11 gathered check | 27.782 | 1.363 | 1.067 |

All nine answers in every run pass under 8 GiB with zero swap. Q18 improves
about 23% and is below its saved 733.755 ms. Q11 remains below its saved
30.496 ms. The next full workload must verify these retained changes together
with Q17's bitmap summary; individual medians do not replace full-run values.

Evidence: [Q18 control](native-direct-sorted-runs-control-q18-sf100.json),
[Q18 candidate](native-direct-sorted-runs-q18-sf100.json),
[Q11 check](native-direct-sorted-runs-q11-sf100.json),
[SF1 gate](auto-projection-sf1-sorted-runs-gate.json).
