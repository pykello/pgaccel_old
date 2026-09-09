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
