# Correlated-query iterations

Times are client elapsed milliseconds with eight workers. Warm medians
exclude trial 1. Each experiment retains all timings and exact answer
checks in [CORRELATED.json](CORRELATED.json). Native loading is separate.
These queries have not passed the fourfold performance target.

## Fresh SF10 baseline

Frozen engine checkpoint: `7c92573`. Three trials per query and engine.

| Query | PGAccel ms | DuckDB ms | ClickHouse ms | PGAccel query peak MiB |
|---|---:|---:|---:|---:|
| Q20 | 35730.972 | 145.186 | 437.550 | 8767.47 |
| Q17 | 55118.722 | 182.220 | 586.614 | 2930.05 |
| Q2 | 8003.267 | 65.772 | 300.185 | 1140.15 |

## Q20 required-domain pushdown

Commit `7a7f29a` copies a required outer IN restriction onto its matching
correlated child key. The child uses existing membership execution and
budget accounting, retaining composite grouping and empty-result semantics.
The membership subquery is currently reevaluated for the child; caching
or sharing is not assumed. All 155 native and sanitizer tests pass.

| Experiment | Query | Warm median ms | Warm min ms | Warm max ms | Query peak MiB |
|---|---|---:|---:|---:|---:|
| pgaccel-domain-v1-sf10 | Q20 | 2016.970 | 2005.738 | 2028.203 | 96.78 |
| pgaccel-domain-v2-sf10 | Q20 | 932.892 | 896.343 | 952.205 | 94.72 |
| pgaccel-domain-v3-sf10 | Q17 | 195.697 | 193.578 | 201.508 | 3.20 |
| pgaccel-domain-v3-sf10 | Q2 | 187.239 | 186.044 | 188.367 | 6.45 |
| pgaccel-domain-v3-sf10 | Q20 | 895.040 | 888.271 | 923.528 | 94.72 |
| pgaccel-domain-v4-sf10 | Q17 | 122.720 | 121.438 | 123.378 | 3.62 |
| pgaccel-domain-v4-sf10 | Q2 | 182.752 | 180.945 | 184.545 | 6.45 |
| pgaccel-domain-v4-sf10 | Q20 | 867.227 | 852.793 | 889.320 | 94.72 |
| pgaccel-domain-v5-sf10 | Q20 | 529.833 | 525.367 | 533.957 | 94.72 |
| pgaccel-domain-capacity-sf100 | Q20 | 10853.663 | 10799.385 | 10907.941 | 952.78 |

Retain v1 for the measured capacity and latency improvement. It is still
slower than both competitors at SF10. Retain v2 (`100285a`): nine correct
trials cut its warm median to 932.892 ms using exact bitmap membership.
All 158 native and sanitizer tests pass for v2. Broad or unsuitable
domains retain the hash representation.

Retain v3 (`2f9a808`): independently filtered outer dimensions produce bounded key
domains for correlated children and equality-connected outer inputs.
At most one million selected rows and one eighth of the dimension may
qualify. Broader or unsupported shapes keep the prior execution plan.
Duplicate dimension rows retain join multiplicity. All 159 native and
sanitizer tests pass. Q17 and Q2 improve substantially but still miss
the fourfold target; their measured temporary states are now small.

Retain v4: parallel native/membership mask preparation uses at most
2 MiB batch scratch and preserves serial correlation bindings. Q17 falls
from 195.697 ms to 122.720 ms. Q2 and Q20 changes are small enough that
no separate speedup claim is made for them. All 160 native and sanitizer
tests pass, including partial final batches and anti-membership.

SF100 capacity checks, if listed above, are separate from the latency
gate. They run under a 192 GiB service cap, zero swap, and a 64 GiB query
allocation cap. They do not establish a fourfold performance win.

[Staged plan and research](CORRELATED-PLAN.md) ·
[Six completed queries](SIX.md)

## Q20 SF100 capacity result

The frozen v3 binary (`2f9a808`) completes three exact-answer trials.
Warm median is 10.854 seconds with 952.78 MiB peak query allocation.
Service peak including loading/cache is 107.07 GiB, with
0 swap bytes and 0 OOM events. The prior Q20 execution exceeded
its 64 GiB query allocation limit; this capacity failure is resolved.
Competitors were not rerun at SF100 in this capacity-only experiment.
This is not a fresh full 22-query comparison or a latency-gate pass.

## Retained prefix pruning

V5 narrows literal-prefix LIKE to a sorted dictionary label range before
running exact regex checks. Nine SF10 Q20 trials validate; warm median is
529.833 ms versus v4's 867.227 ms. Peak query memory remains bounded. All
161 native and sanitizer tests pass. The fourfold target remains unmet.
Q20 SF100 numbers above belong to v3, not this newer binary.

## Shared-executor regression checks

Q16, Q18 and Q22 each pass a fresh SF10 answer check against DuckDB and
ClickHouse using v4 (`6cba692`). These are single-trial correctness checks,
not warm performance comparisons. See [MEMBERSHIP-REGRESSION.json](MEMBERSHIP-REGRESSION.json).
