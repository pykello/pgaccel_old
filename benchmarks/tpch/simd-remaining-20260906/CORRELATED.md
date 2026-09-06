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

SF100 capacity checks, if listed above, are separate from the latency
gate. They run under a 192 GiB service cap, zero swap, and a 64 GiB query
allocation cap. They do not establish a fourfold performance win.

[Staged plan and research](CORRELATED-PLAN.md) ·
[Six completed queries](SIX.md)
