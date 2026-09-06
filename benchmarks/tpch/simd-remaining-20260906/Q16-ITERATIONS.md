# Q16 iteration measurements

Nine-trial SF10 iterations; warm medians exclude only the first trial.
Every answer matches the DuckDB oracle. These are not promotion runs.

| Runner | Client median ms | Engine median ms | Peak query bytes |
|---|---:|---:|---:|
| before | 3436.060 | 3378.088 | 142875384 |
| packed-v1 | 120.637 | 80.024 | 39064365 |
| profile-v1 | 114.019 | 80.732 | 39064365 |
| ordered-v1 | 100.739 | 68.816 | 39064365 |
| radix-v1 | 91.242 | 57.167 | 39064365 |
| sortkey-v1 | 105.942 | 69.193 | 39064365 |
| gather-v1 | 94.416 | 59.438 | 39064301 |
| json-v1 | 81.278 | 61.714 | 39064301 |

The profile binary adds per-stage timers and is diagnostic only. Its
warm phase medians are approximately: preparation 2.9 ms, dimension
3.7 ms, membership 2.4 ms, fact scan 12.4 ms, distinct sorting 19.6 ms,
and output 37.8 ms. The original uninstrumented runner is retained.

[Design](Q16-DESIGN.md) · [Every iteration trial](Q16-ITERATIONS.json)
