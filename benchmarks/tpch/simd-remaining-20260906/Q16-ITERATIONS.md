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
| nokey-v1 | 89.032 | 66.668 | 39064301 |
| profile-v2 | 76.556 | 57.271 | 39064301 |
| narrow-v1 | 75.356 | 55.004 | 39063789 |
| format-v1 | 62.943 | 41.168 | 39063789 |
| localhash-v1 | 66.242 | 45.551 | 39063789 |
| firstrow-v1 | 66.802 | 45.565 | 39063789 |
| orderhelper-v1 | 54.615 | 32.655 | 39063789 |
| duplicatebits-v1 | 52.914 | 31.371 | 39063789 |
| ownedlines-v1 | 61.802 | 39.305 | 39063789 |
| compactin-v1 | 51.858 | 30.197 | 39056047 |
| localcounts-v1 | 51.488 | 30.038 | 39056047 |
| identical-v1 | 49.765 | 29.404 | 39034371 |
| jsonsimd-v1 | 52.870 | 31.912 | 39034371 |
| runs-v1 | 50.898 | 30.327 | 39026435 |

The profile binary adds per-stage timers and is diagnostic only. Its
First-profile warm phase medians: preparation 2.9 ms, dimension
3.7 ms, membership 2.4 ms, fact scan 12.4 ms, distinct sorting 19.6 ms,
and output 37.8 ms. The original uninstrumented runner is retained.
After radix, gathers, buffered JSON, and the sort-key revert, profile v2
shows fact probing around 17.3 ms, distinct 7.5 ms, ordering 10.0 ms,
and formatting 12.2 ms. Reverting the sort-key experiment did not recover
its earlier baseline timing; retain that uncertainty rather than attributing
all timing differences to one sorting algorithm.

[Design](Q16-DESIGN.md) · [Every iteration trial](Q16-ITERATIONS.json)
