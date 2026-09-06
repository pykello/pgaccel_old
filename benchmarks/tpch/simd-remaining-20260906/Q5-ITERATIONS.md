# Q5 SF10 iterations

Native Python complete-call measurements. Nine trials; only trial one is
excluded from warm medians. These are iterations, not fresh promotion gates.

| Checkpoint | Client ms | Engine ms | Peak query bytes |
|---|---:|---:|---:|
| before-native | 11467.258 | 11467.097 | 855638064 |
| convergent-v1 | 57.431 | 57.276 | 29497912 |
| profile-v1 | 56.964 | 56.800 | 29497912 |
| profile-v2 | 57.029 | 56.891 | 29497912 |
| rootbits-v1 | 46.824 | 46.677 | 29310392 |
| narrow-v1 | 40.366 | 40.222 | 24810302 |
| q07-simd-v1 | 41.665 | 41.508 | 24810311 |

The first profile identifies roughly 11.2 ms building customer payloads,
20.8 ms building selected order payloads, and 20.5 ms scanning fact rows.
Key proof is around 1.8 ms and output is below 0.1 ms. Profile modules add
diagnostic timers and are not eligible for promotion. The clean source is
restored before building retained checkpoints.
The finer profile attributes 3.0 ms to customer allocation, 3.0 ms to
customer propagation, and 4.6 ms to an unused customer membership bitmap.
Orders take 4.0 ms allocating, 12.3 ms propagating, 3.4 ms finalizing rank,
and 1.2 ms scattering payloads. The unused bitmap is removed in rootbits-v1.
Timing differences exceed that one measured phase, so do not attribute the
entire iteration change to one loop.

[Design](Q5-DESIGN.md) · [All trial diagnostics](Q5-ITERATIONS.json)
