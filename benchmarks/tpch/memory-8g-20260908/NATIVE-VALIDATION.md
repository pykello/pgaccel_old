# Native dictionary validation during direct preparation

A temporary loader profile on dense-retention commit `3a395d0` attributes
5.536 seconds of SF100 Q1 preparation to dictionary loading. Code-range
and dictionary-coverage validation account for 2.887 seconds of that total;
dictionary value decoding accounts for 0.088 seconds. These are nested
elapsed measurements, not additive independent phases. Instrumentation
runs once per stripe and is absent from production source.

The instrumented first call takes 12.450 seconds, with a 416.670 ms warm
median. All nine answers pass under the 8 GiB cap with zero permitted swap.
This identifies CPU validation as a useful next target; it does not measure
how much asynchronous input could overlap the remaining loading work.

Reproduction artifacts: [instrumentation](native-validation-profile.patch)
and [profile evidence](native-direct-native-validation-profile-q1-sf100.json).
The evidence records the frozen module hash and raw result hash.

## Retained change: byte usage flags

Commit `a00cb1a` replaces `vector<bool>` dictionary usage flags with bytes.
Each code still gets its range check before indexing the scratch array;
a final scan still rejects declared dictionary entries absent from the
column. A byte store avoids packed-bit read/modify/write bookkeeping.
The scratch array is bounded by the stripe dictionary: at most 64 KiB,
about 56 KiB more than the old packed flags at maximum cardinality.

All 288 native tests, Python integration and all 22 typed SF1 answers pass.
The added test exercises empty, singleton, 255-, 256- and 65536-entry
columns, missing dictionary entries and out-of-range representable codes.

| SF100 query/build | First call s | Warm median ms | Total peak GiB |
|---|---:|---:|---:|
| Q1 dense-retention control | 12.444 | 407.976 | 6.904 |
| Q1 byte flags | 11.465 | 419.480 | 6.904 |
| Q13 dense-retention control | 30.724 | 546.458 | 1.811 |
| Q13 byte flags | 29.776 | 548.904 | 1.866 |

Each run uses nine calls in a fresh process with MemoryMax=8G and
MemorySwapMax=0. Q1 uses the preceding dense-retention control; Q13 uses a
new matching control after the candidate. This is one sample per build,
not a confidence interval. First-call reductions are about 7.9% and 3.1%.
Warm medians are recorded unchanged; the modified loader is not invoked
by reused warm inputs. All 27 new SF100 calls pass their typed oracle.

Evidence: [Q1 candidate](native-direct-byte-usage-q1-sf100.json),
[Q13 candidate](native-direct-byte-usage-q13-sf100.json),
[Q13 control](native-direct-byte-usage-control-q13-sf100.json),
[SF1 gate](auto-projection-sf1-byte-usage-gate.json).

This is a preparation follow-up, not a replacement for the complete
[all-query warm comparison](TRANSITION-CHAINS.md). First calls still take
seconds. Matching the old resident latency for changing SQL within 8 GiB
remains open. Input remains synchronous O_DIRECT with bounded 8 MiB
buffers. An asynchronous reader experiment should measure remaining I/O
wait and charge all in-flight buffers before adopting io_uring.
