# Parallel matching of large predicate dictionaries

The [matching profile](PREDICATE-PREPARATION.md) attributes 5.943 seconds to
Q13 label matching and only 0.112 seconds to applying label bits to row codes.
The experiment therefore parallelizes label matching while decoding remains
serial. It uses the existing eight-worker arena and no additional dependency.

Tasks own disjoint 32-bit output words in the existing 8 KiB label bitmap.
Task boundaries are rounded to cache-line-sized word groups; a blocking join
completes every read before the caller applies row codes or reuses source
string allocations. Predicate state is read-only. Existing prefix, sparse
and signature paths retain their behavior, and small dictionaries stay serial.
Parallelism is enabled only by the direct preparer's existing parallel flag.

## Broader experiment

The first candidate enables this for the remaining dense value-predicate
path when the dictionary contains at least 8192 labels. All 295 native tests,
Python integration, all 22 SF1 answers and all 18 SF100 answers pass.

| Query/build | First call s | Warm median ms | Total peak GiB |
|---|---:|---:|---:|
| Q13 control | 15.281 | 542.819 | 1.791 |
| Q13 broad parallel | 13.139 | 548.065 | 1.792 |
| Q9 control | 16.019 | 1536.688 | 2.759 |
| Q9 broad parallel | 16.332 | 1564.377 | 2.758 |

Each fresh process executes nine calls under MemoryMax=8G and MemorySwapMax=0.
Controls are the preceding string-guard runs. Q13 improves about 14.0%, but
Q9 becomes about 2.0% slower. These are individual runs, not confidence
intervals. Preserve the slower Q9 measurement and narrow the dispatch to
ordered multi-literal LIKE sequences, the measured expensive pattern class.
Single-substring and membership predicates retain the previous serial path.

The broader implementation is saved as a
[patch](parallel-labels-general-experiment.patch), with
[Q13](native-direct-parallel-labels-q13-sf100.json),
[Q9](native-direct-parallel-labels-q9-sf100.json), and
[SF1 gate](auto-projection-sf1-parallel-labels-gate.json).
It is not the final dispatch policy; its timings must not be substituted for
the narrowed build's results.
