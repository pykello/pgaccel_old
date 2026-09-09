# Q13 predicate preparation costs

A temporary profile on `a8e0f9d` splits PredicateDictionary into filter-node
construction, ExecuteSet matching, and representative/code remapping. Across
2289 stripes, construction takes 56.039 ms, matching 5883.500 ms and remapping
439.647 ms. These phases are sequential within the transformation and exclude
native decoding, other columns and query execution. They do not split literal
matching from dictionary-code bitmap application inside ExecuteSet.

The first call takes 15.158 seconds, warm median is
541.691 ms, and live total peak is
1.792 GiB. All nine typed SF100
answers pass under MemoryMax=8G and MemorySwapMax=0.

Caching node construction could remove at most about 56 ms in this profile;
matching is the dominant measured target. The dictionary label loop repeatedly
fetches dictionary size and logical type through virtual calls, although those
properties are fixed for a stripe. The next experiment hoists these lookups
without changing literal matching, Unicode fallback or row-code mapping.

The [profile patch](predicate-phases-profile.patch) and
[evidence](native-direct-predicate-phases-q13-sf100.json) preserve the frozen
module and raw answer hashes. Timing instrumentation is absent from production
source and the installed module. The complete all-query comparison remains
[TRANSITION-READ-AHEAD.md](TRANSITION-READ-AHEAD.md), which predates the later
string preparation improvements.

## Rejected experiment: hoist dictionary metadata lookups

The candidate caches dictionary size and logical type once at entry to the
dictionary branch, replacing the repeated virtual lookups in dense, sparse
and signature-assisted label matching. It passes all 294 native tests,
Python integration and all 22 SF1 answers without changing matching semantics.

| Query/build | First call s | Warm median ms | Total peak GiB |
|---|---:|---:|---:|
| Q13 control | 15.281 | 542.819 | 1.791 |
| Q13 candidate | 15.175 | 544.855 | 1.791 |
| Q9 control | 16.019 | 1536.688 | 2.759 |
| Q9 candidate | 16.091 | 1555.658 | 2.760 |

Each candidate executes nine calls in a fresh process with MemoryMax=8G and
MemorySwapMax=0. All 18 candidate answers pass. Controls are the preceding
string-guard runs. Q13 first-call time improves only 0.7%, while Q9 becomes
0.5% slower; those small differences do not establish a useful end-to-end
improvement. Revert the candidate and keep the frozen production Python module
at string-guard. Do not repeat runs merely to obtain a favorable sample.

The [patch](label-metadata-experiment.patch),
[Q13 evidence](native-direct-label-metadata-q13-sf100.json),
[Q9 evidence](native-direct-label-metadata-q9-sf100.json), and
[SF1 gate](auto-projection-sf1-label-metadata-gate.json) remain available.
This result does not isolate the virtual calls' CPU cost, nor prove the compiler
already eliminated them. It shows this particular source change does not
sufficiently address the measured matching bottleneck.

Next investigate matching itself, separating literal-byte searches from
row-code bitmap application. If testing parallel label matching, share only
immutable predicate state, partition output words to avoid races, and keep
decoding serial. The rejected concurrent-column-loading experiment combined
I/O, allocation and decoding and must not be repeated under a different name.

## Label matching versus row-code application

A [second profile](native-direct-match-phases-q13-sf100.json) on `2eb2e10`
instruments only dictionaries with at least 8192 labels. Across 2289 Q13
calls to that path, label matching takes 5943.298 ms and applying label bits
to row codes takes 111.983 ms. Small transformed dictionaries used by warm
calls are excluded by this size gate. The earlier combined phase included
both operations; these timings support targeting label matching itself.

The first call takes 15.565 seconds, warm median
546.954 ms, and live peak
1.792 GiB. All nine answers pass
under 8 GiB with zero permitted swap. The [patch](match-phases-profile.patch)
is temporary instrumentation, absent from production source and its module.

The next experiment parallelizes only large-dictionary label matching during
predicate preparation. Decoding remains serial, predicate state is immutable,
and tasks own disjoint bitmap words before a blocking join. The existing
serial and SIMD row-code application paths remain in place.
