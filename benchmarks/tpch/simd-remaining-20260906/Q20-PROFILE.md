# Q20: removing intermediate representation costs

The diagnostic sum-v1 build separates the child from its enclosing
membership query. These are instrumented SF10 observations, not benchmark
results. Nested timings overlap and must not be added together.

| Child phase | Approximate warm ms |
|---|---:|
| Filtered source and membership domain | 13.4 |
| Outer tuple directory and row retention | 19.0 |
| Parallel fact scan and SUM | 16.3 |
| Merge and scalar formula | 6.8 |
| Project intermediate rows | 13.2 |
| Format intermediate strings | 8.0 |

The parent membership phase takes about 98 ms, including the child. Its
additional conversion and hash construction account for roughly 21 ms.
The full parent result contains supplier strings, but the large child
result is only integer supplier keys. Formatting that child into strings
and parsing them back cannot help the join.

The first retained step constructs exact numeric membership bitmaps
directly, reducing the measured Q20 median from 106.690 to 96.128 ms.
The next experiment lets an internal membership consumer request a typed
numeric column. It must preserve ordering and limit semantics through a
fallback, and account for retained output after the child resource dies.
Client output remains formatted through the ordinary result path.

If that is insufficient, measure directory construction and merge again.
Candidates include pre-sizing the tuple directory after parallel row
selection, parallelizing disjoint aggregate-slot reductions, and a
primary-key rank directory with short secondary-key lists. The last
candidate needs a measured fanout bound and exact fallback for arbitrary
keys and duplicates. None of these candidates is a retained optimization
until correctness and performance measurements support it.

SIMD width alone does not establish which join wins. Balkesen et al.
compare tuned sort and hash joins and report that radix hash remains
competitive despite wider SIMD; their experiments motivate measuring
partitioning, cache behavior and thread placement together. Applying that
lesson to this small filtered tuple directory is a local inference, not a
claim from their experiment. [Primary paper](https://www.vldb.org/pvldb/vol7/p85-balkesen.pdf).

All implementations here are original changes to existing project code.
No implementation from the research paper is imported.

## After parallel outer filtering

The instrumented sum-v8 checkpoint uses nine SF10 trials. These phase
medians exclude trial 1 and are diagnostic only; they are not promotion
timings. Parent membership time includes this child and must not be added
to its phases.

| Child phase | Diagnostic warm median ms |
|---|---:|
| membership | 6.862 |
| outer-filter | 3.008 |
| outer-index | 7.245 |
| scan | 20.456 |
| merge | 3.319 |
| project | 2.581 |
| output | 0.009 |

Fact scanning remains the largest phase. The next experiment halves
per-worker sum-state width only after bounding positive and negative
contributions separately. Bounding only the final total would miss
intermediate overflow during cancellation. Unproven ranges retain checked
128-bit accumulation, and merged formula thresholds remain 128-bit.
