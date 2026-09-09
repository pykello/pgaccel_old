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
