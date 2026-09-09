# Q13 string preparation profile

On small-header commit `5d8f644`, the instrumented SF100 Q13 first call takes
28.622 seconds. Orders preparation takes 28.020 seconds: selection is 0.032,
compaction 0.165 and predicate transformation 5.126 seconds. String dictionary
loading totals 15.007 seconds, including 13.205 seconds decoding 149459270
dictionary entries and 0.233 seconds validating dictionary codes.

These are nested elapsed measurements, not independent CPU totals. Decoder
time includes its allocations and any read waits. The remainder of table
preparation includes other column loading, allocation/destruction and scan
bookkeeping; this profile does not separately attribute those costs.

All nine SF100 answers pass under the 8 GiB cap with zero permitted swap.
Warm median is 549.640 ms and live total peak 1.859 GiB. Temporary timing
instrumentation is absent from production source and the installed module.
The [patch](string-preparation-profile.patch) and
[evidence](native-direct-string-prep-profile-q13-sf100.json) preserve the
profile's frozen module hash and raw answer hash.

The next experiment reuses released string dictionary slots between stripes.
It leaves the decoder and predicate evaluator intact, avoiding a new streamed
predicate implementation before measuring simpler allocation reuse. A consumer
that retains a column must prevent reuse of that column's labels.

## Retained change: reuse released dictionary slots

Commit `20c93d6` adds a native-load entry point accepting an owned string
scratch vector. Before decoding a string dictionary, the decoder moves those
slots into the new column and resizes them to the validated dictionary count.
Existing string capacity can satisfy subsequent reads without another heap
allocation. Sorting/uniqueness, lengths, padding, code ranges and dictionary
coverage use the original validation path. New column objects retain the
usual empty auxiliary metadata; no old indexes are carried across stripes.

The direct scanner keeps one scratch vector per projected column. After the
serial consumer returns, it recycles only a string column whose shared_ptr
use count is exactly one. Retained columns keep their original spellings and
code space. Scratch is moved into the next column, not copied, and is released
when the scan ends. String capacity can exceed a later value's length; the
external total-memory measurement includes that retained allocation.

All 293 native tests, Python integration and all 22 SF1 answers pass. Added
tests check retained-column immutability, changing dictionary/string sizes,
and rejection of duplicate labels, missing entries and invalid row codes.

| Query/build | First call s | Warm median ms | Total peak GiB |
|---|---:|---:|---:|
| Q13 control | 27.682 | 547.945 | 1.848 |
| Q13 reused slots | 18.222 | 541.537 | 1.791 |
| Q9 control | 17.403 | 1556.249 | 2.758 |
| Q9 reused slots | 16.344 | 1575.506 | 2.759 |

Each fresh process performs nine calls under MemoryMax=8G and MemorySwapMax=0.
Q13 uses the preceding small-header control; Q9 has a matching fresh control
after the candidate. All 18 candidate answers and nine Q9 control answers
pass. First-call improvements are about 34.2% and 6.1%. These are individual
runs, not confidence intervals; Q9's slightly slower warm sample is preserved.

Evidence: [Q13](native-direct-string-reuse-q13-sf100.json),
[Q9](native-direct-string-reuse-q9-sf100.json),
[Q9 control](native-direct-string-reuse-control-q9-sf100.json),
[SF1 gate](auto-projection-sf1-string-reuse-gate.json).

This follow-up does not replace the complete
[read-ahead sweep](TRANSITION-READ-AHEAD.md). It changes transient preparation
allocation, not the statement-specific residency policy. Q13 still needs
18 seconds on a first call; the broader performance goal remains open.
