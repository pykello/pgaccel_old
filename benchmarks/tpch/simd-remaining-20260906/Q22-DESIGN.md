# Q22: prefix labels and a filtered anti-join bitmap

The implementation is at `a7bccf3`; performance confirmation is pending. The acceptance
criterion is a 2x warm-median speedup over both engines at both scales.

The query selects customer phone prefixes, compares account balances with
an uncorrelated filtered average, excludes customers having orders, and
groups by the selected phone prefix. Repeated substring evaluation and
ordinary join/hash state can be replaced with small exact domains.

Execution:

1. Recognize a bounded literal prefix set and prove the outer and scalar
   substring expressions refer to the same physical customer column. Map
   each stripe's phone dictionary values to exact selected-prefix labels.
   Preserve SUBSTRING's UTF-8 semantics and validation, including strings
   shorter than the requested slice. Do not compare unrelated local IDs.
2. Evaluate the scalar average with its actual filters. Use the same wide
   sum, count, mean formatting and scalar conversion as the general
   executor. Replacing the threshold with an unrounded rational inequality
   would change boundary behavior and is not automatically equivalent.
3. Mark candidate customer keys only after prefix and balance filtering.
   Prove the integer key domain is suitable for a bitmap; duplicate keys
   require preserved semantics or fallback.
4. Scan order foreign keys against candidate membership. Use private hit
   bitmaps per worker and merge them, avoiding shared random atomic writes
   for every order. Retain every candidate with no hit.
5. Accumulate counts and exact account-balance sums by prefix label and use
   the existing output sorter and formatter.

Reference regressions cover repeated orders, absent customers, missing
foreign keys, empty scalar-average input, rounding-boundary balances,
differing dictionaries, short and non-ASCII phone values, and additional
predicate rejection. All scans, dictionary translations, bitmap allocation,
merging and result conversion must remain within the timed query call.

All 256 native tests pass. The fine-decimal regression uses balances at
scale 13 whose average rounds to zero at the scalar result's declared
scale 12. The optimized path keeps all three positive customers, matching
the general executor; a direct rational threshold would incorrectly drop
two. Formatting uses the existing 17-significant-digit mean formatter and
ResultNumber conversion rather than an independently invented rule.

The implementation uses existing project components and original code. No
new dependency or externally licensed algorithm implementation is added.
