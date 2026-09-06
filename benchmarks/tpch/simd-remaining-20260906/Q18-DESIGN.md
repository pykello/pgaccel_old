# Q18 sparse order selection

Status: [fresh confirmation](Q18.md) passes at SF10 and SF100 after the
contiguous-decode change. Q3 also passes again on the same frozen binary.

Q18 first selects order keys whose lineitem quantity sum exceeds its HAVING
threshold, then joins orders and customers and returns the largest orders.
The new execution composes four reusable operations:

1. `2630d42` rejects completed interior sorted SUM groups as they are scanned.
   First and last groups remain until stripe boundaries are merged, because
   rejected partial sums can form a qualifying complete group. This avoids
   retaining every order group merely to discard most of them afterward.
2. `5f83039` proves strict ordering and uniqueness of raw integer bridge
   keys. Adjacent comparisons use AVX-512 where enabled; a stripe directory
   supports binary searches despite gaps and physical stripe reordering.
   Storage is proportional to stripes, without persistent indexes.
3. `d0f75dd` projects customer attributes through the verified dimension key.
   A unique bridge grouping key must determine the output group. String and
   date values use ordinary result ordering and slicing.
4. `0b82b83` executes one positive integer membership child under clamped
   memory and spill limits. It deduplicates keys and looks up only selected
   bridge rows, then uses the existing rank bitmap to filter fact rows and
   aggregate repeated matches. Dense domains and unsupported layouts fall
   back to general execution.

For example, order keys 2 and 18 can be found in separate shuffled stripes
without allocating row mappings for all values between them. Repeated
lineitem rows still contribute repeatedly to SUM; repeated membership keys
select each order only once. Duplicate bridge primary keys invalidate the
proof and preserve general join multiplicity through fallback.

All 189 native tests, 12 focused sanitizer tests and all 22 SF1 answer
comparisons pass at `0b82b83`. Tests cover signed key widths, gaps, stripe ordering, invalid
uniqueness proofs, dimension projection, fact multiplicity, decimal sums,
empty membership, slicing and strict child memory limits.

Next: validate all 22 SF1 answers, measure 15 sequential trials per engine
at SF10, and proceed to nine SF100 trials only after the fresh fourfold gate
passes. Retain all trials and compare warm client medians excluding only
the first trial. Recheck Q3 because it shares the star aggregation path.

Implementation is original, using existing project primitives and no new
dependencies or licensed third-party algorithm implementation.

## First SF10 confirmation

The frozen `0b82b83` runner returned correct answers in all 15 trials.
Warm client medians: PGAccel 121.441 ms, DuckDB 373.561 ms, ClickHouse
750.649 ms. This is 3.08x and 6.18x respectively: the fourfold gate fails,
so SF100 was not run. PGAccel query peak was 12,493,860 bytes.

A separate instrumented diagnostic run attributed warm median 94.152 ms
to the membership child, 0.499 ms to dimension preparation, 1.117 ms to
bridge selection, 1.953 ms to fact preparation, 6.291 ms to fact scan and
0.682 ms to output. Instrumented phases are diagnostic and are not the
client-runtime promotion measurement. All nine diagnostic answers match.

`537c8aa` now decodes contiguous blocks for unfiltered sorted sums. Native
tests and ten focused sanitizer tests pass. The clean-runner iteration
measured 75.320 ms, with all nine answers correct. The subsequent fresh
15-trial SF10 and nine-trial SF100 confirmation passes both scales.

[Compact iteration evidence](Q18-ITERATIONS.json) preserves trial data and
raw-answer hashes. All 22 SF1 answers also pass on final `537c8aa`.
