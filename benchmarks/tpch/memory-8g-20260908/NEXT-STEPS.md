# Remaining work toward the 8 GiB performance goal

The [latest complete sweep](TRANSITION-STRINGS.md) passes all 198 answers at
6.954 GiB peak. **Every prepared-input warm median meets its saved pgaccel
time.** Preserve that complete milestone; do not rerun to improve its numbers.
All 295 tests, 22 SF1 answers and Python integration pass for this module.

The original fully resident database and this statement-specific working set
have different preparation behavior. New SQL still releases old input and
performs direct reads, filtering and compaction. First calls take 1.5–33.0
seconds historically; the latest sweep reduces this to 1.191–28.888 seconds.
Warm benchmark success does not prove the same latency when changing
queries or a general-purpose 8 GiB working set. That remains the next work,
with any later implementation checked against the preserved warm baseline.

[Dense retention](DENSE-RETENTION.md) now retains original stripe buffers
when a necessary filter discards very few rows. Q1 first-call time improves
from 20.505 to 12.444 seconds, with a warm-time increase to 407.976 ms.
The Q1 → Q21 → Q1 transition check passes all 27 answers at 6.907 GiB.
[Byte dictionary flags](NATIVE-VALIDATION.md) further reduce Q1 first-call
time to 11.465 seconds and Q13 to 29.776 seconds. All 288 native tests,
Python integration and all 22 SF1 answers pass for the retained build.

[Concurrent column loading](PARALLEL-LOAD-EXPERIMENT.md) was tested and
rejected: Q1 improves only to 11.240 seconds while Q13 regresses to 76.008
seconds. Its source and test patch are preserved, not enabled. Keep decoding
on the caller in a future bounded asynchronous-read experiment; this result
does not isolate I/O from allocator and scheduling effects.

1. **Improve preparation and preserve execution margin.** The
   [phase profile](PREPARATION-PROFILE.md) measures Q1 compaction at about
   20 seconds and its total pread wall time at 2.9 seconds. Bounded parallel
   column copies reduce first-call time modestly to 25.615 seconds, with
   6.869 GiB peak in the isolated run. Keep selectors and transformations
   serial because callbacks may share mutable state. The subsequent [per-column profile](COMPACTION-PROFILE.md) identifies
   dictionary compaction as dominant. Resolving virtual code-width lookups
   once reduces Q1 first-call time to 18.298 seconds. Numeric gathering,
   bounds recomputation, dictionary pruning and allocation still need work.
   The repeated selection validation is another candidate, with source bounds
   checks preserved. Q13 has only 0.164 seconds of compaction; investigate its
   native dictionary loading and 5.254-second transformation separately. Direct construction in final dictionary slots now reduces Q13
   first-call time from 35.907 to 30.708 seconds in a sequential control and
   candidate comparison. Further loading and transformation costs remain.
   Q11's grouped fact scan remains the measured warm bottleneck. The
   [adaptive HAVING implementation](HAVING-PRUNING-PLAN.md) reduces Q11
   query-state peak from 49.4 to 1.68 MiB, with paired warm times of 28.571
   and 28.181 ms. The complete sweep now confirms Q11 at 29.873 ms. Preserve the
   eight-worker limit and do not select favorable repeats or relax thresholds.

2. **Revalidate the complete workload after retained changes.** Use one frozen
   module, Q1 through Q22 in one Database, nine calls each, an enforced 8 GiB
   total cap and zero swap. Include query transitions, returned Python rows,
   allocator retention and read buffers in the process. Save every answer,
   first-call time, warm sample and live cgroup measurement. Keep a failed or
   slower sweep intact instead of restarting to obtain preferred numbers.

3. **Reduce preparation without hiding it.** Current warm calls reuse
   SQL-specific projected and filtered input. Changing SQL rereads and
   reconstructs that input; the latest first calls take 1.5–32.6 seconds.
   Investigate which retained physical columns or immutable blocks can be
   reused safely across different queries, with explicit byte admission and
   eviction. A representation transformed for one query cannot be treated as
   a general replacement column. Avoid admitting old and new working sets
   together beyond the total limit, and include cleanup in client latency.

4. **Measure asynchronous I/O against the improved synchronous reader.**
   [Bounded read-ahead](READ-AHEAD.md) now improves Q1 first-call latency
   from 11.465 to 8.626 seconds and Q13 from 29.776 to 28.727 seconds.
   It keeps serial decoding and uses two 8 MiB buffers per stream, one
   pending background pread, with charged memory and joined cleanup.
   The Q1 → Q21 → Q1 transition check passes all 27 answers below 8 GiB.
   The complete post-change sweep passes all 198 answers at 6.968 GiB.
   Measure remaining
   wait before replacing thread-per-read overhead with a persistent worker
   or io_uring. Keep O_DIRECT, count speculative bytes and charge every
   in-flight buffer; no payload page-cache fallback is permitted.

The same-performance goal remains open. The full report distinguishes warm
reuse from changing-query execution; arbitrary-workload admission and spill
behavior are also not yet a proved property of this experimental loader.

## Header-read follow-up completed

[Small header buffers](SMALL-HEADERS.md) now use 4 KiB without read-ahead
for schema identity validation, retaining paired 8 MiB payload buffers.
All 291 tests and 22 SF1 answers pass. A matching Q2 control improves from
1.783 to 1.475 seconds; Q13 improves from 28.727 to 27.682 seconds, while
Q1 is slightly slower in its preserved sample. Peak candidate memory across
those queries is 6.953 GiB. The prior complete sweep remains the latest
all-query measurement, predating this follow-up.

## Next preparation target

[String allocation reuse and stream setup](STRING-PREPARATION.md) now reduce
Q13 first-call time from 27.682 to 18.222 and then 15.281 seconds. Q9 improves
from 17.403 to 16.344 and then 16.019 seconds. All 294 native tests, Python
integration and all 22 SF1 answers pass. The post-allocation profile measures
7.897 seconds decoding 149 million labels and 6.406 seconds in predicate
transformation; it predates the later stream-guard change.

The [predicate phase profile](PREDICATE-PREPARATION.md) now separates this:
filter construction is 56 ms, matching 5884 ms and remapping 440 ms across
2289 stripes. Caching construction cannot address most of the cost. Hoisting
size/type metadata lookups was tested and reverted: Q13 improves only 0.7%
while Q9 becomes 0.5% slower in the recorded samples.

A second profile attributes 5.943 seconds to label matching and only 0.112
seconds to row-code application. [Parallel ordered-literal matching](PARALLEL-LABELS.md)
now reduces Q13 first-call time to 13.019 seconds. It uses eight workers and
word-disjoint label bitmaps for large ordered multi-literal patterns only;
decoding remains serial. Broader parallel matching slightly regressed Q9 and
was narrowed. All 295 tests, Python integration and 22 SF1 answers pass.

**The complete frozen-build sweep now passes all 198 answers at 6.954 GiB.**
All warm medians meet the original saved pgaccel times. Combined first calls
fall from 168.749 to 146.768 seconds versus the previous complete sweep.
Use [the input-volume gap](READ-VOLUME-GAP.md) to guide the next investigation:
inventory original per-query column inputs and distinguish reusable snapshot
data from query-specific transformations. Measure lower-transfer formats or
safe byte-admitted cross-query reuse before returning to small loop changes.

Q13 still prepares predicate-only strings on every changed statement.
`PredicateOnlyColumns` proves when every use observes the same predicate,
and `PredicateDictionary` later replaces the loaded dictionary with at most
two representative strings. The loader currently constructs the complete
string dictionary first. Investigate applying that proven transformation
while decoding: preserve sorted/unique dictionary validation and code-range
and coverage checks, but retain only the representatives and a code map.
Do not discard original spellings when any scope observes values, order or
identity. Match the existing literal-filter semantics, including negation
and escaping, and leave original SQL execution intact.

Profile before choosing the implementation. A streamed transformation must
preserve early errors, stripe lifetime, and the full source validation
contract; avoiding allocations cannot justify accepting malformed snapshots.
Measure Q13 end-to-end and a second predicate-only query, then verify the
all-query correctness gate and total memory. This is preparation work;
statement-specific transformed columns still do not provide cross-query
residency. Internal byte admission and safe reuse remain separate open work.

## Original-column inventory completed

[INPUT-INVENTORY.md](INPUT-INVENTORY.md) now records all 22 parser-bound
requirements and original native spans. SF1 and SF100 dependency/proof sets
agree. The required union is 31.381 GiB on disk; 17 repeatedly used columns
smaller than 64 MiB each total 283.619 MiB. These are not decoded-memory sizes
or proof that selected/compact outputs can be reused.

The next implementation is a byte-admitted cache of original snapshot column
blocks, beginning with small repeated dimensions. Establish owned-memory
accounting and snapshot identity first, then supply hits before selection and
transformation in the scan/load paths. Do not cache statement-specific prepared
tables. Test retention, eviction, identity changes and cross-query row alignment,
then measure actual bytes avoided and total 8 GiB memory on SQL transitions.

Also investigate a compact/searchable comments representation: o_comment is
7.589 GiB and appears only in Q13. A small-dimension cache cannot remove that
query's dominant first-use input cost. Keep index verification, decompression,
and original query execution inside the appropriate measured boundary.

## Cache admission measured

[COLUMN-ADMISSION.md](COLUMN-ADMISSION.md) now provides owned native-block
charging and a simultaneous-retention measurement of all 17 candidates.
Their block charges total 479.175 MiB, with 32 KiB of retained pointers and
485.645 MiB total cgroup peak. All 296 native tests pass. The charge is not
RSS or a provenance proof; the loader must supply unmodified owned blocks,
and cache metadata and active query references require separate accounting.

Use 512 MiB as the prototype cache admission budget, then implement versioned
snapshot identity and entry ownership/eviction before supplying cache hits to
scans. Decline unsupported auxiliary ownership. Verify eviction while a query
still holds references and test snapshot changes; measure real transitions
before claiming the combined cache/query process remains within 8 GiB.
The cache is not yet active in query execution.

## Cache identity and eviction implemented

[CACHE-OWNERSHIP.md](CACHE-OWNERSHIP.md) completes the identity/ownership
foundation and its tests. Connect original-block reuse before selection and
transformation next. Variable-length stripes require retained end offsets
or whole-column reuse; separately opened payload descriptors must agree with
the validated identity. Keep cached sources immutable, expose hit/read counts,
and measure combined query/cache memory on transitions. No runtime cache is
active yet; the SF100 report is unchanged.
