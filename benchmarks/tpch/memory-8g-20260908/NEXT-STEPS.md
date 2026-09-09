# Remaining work toward the 8 GiB performance goal

The [latest complete sweep](TRANSITION-CHAINS.md) passes all 198 answers at
7.060 GiB peak. **Every prepared-input warm median meets its saved pgaccel
time.** Preserve that complete milestone; do not rerun to improve its numbers.
All 286 tests, 22 SF1 answers and Python integration pass for this module.

The original fully resident database and this statement-specific working set
have different preparation behavior. New SQL still releases old input and
performs direct reads, filtering and compaction. First calls take 1.5–33.0
seconds. Warm benchmark success does not prove the same latency when changing
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
   Larger 8 MiB transfers already improve cold preparation. Determine remaining
   read-wait time separately from decoding, validation and compaction. A
   bounded prefetch or `io_uring` experiment must account for all submitted
   buffers, read amplification, completion errors and early-stop cleanup.
   Keep `O_DIRECT`; a synchronous fallback must not become a buffered payload
   fallback. Retain an asynchronous implementation only if complete end-to-end
   measurements justify its complexity and memory. No new dependency is
   currently required or installed for this step.

The same-performance goal remains open. The full report distinguishes warm
reuse from changing-query execution; arbitrary-workload admission and spill
behavior are also not yet a proved property of this experimental loader.
