# Remaining work toward the 8 GiB performance goal

The [latest complete sweep](TRANSITION-DENSE.md) passes all 198 answers at
6.930 GiB peak. Q11 alone exceeds the saved pgaccel warm median: 30.761 versus
30.496 ms. Its faster individual results remain separate evidence. Q1, Q12
and Q22 now have substantial margin in the complete workload.

1. **Improve preparation and preserve execution margin.** The
   [phase profile](PREPARATION-PROFILE.md) measures Q1 compaction at about
   20 seconds and its total pread wall time at 2.9 seconds. Bounded parallel
   column copies reduce first-call time modestly to 25.615 seconds, with
   6.869 GiB peak in the isolated run. Keep selectors and transformations
   serial because callbacks may share mutable state. Next, investigate why
   compaction still costs so much: copying selected values, recomputing
   bounds, dictionary pruning and allocation need separate measurements.
   The repeated selection validation is another candidate, with source bounds
   checks preserved. Q13 has only 0.164 seconds of compaction; investigate its
   native dictionary loading and 5.254-second transformation separately. Its
   loader currently copies each temporary string into a growing dictionary;
   direct construction in reserved storage is a concrete allocation candidate.
   Q11's grouped fact scan remains the measured warm bottleneck. Preserve the
   eight-worker limit and do not select favorable repeats or relax thresholds.

2. **Revalidate the complete workload after retained changes.** Use one frozen
   module, Q1 through Q22 in one Database, nine calls each, an enforced 8 GiB
   total cap and zero swap. Include query transitions, returned Python rows,
   allocator retention and read buffers in the process. Save every answer,
   first-call time, warm sample and live cgroup measurement. Keep a failed or
   slower sweep intact instead of restarting to obtain preferred numbers.

3. **Reduce preparation without hiding it.** Current warm calls reuse
   SQL-specific projected and filtered input. Changing SQL rereads and
   reconstructs that input; the latest first calls take 1.5–38.3 seconds.
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
