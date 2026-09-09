# Remaining work toward the 8 GiB performance goal

The [latest complete sweep](TRANSITION-NARROW.md) passes every answer at
7.681 GiB peak, but Q1, Q11, Q12 and Q22 exceed the saved pgaccel medians.
The subsequent Q22 prefix change has a substantial individual-run margin.
Subsequent Q12 conditional-count preparation reaches 204.854 ms and 1.294 GiB
peak, while scoped arithmetic gathers give Q11 a 28.640 ms individual result.
Keep both measurements. Do not choose faster individual samples to rewrite
a complete sweep or relax the timing threshold to declare completion.

1. **Create and confirm margin in the remaining kernels.** Q1 was 454.791 versus 440.185 ms;
   Q11 was 31.874 versus 30.496 ms; Q12 was 343.101 versus 341.982 ms. Start
   with Q1 phase measurements, then optimize the dominant work. The Q11 profile
   already identifies its grouped fact scan. Keep eight executor workers so
   comparisons do not gain from an unreported concurrency increase. Check
   meaningful nonempty and boundary answers as well as the SF100 timings.

2. **Revalidate the complete workload after retained changes.** Use one frozen
   module, Q1 through Q22 in one Database, nine calls each, an enforced 8 GiB
   total cap and zero swap. Include query transitions, returned Python rows,
   allocator retention and read buffers in the process. Save every answer,
   first-call time, warm sample and live cgroup measurement. Keep a failed or
   slower sweep intact instead of restarting to obtain preferred numbers.

3. **Reduce preparation without hiding it.** Current warm calls reuse
   SQL-specific projected and filtered input. Changing SQL rereads and
   reconstructs that input; the latest first calls take 1.5–38.4 seconds.
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
