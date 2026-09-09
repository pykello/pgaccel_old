# Larger bounded direct reads

`d186cc2` increases the owned buffer per direct reader from 1 MiB to 8 MiB. All reads still use synchronous Linux `O_DIRECT`; there is no buffered payload fallback or asynchronous queue. Scan statistics use the same size constant, including the header reader and each projected column reader. Resident columns and query state remain additional memory.

The [read diagnostic](direct-read-diagnostic-q6-sf100.json) found 4.847 seconds in traced `pread64` calls during an 11.865-second Q6 preparation. Tracing perturbs timings; this is phase evidence, not a promotion run. Untraced sequential reads of the same 4 GiB file prefix took 2.994 seconds at 1 MiB transfers (1.4 GB/s) and 1.664 seconds at 8 MiB (2.6 GB/s). These are actual local filesystem measurements, not the assumed 4 GB/s peak or an asynchronous queue-depth test.

| Query | Earlier first call s | 8 MiB first call s | 8 MiB warm ms | 8 MiB peak GiB |
|---|---:|---:|---:|---:|
| Q6 | 9.598 | 4.857 | 4.318 | 0.192 |
| Q12 | 18.032 | 13.282 | 338.393 | 7.459 |
| Q14 | 11.555 | 6.149 | 105.374 | 0.243 |

Each new query ran in a fresh 8 GiB zero-swap cgroup, with nine typed-oracle-checked answers. The earlier column is the saved isolated 1 MiB sweep; the new measurements follow the subsequent scope-filter and allocator-release changes as well as the transfer-size change. This is not an interleaved experiment isolating only transfer size. The most recent complete single-build transition sweep is still the 1 MiB build in [TRANSITION-SWEEP.md](TRANSITION-SWEEP.md).

Keep the larger transfer size: these first calls improve substantially and Q12, the high-residency case, remains below the cap. All 275 native tests, all 22 SF1 answers and Python integration pass. The reader test crosses the new buffer boundary and checks an unaligned file tail. This does not establish a general internal 8 GiB admission policy or complete the same-performance goal.

Next work: profile the remaining warm regressions in Q2 and Q11, confirm Q14’s improved follow-up in the next complete sweep, and investigate how much read waiting remains after larger transfers. Consider bounded asynchronous prefetch or `io_uring` only against this improved synchronous baseline, with answer checks, first-call time, actual read bytes and total cgroup memory. No new dependency or third-party algorithm was introduced.
