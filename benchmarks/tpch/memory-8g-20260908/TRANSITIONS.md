# SF100 query transitions under 8 GiB

One native Python Database executed Q20 → Q1 → Q9 → Q1 → Q20. Each step made nine calls; changing SQL reread and prepared its columns, while the next eight calls reused that statement-specific input. Both versions passed all 45 typed-oracle checks under an 8 GiB total cgroup cap with zero swap. This is a representative transition test, not the complete 22-query sequence.

`ccfb9ac` calls glibc `malloc_trim(0)` after releasing the previous statement’s tables. Release time is included in preparation. Identical SQL does not trim or reread. This returns unused arena pages; it does not establish an internal admission budget for arbitrary queries. The call affects free arenas process-wide and can increase subsequent allocation costs. Other allocators do not use this glibc-specific release step.

| Step | Query | Before: resident GiB | After: resident GiB | Before: first s | After: first s | Before: warm ms | After: warm ms |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | Q20 | 7.856 | 7.844 | 21.222 | 21.351 | 242.556 | 226.618 |
| 2 | Q1 | 7.880 | 6.889 | 26.790 | 28.518 | 424.585 | 439.382 |
| 3 | Q9 | 7.879 | 2.789 | 29.150 | 31.247 | 1406.582 | 1407.730 |
| 4 | Q1 | 7.879 | 6.855 | 26.554 | 28.198 | 427.155 | 449.908 |
| 5 | Q20 | 7.880 | 7.845 | 14.757 | 22.162 | 213.418 | 225.054 |

Resident columns are live cgroup `memory.current` after the ninth call, including retained allocator pages and kernel overhead. Cumulative peak was 7.880 GiB before and 7.858 GiB after. Every recorded memory sample in the new run has zero file-cache bytes and no memory-limit/OOM events. Payload reads use `O_DIRECT`; metadata still uses ordinary small reads.

Returning free pages recovered about 5.09 GiB at the Q9 step. It did not improve every warm or first-call timing. In particular, the last Q20 reload rose from 14.757 to 22.162 seconds. These are sequential runs, not a controlled estimate of the isolated trim cost. Keep this change for released residency, not as a demonstrated speedup.

The overall performance goal remains open. First calls still take seconds, the data is prepared specifically for the SQL, and a complete query-transition sweep and the remaining warm regressions need work. `io_uring` has not been added; it cannot by itself remove decoding, validation, filtering and compaction costs.

Evidence: [before](native-transitions-common-filter-sf100.json), [after](native-transitions-trim-sf100.json), [harness](run_direct_transitions.py). Records include module and SQL hashes, all timing samples, live memory limits, answer hashes, and full-answer evidence locations.
