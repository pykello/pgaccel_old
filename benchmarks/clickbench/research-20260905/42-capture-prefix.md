# Experiment 42: avoid an unrestricted regex suffix on ASCII input

Implementation: `f489dcf`. Q29's prior CPU profile attributed 82.52% of cycles
to RE2. The grouping regex captures a prefix component and discards the rest
of the string. A restricted generic rewrite recognizes `^P.*$` with dot-newline
enabled and whole-input replacement by one numbered capture from P.

On ASCII input, the trailing `.*$` imposes no additional constraint. Matching
P against the full input preserves its greedy and lazy capture choices while
avoiding the discarded suffix. The executor returns a view into the original
input and retains any owner. Non-ASCII inputs, other replacements, top-level
alternation, inline flags, quoted regex regions and unsupported class syntax
use the original RE2 path. No URL or benchmark column name is recognized.

The conservative ASCII restriction also preserves invalid UTF-8 behavior:
RE2's dot is rune-aware, so blindly discarding the suffix on arbitrary bytes
could incorrectly accept an input the original regex rejects.

| Q29 warm client measurement | ms |
| --- | ---: |
| Weighted executor before this change | 5550.624 |
| Restricted capture-prefix evaluation | 4361.307 |
| Previous complete ClickHouse checkpoint | 1948.511 |

Q29 trials are 4368.915, 4361.777 and 4361.307 ms. All 15 trial answers for
Q21, Q22, Q24, Q28 and Q29 pass the gate. All 90 native tests pass. Tests
compare output directly with RE2 replacement across nine pattern families,
random inputs, greedy/lazy and optional captures, non-ASCII and invalid bytes,
NUL/newline input, later capture numbers and owned intermediate strings.

Decision: keep, while recording that Q29 still loses. No new dependency was
introduced. The existing RE2 implementation remains the fallback and reference.
[RE2's primary implementation](https://github.com/google/re2/blob/main/re2/re2.cc)
was inspected for context; no implementation code was copied.

Raw artifacts: `pg-capture-prefix.jsonl`, its metadata and gate. After all
saved timing trials, a separate Q21 warmup and 40 profiled executions collected
the `.perf.data` and `.perf.txt` artifacts. That Q21 profile identifies libc
string routines and ValuePredicate as the remaining substring-filter work;
its samples are excluded from benchmark latency.
