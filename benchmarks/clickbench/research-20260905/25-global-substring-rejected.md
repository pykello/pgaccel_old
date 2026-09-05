# Experiment 25: global per-query substring bitmap — rejected

Reject the global preparation path. It evaluated the exact substring
predicate once per global string label, using the existing conservative
signature as a prefilter. A query-local bitmap then supplied membership
to each stripe through its local-to-global label mapping. Its allocation
was charged to the query budget and rebuilt inside every timed query.

Preparation happened before stripe tasks, using the same eight-worker
arena; no nested initialization barrier or cross-query cache was used.
Although this reduces repeated label evaluation in principle, it does
not improve this workload's observed latency.

## Full-data comparison

Original SQL, eight threads, experimental indexed configuration.
Minimum client milliseconds of trials two and three.

| Query | Per-stripe signatures | Global prepared bitmap | ClickHouse |
| ---: | ---: | ---: | ---: |
| 21 | 161.782 | 231.511 | 336.286 |
| 22 | 168.190 | 235.275 | 96.774 |
| 23 | 270.440 | 340.208 | 537.647 |
| 24 | 163.205 | 232.786 | 96.147 |

Every measured query regresses. The global label pass and subsequent
mapping add observable work; this experiment does not isolate which
memory-access or scheduling cost dominates, so no detailed causal claim
is made without profiling.

Implementation commit a11c372 is preserved in history. Commit cb30e83
removes the preparation path, its unused API, and the filtered-count
wrapper, restoring per-stripe exact signature checks. No dependency was
added. The radix grouping improvement from the same binary is retained.

All 24 combined-run answers validate. Preparation-specific tests had
covered different patterns, conjunctions, failed allocation, and independent
stripe code spaces. Correctness passed; performance determined rejection.

Raw evidence and frozen binary are shared with experiment 24:
prepared-radix.jsonl, prepared-radix.meta.json, prepared-radix-gate.json,
and ../clickbench-work/research-20260905/bin/prepared-radix-query-runner.
