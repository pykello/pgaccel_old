# Experiments 2 and 3: compact distinct and inline state — keep

The original integer set uses open addressing over uint64 slots, a separate
zero flag, and transactional replacement allocation. It removes per-value
Cell objects and node allocations. Hash ownership uses high hash bits so it
does not constrain the low bits used by local table addressing. The query
budget covers both old and replacement arrays during growth. A forced growth
failure leaves existing entries readable and releases all memory afterward.

The independent composite change stores arrays of up to four NumericState
objects inside each map entry. It removes one heap allocation per group while
retaining the original numeric semantics and the dynamic fallback for larger
aggregate lists. This change affects Q31–Q33, not Q5's distinct path.

## Full-data repeated results

Eight threads, full fixture, three complete client requests per query. Warm
values below are min(trials 2,3); each of 15 answers passed the subset gate.
Q32/Q33 preserve raw tied-output differences and pass independent refinements.
All 35 native tests pass, including failure safety and all inline arities.

| Query | Previous warm ms | Experiment warm ms | Speedup | Decision |
| --- | ---: | ---: | ---: | --- |
| Q5 | 9029.440 | 2463.171 | 3.67× | Keep original flat integer set |
| Q6 | 3103.989 | 3154.953 | 0.98× | Unchanged string path; control |
| Q31 | 5458.593 | 4064.579 | 1.34× | Keep inline numeric state |
| Q32 | 10255.323 | 7461.196 | 1.37× | Keep inline numeric state |
| Q33 | 122262.240 | 61299.439 | 1.99× | Keep inline numeric state |

The previous baseline is the preserved pre-research three-trial run. It is not
an interleaved A/B measurement; later acceptance will rerun both full engines.
These queries still lose to ClickHouse. The changes reduce the gap, not satisfy
the final stopping target.

Raw records and every trial's validation:
`../docs/clickbench/research-20260905/flat-and-inline.jsonl` and
`../docs/clickbench/research-20260905/flat-and-inline-gate.json`. The binary and metadata record source/hash identity.

## Profiling context

A cycles profile of baseline Q33, including native load, attributed 22.32% of
samples to `Budget::charge` and 7.49% to its deallocation function. There were
no lost samples. Hash insertion and allocator routines also featured heavily.
This supports removing per-entry allocation rather than weakening the budget.
The sample run is diagnostic, not a reported warm benchmark.

An initial profile accidentally used the default 8 GiB query budget. It was
aborted and retained with `default-budget-aborted` in its name. The completed
profile explicitly used 32 GiB, matching the benchmark configuration. Raw
profile data is in `~/projects/clickbench-work/research-20260905/`; the text
report and runner output are in the external documentation directory.
