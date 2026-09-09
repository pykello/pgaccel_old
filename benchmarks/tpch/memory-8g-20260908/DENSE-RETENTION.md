# Avoid copying nearly complete stripes during preparation

`f9e50ea` lets the query preparer retain a complete original stripe when it
contains at least 16384 rows and its necessary filters retain at least 95% of
them. This is optional physical pruning: the original SQL still evaluates
its predicates on every execution. The selected-load API retains its exact
selection contract. The preparer supplies the complete selection explicitly,
allowing that loader's existing full-row path to retain source buffers.

Dense retention occurs before recording selected rows for date-predicate
transforms, so those transforms see the same row set as the retained columns.
It adds neither an answer cache nor a persistent predicate result. It can
retain extra rows and looser metadata bounds, increasing warm filtering work.
The density threshold alone does not guarantee an 8 GiB working set; actual
memory remains subject to the external total cap and measurement.

All 287 native tests, all 22 SF1 answers and Python integration pass. The new
test checks the threshold, empty selections and the original SUM answer with
retained rows that do not satisfy the predicate.

| SF100 Q1 | First call s | Warm median ms | Total peak GiB |
|---|---:|---:|---:|
| Control | 20.505 | 358.502 | 6.875 |
| Dense retention | 12.444 | 407.976 | 6.904 |

Both sequential fresh-process runs pass all nine typed answers under 8 GiB
with zero swap. Preparation improves by about 39%, while warm execution is
slower and retains less margin below the saved 440.185 ms. Keep this tradeoff
visible; do not substitute the older faster warm sample. This follow-up does
not replace the earlier complete all-query milestone.

Evidence: [control](native-direct-dense-retain-control-q1-sf100.json),
[candidate](native-direct-dense-retain-q1-sf100.json),
[SF1 gate](auto-projection-sf1-dense-retain-gate.json).


## Transition check

The [Q1 → Q21 → Q1 run](native-transitions-dense-retain-sf100.json) passes all
27 answers in one database under 8 GiB with zero swap. Peak total memory is
6.907 GiB, with zero sampled file charge and no memory-limit events.

| Step | Query | First call s | Warm median ms |
|---|---|---:|---:|
| 1 | Q1 | 12.459 | 432.812 |
| 2 | Q21 | 16.640 | 613.062 |
| 3 | Q1 | 12.969 | 420.150 |

These results verify this transition sequence only. They do not replace the
complete 22-query report or establish the unfinished changing-query target.
