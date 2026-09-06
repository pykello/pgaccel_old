# Rejected constant-multiplier shortcut

The `8aff0be` shortcut parsed a constant multiplier once per correlated
query, then bypassed the general formula evaluator for each aggregate slot.
All 169 native tests and focused sanitizers passed. The nine-trial Q20
client median was 41.629 ms versus 36.952 ms before this experiment.

A controlled ABBA sequence ran 31 Q20 repetitions inside each process.
These are execution-only diagnostic batches, not the client promotion
gate. Exclude only repetition 1 from each median. Each final batch answer
matches the fresh DuckDB oracle; the runner returns only the last answer.

| Run | Version | Warm execution median ms |
|---|---|---:|
| 1 | sum-v12 | 31.762 |
| 2 | sum-v13 | 31.124 |
| 3 | sum-v13 | 32.080 |
| 4 | sum-v12 | 31.086 |

The relative result changes with run order and does not establish a
consistent improvement. Reject the shortcut (`d94bc17`), restoring the
general checked scalar formula evaluator. Do not attribute the initial
full-query slowdown solely to the shortcut.

[Raw diagnostic batches](factor-abba-sf10.json) · [Q20 iterations](Q20-SUM.md)
