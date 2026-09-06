# Rejected prefix dictionary-code interval experiment

The `d712eb2` experiment compared 32 dictionary codes directly when exact
regex checks accepted a complete prefix interval. It preserved UTF-8 holes
through fallback and passed all 169 native tests plus focused sanitizers.

Its nine-trial Q20 client median was 54.245 ms, versus 49.631 ms before
the change. Unchanged Q17 also slowed, so that run alone did not establish
a causal regression. A separate ABBA component check followed.

Load the same SF10 part table for each process. Execute 51 repeats of each
count query, excluding only repeat 1 from execution-time medians. This
isolates prefix filtering and is **not** the client-time promotion gate.
Final batch answers match independently computed DuckDB counts; the runner
returns only the last result of each batch. Preserve every elapsed sample.

| Run | Version | LIKE median ms | NOT LIKE median ms |
|---|---|---:|---:|
| 1 | sum-v9 | 3.656 | 3.806 |
| 2 | sum-v10 | 3.687 | 3.602 |
| 3 | sum-v10 | 3.555 | 3.477 |
| 4 | sum-v9 | 3.469 | 3.405 |

There is no consistent gain across both query forms and both positions
in the sequence. Reject the additional interval execution path (`58c4008`
reverts it), retaining the earlier exact prefix pruning and bitmap gathers.
The data do not support claiming either a reliable SIMD gain or a causal
full-query slowdown.

[Raw component trials](prefix-abba-sf10.json) · [Q20 iterations](Q20-SUM.md)
