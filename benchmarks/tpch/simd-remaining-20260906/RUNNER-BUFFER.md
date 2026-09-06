# Query runner buffering

Checkpoint `8185921` disables C stdio synchronization and the input stream
tie. Every JSON response retains its explicit flush. Native and sanitizer
checks confirm loaded responses, result rows, parse errors and subsequent
queries remain visible without waiting for another input line.

This is a **client transport optimization**, not a join algorithm change.
The promotion gate still includes execution, fetching and answer conversion.

The controlled ABBA test returns 1,804 supplier rows for each of 31 requests.
Every response matches DuckDB. Exclude only trial 1 from medians. The
client-minus-execution difference includes parsing, transport and conversion;
it is not a pure serialization timer.

| Run | Version | Client median ms | Median client minus execution ms |
|---|---|---:|---:|
| 1 | sum-v14 | 4.496 | 2.926 |
| 2 | sum-v15 | 3.889 | 2.551 |
| 3 | sum-v15 | 3.871 | 2.265 |
| 4 | sum-v14 | 4.188 | 2.779 |

Both positions show a smaller client overhead, so retain the buffering
change. Q20's separate nine-trial run measured 40.503 ms, compared with
34.974 ms previously; unchanged Q17 also slowed. Do not claim an end-to-end
Q20 gain from that noisy run or substitute the component result for its
promotion gate. A new full comparison remains required.

[Raw interactive trials](runner-buffer-abba-sf10.json) · [Q20 iterations](Q20-SUM.md)
