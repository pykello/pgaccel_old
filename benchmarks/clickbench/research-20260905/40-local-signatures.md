# Experiment 40: signatures in local dictionary order

Hypothesis: substring scans pay for dependent random reads when each local
label fetches its global ID and then a global trigram signature. A duplicate
signature array in local label order should turn that work into a sequential
scan. Implementation: `362bab4`.

Index construction copies each global 128-bit signature into the corresponding
stripe-local label slot. The immutable attachment is published only with a
completed index, is not serialized, and is fully charged to the index budget.
Exact string checks still follow possible matches; negative predicates retain
the inverse exact result. Independent local code spaces are tested explicitly.

| Query | Previous complete PG ms | Local signature warm ms | Previous CH ms |
| --- | ---: | ---: | ---: |
| Q21 | 165.489 | 138.284 | 321.574 |
| Q22 | 168.090 | 144.044 | 105.183 |
| Q23 | 268.931 | 217.954 | 556.302 |
| Q24 | 165.344 | 144.182 | 114.553 |

Q28 control is 141.609 ms; Q29 remains 5565.801 ms after the weighted executor.
All 18 trial answers pass the full gate, including valid Q22 boundary ties.
All 87 native tests pass. Index bytes increase from 28,105,839,348 to
29,615,085,204 (an additional 1,509,245,856 bytes, about 1.406 GiB).
This run loads native data in 51.766 s and builds indexes in 138.099 s.

Decision: retain provisionally as the basis for vectorized signature rejection.
The layout improves the target filters but does not yet win Q22 or Q24. Its
extra resident memory must remain visible in the final comparison.

## Separate Q29 profile

After all six queries' timed trials were saved, the same resident process ran
an additional Q29 warmup followed by three profiled executions. Those executions
are excluded from the timing file. `perf record -F 199 -e cycles:u -p PID`
collected 26,038 samples with no lost samples. Shared-object attribution is:

| Object | Sampled cycles |
| --- | ---: |
| RE2 | 82.52% |
| Query runner | 8.24% |
| libc | 4.99% |
| libstdc++ | 4.02% |

This motivates examining regex work before changing aggregate tables again.
These percentages describe sampled CPU work, not wall-clock latency or a
promised speedup. The profile wrapper only attaches after the timed section.

Raw artifacts in the external mirror: `pg-local-signatures.jsonl`, its metadata
and gate, plus `pg-local-signatures.perf.data`, `.perf.log` and `.perf.txt`.
