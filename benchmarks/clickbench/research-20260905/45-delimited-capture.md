# Experiment 45: bounded literal-prefix delimiter captures

Implementation: `e7dd247`. The prior capture-prefix path still spent several
seconds in regex evaluation. Compile a narrower, structurally recognized
regex family into an original finite literal-prefix plan:

```text
^L([^D]+)D.*$  -> replacement \1
```

L contains ASCII literals and greedy optional literals/noncapturing literal
groups. D is one ASCII delimiter. At most 16 ordered prefix alternatives are
materialized. No query ID, URL hostname, or column name is recognized.
Unsupported regex syntax and other replacements retain the existing evaluator.

For each input, try literal alternatives in the regex's priority order. Find
the first delimiter after a matching prefix, requiring a nonempty capture.
If the capture is empty, try the next prefix alternative: for example,
`^x(?:ab)?([^:]+):.*$` on `xab:tail` must backtrack from optional `ab` and
capture `ab`, rather than incorrectly fail. Once a candidate exists, strict
UTF-8 validation guarantees the original rune-aware suffix can consume the
remaining string. Return a borrowed capture view while retaining the input's
owner. Non-standard or invalid byte encodings use the original RE2 path.

The UTF-8 guard is sufficient, not an assertion that RE2 rejects all other
encodings. Testing found that this RE2 version accepts some non-standard
three/four-byte sequences. Those retain fallback behavior. The validator has
explicit scalar-value expectations, exhaustive one/two-byte tests, random
inputs, and valid/invalid three/four-byte boundary cases.

All 95 native and 95 ASan/UBSan tests pass. Differential replacement tests
cover six eligible pattern families, optional-prefix priority, Unicode,
invalid bytes, NULs, newlines and unsupported syntax. Earlier expression tests
also remain enabled. No new library, external implementation, persistent
metadata, or query-answer cache is introduced.

## Full-data result

| Q29 warm client measurement | ms |
| --- | ---: |
| Previous capture-prefix path | 4361.307 |
| Delimiter-capture plan in complete checkpoint 4 | 1205.125 |
| Previous complete ClickHouse checkpoint | 1948.511 |

The three Q29 client times are 1246.528, 1235.382 and 1205.125 ms.
All 129 PGAccel answers in the complete 43-query checkpoint pass validation.
The other previously losing queries measure Q22 40.947 ms, Q24 16.434 ms,
and Q28 143.676 ms. Every PGAccel query in this run is below its previous
ClickHouse checkpoint time. A fresh full ClickHouse comparison is the final
acceptance gate; the preceding checkpoint is not substituted for that run.

Decision: keep. The exact structural plan removes the dominant regex cost
without storing any derived query results between requests. It is limited to
the documented grammar; RE2 remains necessary for the full expression surface.

Evidence in the external docs mirror: `pg-checkpoint4-indexed.jsonl`, its
metadata and 129-answer gate, `native-checkpoint4.log`, and
`sanitize-checkpoint4.log`. The final paired report records setup costs and
all 43 client latencies from the same frozen engine.
