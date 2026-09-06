# Q20 typed correlated-sum iterations

Client elapsed milliseconds, eight workers, warm medians excluding only
trial 1. All values and rows validate against the saved DuckDB oracle.
This work has not passed the fourfold target against both engines.

| Version | Warm median ms | Warm min ms | Warm max ms | Peak query MiB |
|---|---:|---:|---:|---:|
| domain-v5 | 529.833 | 525.367 | 533.957 | 94.72 |
| sum-v1 | 106.690 | 102.113 | 112.584 | 45.53 |
| sum-v2 | 96.128 | 90.398 | 103.390 | 45.53 |
| sum-v3 | 69.331 | 64.989 | 80.216 | 30.73 |
| sum-v4 | 65.962 | 60.470 | 72.885 | 30.73 |
| sum-v5 | 63.145 | 60.754 | 82.531 | 30.73 |
| sum-v6 | 62.231 | 53.149 | 71.507 | 30.73 |
| sum-v7 | 62.867 | 50.667 | 69.530 | 29.70 |
| sum-v8 | 52.954 | 43.791 | 57.649 | 29.19 |
| sum-v9 | 49.631 | 39.883 | 54.417 | 17.41 |
| sum-v10 | 54.245 | 39.566 | 59.336 | 17.41 |
| sum-v11 | 37.682 | 34.459 | 41.136 | 17.41 |
| sum-v12 | 36.952 | 33.842 | 40.945 | 17.41 |
| sum-v13 | 41.629 | 39.858 | 44.820 | 17.41 |
| sum-v14 | 34.974 | 31.622 | 38.053 | 17.41 |
| sum-v15 | 40.503 | 33.957 | 47.720 | 17.41 |

Sum-v15 (`8185921`) changes only runner stream buffering. A controlled
[interactive comparison](RUNNER-BUFFER.md) shows lower client overhead in
both ABBA positions, but its full Q20 median was 40.503 ms. Preserve this
run without claiming an end-to-end gain; all nine answers match.

Sum-v14 (`d6c1726`) matches ASCII literal prefixes directly while keeping
RE2 for non-ASCII values. Its median is 34.974 ms with nine correct
answers. A [controlled component comparison](PREFIX-ASCII.md) shows a
consistent gain; all 169 native tests and focused sanitizers pass.

Reject sum-v13 (`8aff0be`, reverted by `d94bc17`). Its constant-factor
shortcut measured 41.629 ms and showed no consistent improvement in
[controlled ABBA batches](FACTOR-EXPERIMENT.md).

Sum-v12 (`20b860a`) precomputes comparison scales and avoids repeated
expression traversal for row thresholds. Its median is 36.952 ms, with
nine correct answers. All 169 native tests and focused sanitizer checks
pass for every operator and both scalar operand orders.

Sum-v11 (`b2deffc`) builds independent tuple directories in parallel.
Its median is 37.682 ms with nine correct answers and 17.41 MiB peak
query allocations. All 169 native tests and focused sanitizer checks
pass, including duplicate rows and keys concentrated in one partition.
This version excludes the rejected prefix interval experiment.

Reject sum-v10 (`d712eb2`, reverted by `58c4008`). Its SIMD prefix
interval experiment measured 54.245 ms and did not show consistent gains
in a separate [ABBA component comparison](PREFIX-EXPERIMENT.md).

Sum-v9 (`a8e46d6`) narrows worker sums only after a subset-range proof.
Its median is 49.631 ms and peak query allocations fall to 17.41 MiB.
All nine answers, 168 native tests and focused sanitizer checks pass.
Wide partial sums and cancellation retain checked 128-bit accumulation.

Sum-v8 (`a8e4a99`) filters outer stripes in parallel before directory
construction. Its median is 52.954 ms with nine correct answers. All
167 native tests and focused sanitizer checks pass, including duplicates
across outer stripes. The fresh 32.720 ms target remains unmet.

Sum-v7 (`2bbcf4a`) reserves the tuple directory. Its 62.867 ms median
does not improve on sum-v6, though peak allocations fall by about 1 MiB.
Retain provisionally for memory and parallel row collection; see the
[fresh 15-trial comparison](Q20-PEERS.md) for the current target.

Sum-v6 (`e620fb6`) scans source stripes in parallel. Its median is
62.231 ms; the full warm range remains in the table rather than selecting
the fastest trials. All nine answers, 166 native tests and focused
sanitizer checks pass, including duplicate source keys across stripes.

Sum-v5 (`25aa392`) evaluates thresholds and compacts numeric projection
ranges in parallel. Its median is 63.145 ms with nine correct answers.
All 166 native tests and focused sanitizer checks pass.

Sum-v4 (`1437c22`) merges independent slot ranges in parallel. Its
nine-trial median is 65.962 ms, a modest improvement over sum-v3.
All answers match; 166 native tests and focused sanitizer checks pass.

Retain sum-v3 (`4e991dc`). Typed numeric membership output lowers the
SF10 median to 69.331 ms from 96.128 ms in sum-v2 (`5f71146`).
All nine answers match, and all 166 native and sanitizer tests pass.
Sum-v2 constructs the source bitmap directly; sum-v3 additionally avoids
formatting child keys and parsing them back into a parent hash set.

Retain sum-v1 (`03b409f`). A fixed pair of integer keys replaces vector
hash keys. Selected outer tuples define compact aggregate slots. Parallel
fact probes update typed per-slot SUM/count arrays; each outer row then
evaluates its threshold separately. Duplicate outer rows retain their
multiplicity and values. Missing groups remain distinct from zero sums.

The path directly constructs the required membership domain from a
simple filtered source projection. It preserves native date bounds and
uses general execution for unsupported shapes or stricter child caps.
All 165 native and sanitizer tests pass. No external implementation or
new dependency is introduced.

The prior fresh SF10 competitor baseline is DuckDB 145.186 ms and
ClickHouse 437.550 ms. A new comparison against both engines is required
before promotion; [fresh results](Q20-PEERS.md) now set a 32.720 ms target. Q17 also
validated in the same sum-v1 run and remains below its SF10 target.

[Full objective](ALL-QUERY-GOAL.md) · [Q17 scale results](Q17.md) ·
[Earlier correlated iterations](CORRELATED.md)
