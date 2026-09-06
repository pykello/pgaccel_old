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
before promotion; the current DuckDB target remains unmet. Q17 also
validated in the same sum-v1 run and remains below its SF10 target.

[Full objective](ALL-QUERY-GOAL.md) · [Q17 scale results](Q17.md) ·
[Earlier correlated iterations](CORRELATED.md)
