# Fourfold target for all 22 TPC-H queries

The active objective is at least fourfold faster warm-median client
execution than both DuckDB and ClickHouse for every query, at SF10 and
SF100. Work continues until every query meets the gate; partial cohorts
are progress checkpoints, not completion of the objective.

Run engines sequentially with eight workers and preserve all trial times.
Exclude only trial 1 from warm medians. Use fresh multi-trial comparisons
for promotion, then test the same frozen PGAccel binary at SF100. Preserve
192 GiB SF100 service limits, zero swap and the 64 GiB PGAccel query limit.
Do not run builds or tests during timed benchmark phases. A final complete
22-query comparison on one binary is required after individual gates pass.

Validate row counts, ordering and every output value. The existing harness
compares integer/decimal values exactly. Floating output uses its existing
absolute tolerance of 1e-6 or relative tolerance of 1e-12, whichever is
larger; this accommodates representation differences, not missing rows
or approximate algorithms. Keep exact native-reference regression tests
for new arithmetic and threshold logic.

| Query | Current state |
|---|---|
| Q1 | Both scales pass at checkpoint `7c92573` |
| Q2 | Both scales pass at checkpoint `6e4e3da`; [results](Q2.md) |
| Q3 | Both scales pass at checkpoint `7c92573` |
| Q4 | Both scales pass at checkpoint `7c92573` |
| Q5 | Optimization pending |
| Q6 | Both scales pass at checkpoint `b12ce48`; [results](Q6.md) |
| Q7 | Optimization pending |
| Q8 | Optimization pending |
| Q9 | Optimization pending |
| Q10 | Optimization pending |
| Q11 | Both scales pass at checkpoint `8d118bc`; [results](Q11.md) |
| Q12 | Both scales pass at checkpoint `7c92573` |
| Q13 | Optimization pending |
| Q14 | Both scales pass at checkpoint `7c92573` |
| Q15 | Both scales pass at checkpoint `918e691`; [results](Q15.md) |
| Q16 | Optimization pending; recent SF10 answer check passes |
| Q17 | Both scales pass at checkpoint `fdda059`; [results](Q17.md) |
| Q18 | Both scales pass at checkpoint `537c8aa`; [results](Q18.md) |
| Q19 | Both scales pass at checkpoint `7c92573` |
| Q20 | Both scales pass at checkpoint `409c4ca`; [results](Q20.md) |
| Q21 | Optimization pending |
| Q22 | Optimization pending; recent SF10 answer check passes |

Start with measured Q17/Q20/Q2 bottlenecks, then continue the remaining
queries one by one. Retain original implementations or permissively
licensed dependencies only. Each retained change gets a separate small
commit with an explanatory body wrapped at 72 characters. Record rejected
experiments and preserve the general executor for unsupported shapes.

[Confirmed cohort](SIX.md) · [Correlated results](CORRELATED.md) ·
[Measured next steps](PROFILE-NEXT.md)
