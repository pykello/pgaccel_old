# TPC-H targets: fourfold confirmed cohort, twofold remaining queries

The user revised the target on 2026-09-06: Q7, Q8, Q9, Q10, Q21 and Q22
must be at least twice as fast as both DuckDB and ClickHouse at SF10 and
SF100, measured by warm-median client execution. Preserve the fourfold
target for the other 16 individually confirmed queries. Individual
checkpoints do not replace the final full-workload comparison.

Q7 already meets the revised twofold target at both scales at `7b452eb`:
4.10x/13.58x against DuckDB/ClickHouse at SF10 and 3.57x/8.24x at SF100.
Thus 17 queries meet their individual targets in recorded checkpoints;
Q8, Q9, Q10, Q21 and Q22 remain. No new measurement is implied by this
change of target. Historical fourfold gate failures remain recorded.

Run engines sequentially with eight workers and preserve all trial times.
Exclude only trial 1 from warm medians. Use fresh multi-trial comparisons
for promotion, then test the same frozen PGAccel binary at SF100. Preserve
192 GiB SF100 service limits, zero swap and the 64 GiB PGAccel query limit.
Do not run builds or tests during timed benchmark phases. A final complete
22-query comparison on one binary and one consistent client interface is
required after individual gates pass. Q16 uses the optional native Python API;
earlier individual confirmations use the CLI and remain separately labeled.

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
| Q5 | Native Python passes both scales at `f76d659`; [results](Q5.md) |
| Q6 | Both scales pass at checkpoint `b12ce48`; [results](Q6.md) |
| Q7 | Both scales meet revised 2x target at `7b452eb`; [confirmations](Q7-CONFIRMATIONS.md) |
| Q8 | Optimization pending; 2x target at both scales |
| Q9 | Optimization pending; 2x target at both scales |
| Q10 | Optimization pending; 2x target at both scales |
| Q11 | Both scales pass at checkpoint `8d118bc`; [results](Q11.md) |
| Q12 | Both scales pass at checkpoint `7c92573` |
| Q13 | Both scales pass at checkpoint `5190383`; [results](Q13.md) |
| Q14 | Both scales pass at checkpoint `7c92573` |
| Q15 | Both scales pass at checkpoint `918e691`; [results](Q15.md) |
| Q16 | Native Python passes both scales at `90e5747`; [results](Q16-NATIVE.md); CLI remains below target |
| Q17 | Both scales pass at checkpoint `fdda059`; [results](Q17.md) |
| Q18 | Both scales pass at checkpoint `537c8aa`; [results](Q18.md) |
| Q19 | Both scales pass at checkpoint `7c92573` |
| Q20 | Both scales pass at checkpoint `409c4ca`; [results](Q20.md) |
| Q21 | Optimization pending; 2x target at both scales |
| Q22 | Optimization pending; 2x target; recent SF10 answer check passes |

Continue with Q8, Q9, Q10, Q21 and Q22 one by one. Recheck Q7 and the
16 fourfold confirmations in the final sweep on one frozen module. Retain original implementations or permissively
licensed dependencies only. Each retained change gets a separate small
commit with an explanatory body wrapped at 72 characters. Record rejected
experiments and preserve the general executor for unsupported shapes.

[Confirmed cohort](SIX.md) · [Correlated results](CORRELATED.md) ·
[Measured next steps](PROFILE-NEXT.md)
