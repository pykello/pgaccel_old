# TPC-H targets: threefold cohort, twofold remaining queries

The user accepted 3x on 2026-09-07 for the 16 queries previously targeted
at 4x, retaining 2x for Q7, Q8, Q9, Q10, Q21 and Q22. Do not redo completed
optimizations or benchmarks merely to pursue the old threshold.

The final full sweep is complete. All 22 queries at both SF10 and SF100
meet their revised targets against both engines on the same frozen native
module `a7bccf3`. Every recorded answer validates.

See [all query timings and recorded memory](FINAL-ALL1.md) and
[compact evidence](FINAL-ALL1.json). The original SF10 fourfold gate is
preserved; its measurements were accepted under the revised target,
without replacing them with diagnostic rechecks.

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
| Q8 | Both scales meet 2x target at `0de993a`; [results](Q8.md) |
| Q9 | Both scales meet 2x target at `c37ed93`; [results](Q9.md) |
| Q10 | Both scales meet 2x target at `2e427ac`; [results](Q10.md) |
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
| Q21 | Both scales meet 2x target at `9b3fd59`; [results](Q21.md) |
| Q22 | Both scales meet 2x target at `a7bccf3`; [results](Q22.md) |

The final comparison supersedes the historical individual checkpoints
listed above. Implementations and permissive dependencies are unchanged
from the validated frozen module.

[Confirmed cohort](SIX.md) · [Correlated results](CORRELATED.md) ·
[Measured next steps](PROFILE-NEXT.md)
