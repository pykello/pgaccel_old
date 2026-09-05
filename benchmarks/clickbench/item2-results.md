# High-cardinality checkpoint

Full 99,997,497-row fixture gate passed: 20 ClickHouse answer matches, Q4
accepted against its independent wide-average oracle, 22 expected parse
failures. All 16 native tests and the generated-Parquet integration checks pass.

These are one-pass audit observations with eight workers and AVX-512, not
an official benchmark score. A short test compilation overlapped part of the
audit; final repeated timing will be separate. RSS is the process lifetime
high-water mark including the resident dataset and preceding queries.

| Query | Before ms | After ms | Tracked peak GiB | Process peak GiB |
| --- | ---: | ---: | ---: | ---: |
| Q5 | 20198.2 | 7554.3 | 2.006 | 38.053 |
| Q6 | 11831.9 | 3820.5 | 0.718 | 38.053 |
| Q9 | 23455.2 | 14485.4 | 1.880 | 38.474 |
| Q10 | 23275.4 | 14797.3 | 1.892 | 38.575 |
| Q13 | 16289.8 | 5466.1 | 0.775 | 38.575 |
| Q14 | 27228.0 | 23188.2 | 3.708 | 40.125 |
| Q16 | 44166.8 | 16369.7 | 2.201 | 40.125 |
| Q34 | 54697.2 | 19503.0 | 2.628 | 40.125 |
| Q35 | 57595.4 | 19790.4 | 2.628 | 40.125 |

Raw evidence: `../docs/clickbench/results/staged-work/item{1,2}/`.
Spill exactness and cleanup were tested with a forced 1 MiB budget on
180,000 rows; this full-file audit stayed in memory.
