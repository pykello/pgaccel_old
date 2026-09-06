# Stripe-bound integer decoding for join reduction

Checkpoint `640dce8` binds each raw integer width once per run of probe
row identities from a stripe. It avoids repeated generic value construction
and row-group lookup while preserving in-place row order and duplicates.
Small bitmaps use range-checked unsigned offsets; sparse domains retain
binary search. All 172 native tests and focused sanitizers pass, including
all four storage widths, stripe boundaries and signed wraparound extrema.

The controlled ABBA test runs a supplier–nation projection 31 times per
process. These execution-only batches exclude repetition 1 from medians
and are not the promotion gate. Each final batch answer matches DuckDB.

| Run | Version | Warm execution median ms |
|---|---|---:|
| 1 | sum-v16 | 3.899 |
| 2 | sum-v17 | 3.211 |
| 3 | sum-v17 | 3.126 |
| 4 | sum-v16 | 3.918 |

The reduction is consistent in both positions, so retain the decoding
change. Its full Q20 iteration median was 36.378 ms, and the subsequent
[15-trial confirmation](Q20-CONFIRM1.md) failed the fourfold target.
Do not substitute the isolated gain for end-to-end results.

[Raw component batches](domain-decode-abba-sf10.json) ·
[Small-domain algorithm and SF1 validation](SMALL-DOMAINS.md)
