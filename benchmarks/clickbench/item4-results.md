# Complete corpus correctness checkpoint

All 43 unchanged ClickBench queries execute and pass the full-file gate on
99,997,497 rows. There are 34 direct ClickHouse matches, eight independently
verified tied-order alternatives (Q22, Q25, Q32, Q33, Q39–Q42), and Q4's explicit
wide-average policy. The raw audit preserves every original mismatch.

The first Q29 run exposed regex dot/newline behavior. An independent ClickHouse
check found exactly the missing 61 newline-containing values for its first
domain. A corrected full-file Q29 recheck matches all returned values. The
combined audit replaces that record while retaining both original runs.

All 29 native tests pass in release and under ASan/UBSan with leak detection.
The local CI script also passes the audit-policy and generated-Parquet tests.
A small complete-schema fixture executes the entire pinned corpus in fast CI;
the independent full-file gate remains necessary for answer coverage.

Evidence is in `../docs/clickbench/results/staged-work/item4/`:
`audit.jsonl`, `q29-newline.jsonl`, `audit-latest.jsonl`, `refinements.jsonl`,
and `gate.json`. Current one-pass comparisons are in
`../docs/clickbench/06-current-comparison.md`. These are correctness-audit
observations, not an official benchmark score. Q33 remains a large performance
gap: 226,264 ms before scheduling work versus 2,920 ms in ClickHouse.
