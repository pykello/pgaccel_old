# Experiment 33: bounded timestamp truncation keys

Keep the exact packed timestamp bucket. For DATE_TRUNC minute, hour, or day,
divide the native epoch seconds with correction toward negative infinity.
Subtract the minimum bucket derived from validated source bounds, then pack
the complete remaining domain. Calendar reconstruction is needed only for
typed output, not every grouped row.

For example, -61 seconds belongs to minute -2, while -60 and -1 belong to
minute -1. Tests verify negative boundaries, cross-stripe repetitions,
hour/day domains, and identity separation at both Int64 extremes.

## Storage-type correction

The initial implementation read timestamp bounds through the wrong raw-data
template specialization. Native loading uses RawColumnData<Int64Type>.
Commit 986a978 corrects the concrete type. The earlier time-case-compact
Q43 timing is superseded and is not used for the retention decision.
All 82 native and address/undefined-behavior sanitizer tests pass after
the correction; subsequent conditional-layout tests expand the suite.

## Corrected full-data measurement

Original Q43, full data, eight threads; minimum client latency of trials
two and three in conditional-reserve.jsonl:

- Corrected PGAccel: 7.923 ms.
- Fresh checkpoint 2 ClickHouse: 22.548 ms.

All 21 trial answers in the corrected seven-query subset validate.
The same binary also includes bounded input reservation, so this figure
describes the resulting timestamp grouping path, not a separately isolated
instruction-level effect. No persistent timestamp index was added.

Raw evidence: conditional-reserve.jsonl, its metadata and gate.
Frozen binary: ../clickbench-work/research-20260905/bin/conditional-reserve-query-runner.
The implementation is original and introduces no dependency.
