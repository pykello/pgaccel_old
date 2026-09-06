# Retained ASCII literal-prefix matching

Checkpoint `d6c1726` replaces regex evaluation with an ASCII check and
byte-prefix comparison for simple literal-prefix LIKE patterns. Every
non-ASCII value retains RE2, as do other LIKE shapes. The implementation
uses the existing project ASCII scanner; no dependency or external code
is added.

SQL LIKE is translated here to a quoted literal followed by `.*`, with
dot matching newlines. RE2 documents concatenation, repetition and the
dot/newline option in its [syntax reference](https://github.com/google/re2/wiki/Syntax).
The ASCII shortcut is a local equivalence argument for that restricted
shape, verified against the existing regex executor.

A stricter UTF-8-validator prototype failed the expanded reference tests:
its treatment of malformed byte sequences differed from RE2. That
prototype was corrected before any benchmark. The retained version keeps
RE2 for every non-ASCII value. Tests include overlong encodings, surrogate
values, out-of-range sequences, truncation, valid multibyte characters,
NULs, newlines, negation and prior masks. All 169 native tests and focused
sanitizer checks pass.

The controlled ABBA component run uses 51 internal repetitions per query
and excludes only repetition 1 from execution medians. Final batch answers
match independent DuckDB counts. These execution-only batches are not the
client-time promotion gate.

| Run | Version | LIKE median ms | NOT LIKE median ms |
|---|---|---:|---:|
| 1 | sum-v12 | 3.533 | 3.449 |
| 2 | sum-v14 | 0.444 | 0.442 |
| 3 | sum-v14 | 0.439 | 0.441 |
| 4 | sum-v12 | 3.585 | 3.494 |

The improvement is consistent in both positions and both query forms.
Retain the change. Q20's separate nine-trial client median improves from
36.952 to 34.974 ms, with every answer matching. The current 32.720 ms
fourfold target remains unmet.

[Raw component measurements](prefix-ascii-abba-sf10.json) ·
[Q20 iteration history](Q20-SUM.md) · [Earlier rejected SIMD idea](PREFIX-EXPERIMENT.md)
