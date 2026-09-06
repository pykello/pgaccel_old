# Q13 left-join count histogram

Status: implementation and synthetic validation. Fresh performance
measurements are pending; thirteen queries remain individually confirmed.

Q13 counts qualifying orders for each customer, including customers with
no qualifying orders, then reports the frequency of those counts.

`d81feb1` recognizes that two-stage shape. It verifies unique consecutive
customer keys, applies right-side join filters to orders, and increments
independent worker arrays by customer key. It merges those counts directly
into frequency bins. Customers with no match enter bin zero; duplicate
orders contribute repeatedly. This removes joined-row expansion and the
formatted numeric intermediate. Duplicate customer keys, unsupported
predicates and stricter child budgets retain general execution.

For example, customer counts [2, 0, 1, 0] produce frequencies (0, 2),
(1, 1), (2, 1). Filtering an order changes its customer's count before the
histogram is built. Customers are not discarded when all their orders fail
the join filter.

`22b0cca` handles LIKE patterns containing multiple ordered literals,
with percent wildcards at both ends and between the literals. For ASCII
input, each literal search starts after the previous match. Thus
`%aba%ba%` matches `ababa` but does not match `aba`; matches cannot overlap.
Underscores, escapes, non-ASCII patterns and non-ASCII input retain RE2.
The existing dictionary label bitmap and SIMD row-code mapping remain in
use. No persistent index or query result cache is added.

The new differential tests also exposed an existing single-substring
shortcut accepting malformed UTF-8 that general RE2 evaluation rejects.
`9741d24` restricts that shortcut to ASCII input. It is a separate
correctness fix, with negation and malformed-encoding regression tests.

During sanitizer validation, key verification crashed at an aligned
AVX-512 store to a temporary aligned to only 32 bytes. GDB captured the
instruction `vmovdqa64` and its destination address. `f6bb124` removes the
unnecessary general Cell construction: integer key verification now reads
the first raw value inside its existing width dispatch and checks storage
length. All 198 native tests and 20 focused ASan/UBSan tests pass afterward.

An initial baseline chain was stopped after that sanitizer failure. Its
files are tagged `q13-before-interrupted` and excluded from performance
claims because a sanitizer retry overlapped the incomplete measurement.
New frozen `q13-hist-regex-v2` and `q13-hist-ordered-v2` runners share the
same key-verification fix and differ only in the ordered-literal shortcut.
Fresh baseline and iteration runs follow complete SF1 answer validation.

The implementation is original, using existing project primitives and no
new dependencies. RE2 remains the semantic reference for general LIKE
patterns; its [syntax documentation](https://github.com/google/re2/wiki/Syntax)
describes the regular-expression operations used by the existing evaluator.

## Measured filter refinements

The [iteration table](Q13-ITERATIONS.md) records the old path, histogram
with RE2, ordered byte search, deferred encoding checks and bounded SIMD
literal search. The final iteration is 91.527 ms, with all nine answers
correct; fresh confirmation is still required.

The optional trigram extension was implemented in `d367cfd` and reverted
in `34f00ff`. The comparison harness explicitly sets
`PGACCEL_BUILD_DICTIONARIES=0`, so that extension is not exercised here.
It received no performance credit and does not change the benchmark policy.

`919c396` first searches for required literal bytes. Missing bytes prove
that the regex cannot match; only candidates need an ASCII check and,
where necessary, RE2 evaluation. `5190383` adds masked AVX-512 first/last-byte
probes for short literals and verifies full matches before returning.
The bounds prevent reads outside the string view, including page tails.
All 200 native tests and five focused sanitizer tests pass.

Microsoft's [runtime engineering article](https://devblogs.microsoft.com/dotnet/performance_improvements_in_net_7/)
describes first/last-character filtering before full substring comparison
and notes that its performance depends on the input. This implementation
is original, with bounded masked loads and standard-library fallback; no
third-party source implementation was copied or added as a dependency.
