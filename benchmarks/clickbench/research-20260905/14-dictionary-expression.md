# Experiment 13: dictionary-level grouping expressions — keep

For a derived grouping expression determined entirely by one string/date
dictionary column, evaluate each selected label once per stripe. Retain the
result for hashing and equality until aggregation and merging finish. The
normal evaluator still handles the expression, including lazy branches and
owned strings. Never evaluate dictionary labels excluded by the row filter.

Q29 improves from 40,600.7 ms in the flat-composite experiment to 10,574.336 ms
warm client time. Its three trials are 10,907.991, 10,793.586, and 10,574.336 ms.
All answers validate. Tracked query peak is 3,289,166,816 bytes; the expression
cache is rebuilt and charged inside every timed execution. It is not a
persistent index or query-result cache. Refreshed ClickHouse is 1,914.740 ms,
so Q29 remains a substantial loss.

All 51 native tests pass. New checks cover owned computed string keys across
workers, independent dictionary code spaces, and division-by-zero labels
hidden by WHERE. The initial test expected integer formatting for division;
it was corrected to the engine's existing six-decimal result format. No
execution semantics were changed to satisfy that test.

Keep the reduction, then investigate shared dictionary labels and tighter
expression-state representations. Original implementation; no new dependency.
Raw evidence: `../docs/clickbench/research-20260905/dictionary-expression.jsonl`
and `dictionary-expression-gate.json`. Frozen binary:
`../clickbench-work/research-20260905/bin/dictionary-expression-query-runner`.
