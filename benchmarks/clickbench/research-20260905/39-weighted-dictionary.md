# Experiment 39: weighted evaluation of selected dictionary values

Hypothesis: evaluating a derived grouping expression once per exact global
source value can avoid repeated stripe-local expression work and per-row
aggregate updates. Implementations are `c29bb4f` (bound-reader evaluator)
and `066800f` (weighted dictionary executor).

The executor first applies WHERE normally, materializes selected global source
IDs, and partitions them into disjoint ID ranges. It counts each selected ID
inside its owning range, evaluates the derived key only for nonzero counts,
and contributes that multiplicity to COUNT and SUM/AVG of source byte length.
MIN/MAX of the source use exact string comparisons. Worker hash maps merge
logical derived keys before HAVING, ordering and pagination.

For example, selected occurrences `ax, ax, bx` have weights 2 and 1.
Replacing `x` with a suffix needs two expression evaluations, while aggregate
counts remain 2 and 1. If both values map to the same derived key, the final
merge combines their weights and extrema. An unselected empty label is never
evaluated, so a division by its length does not introduce a new query error.

Eligibility requires one derived group expression depending on one string
column with a global dictionary. Supported aggregates are counts, direct
source-length sums/averages, and direct source MIN/MAX; other expressions
fall back. Filters may depend on other columns because selection precedes
weight construction. All temporary weights and tables are query-local and
budgeted; there is no persistent aggregate or result cache.

| Warm client measurement | Q29 ms |
| --- | ---: |
| Previous complete PGAccel checkpoint | 10569.211 |
| Weighted dictionary executor | 5550.624 |
| Previous complete ClickHouse checkpoint | 1948.511 |

All Q29 trial times: 5670.760, 5617.202, 5550.624 ms. Peak query allocation:
1,255,406,240 bytes. Q28 control is 140.900 ms. All six full-data answers pass
the gate, and all 86 native tests pass. Tests include explicit weighted sums
and extrema, independent stripe code spaces, owned regex output, numeric
computed keys, filters on another column, empty selections, lazy errors,
HAVING, pagination and budget exhaustion.

Decision: keep the substantial Q29 improvement, but it is **still a loss**
against ClickHouse. Profile remaining work before deciding the next step.
The new bound-reader evaluator shares the existing expression implementation;
no third-party code or dependency was added. Primary representation research
is linked in [references](references.md).

Raw artifacts in the external mirror: `pg-weighted-dictionary.jsonl`,
`pg-weighted-dictionary.meta.json`, and `pg-weighted-dictionary-gate.json`.
