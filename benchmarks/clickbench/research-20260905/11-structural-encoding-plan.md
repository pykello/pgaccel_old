# Next staged experiment: shared dictionary identities

This is a plan, not a measured speedup. The goal remains at least 39/43 wins
and a paired shifted geometric mean below one, with every trial validated.

## Benchmark boundary

The [ClickBench rules](https://github.com/ClickHouse/ClickBench/blob/main/README.md#indexing)
allow automatically created indexes, discourage manual extra indexes, and
place preaggregated tuned configurations outside the main competition.
They disallow query result caching and require load time plus storage costs.
Our current protocol measures resident warm execution, not true cold runs.
Retain a complete unindexed checkpoint before introducing another storage
configuration. Disclose configuration, construction time, persistent bytes,
peak construction bytes, and process RSS alongside its results.

A shared dictionary is a lossless representation of column values. A posting
index must retain actual row positions usable by general predicates; merely
storing COUNT DISTINCT or GROUP BY answers is not an acceptable substitute.
Construction must depend on the data and schema, not on benchmark SQL or
hard-coded column names, predicates, or expected answers. No query-output or
late aggregate cache is allowed in the stopping target.

## Stage 1: original interning and immutable dictionary storage

Implement an exception-safe interning table with contiguous keys and index
slots, reusing the original flat-table design. Assign equal logical values
the same global ID, preserving signed values and exact strings. Keep reverse
values plus per-stripe forward mappings. Existing local 8/16-bit dictionary
codes map to global IDs; raw columns can receive row-level IDs. Include only
labels actually used by rows in the reverse dictionary.

Build before timed queries under an explicit construction memory limit.
Publish an index only after complete construction. Preserve the base native
fixture and implement a separate configuration first. Measure build cost and
compare auxiliary storage against the bytes it could eventually replace.
Do not change serialization until the in-memory design earns its cost.

## Stage 2: general operators over global IDs

Use shared IDs for grouping identity and exact distinct membership, decoding
only candidate output rows. Scalar unfiltered distinct can use the complete
reverse dictionary's cardinality, just as ordinary counts use row metadata.
Filtered distinct still executes the predicate and determines selected IDs.
Use narrow or partitioned counters for grouped counts, and measure scan,
partition, merge, and candidate costs independently.

Pack composite identities only when their total bit widths fit, otherwise
use an exact wider representation. Independently validate cross-stripe code
spaces, signed boundaries, filtered empty inputs, and memory failure. Values
of wide computed expressions retain their existing checked representation.

## Stage 3: exact posting lists and uniqueness

Evaluate generic value-to-row-position indexes as an additional experiment.
Postings must support ordinary equality selection and combination with other
predicates, not only aggregate metadata shortcuts. Report every byte and the
construction cost. If the reverse dictionary cardinality equals the table's
row count, uniqueness is proven and grouping can preserve one row per key.
Use that fact only through a generic planner condition, with tests including
nearly unique data containing duplicates.

## Stage 4: decide and remeasure

Compare full 43-query checkpoints, all 129 answers, and the refreshed
ClickHouse run. Run sanitizer and date regressions. Keep only configurations
whose memory/build tradeoff is useful for a documented immutable analytic
use case. Preserve unsuccessful prototypes and their measurements. Distinguish
regular automatic indexing from explicitly tuned or preaggregated references;
never merge those scores into a misleading headline result.

No new dependency is planned. Any later library requires verified permissive
terms before adoption. All interning and execution code in this experiment
will be original implementations.
