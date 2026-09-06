# Memory and execution follow-up after SF100

This is a staged engineering plan, not a claim that the proposed changes
already improve performance. Complete the frozen three-engine baseline
before changing the engine again. Final query rankings belong in RESULTS.md.

## What was kept before the comparison

Compact physical join keys replace general-purpose value containers in
supported hash-join paths. Duplicate keys and exact decimal equality remain
part of the contract. At SF10, Q21's budgeted query allocation peak fell
from 12.809 to 4.177 GiB; process peak RSS fell from 23.953 to 14.265 GiB.
Q3 and Q13 allocation peaks fell 52.2% and 40.6%, respectively. These are
focused before/after measurements, not extrapolated SF100 improvements.
All 22 SF1 queries subsequently passed three complete oracle comparisons.

The benchmark runner now propagates explicit memory and spill limits into
nested plans. The first SF100 attempt exposed a stale 8 GiB child limit
under a 64 GiB root. This configuration fix does not remove accounting:
execution still clamps children to the remaining parent allowance. The
corrected Q2 passes all three SF100 trials, with an 11.521 GiB allocation
peak. Native and sanitizer suites pass 129 tests each.

## Stage 1: restrict correlated aggregates to required keys

The complete 64 GiB SF100 PGAccel phase passes 21/22 queries. Q20 is the
exception: its membership/scalar child reaches the query allocation limit
while service memory remains below 192 GiB. A separate 80 GiB recheck is
recorded outside the baseline. This failure makes reducing correlated
aggregate state the first implementation priority. Q17 also takes about
655 seconds warm with a 31.58 GiB allocation peak; Q2 takes about 110 seconds.

`ExecuteRelational` currently executes scalar child plans and builds their
result maps before applying the outer relation's local filtering. Parsing
turns supported equality correlations into grouped children. That avoids
executing one subquery per outer row, but can still aggregate keys the
outer query never needs.

Start with independently evaluable outer filters. Build a distinct domain
of surviving correlation keys, then restrict the inner aggregate to that
domain. Q2 is a candidate: part size/type filters can reduce the part-key
domain before the regional minimum-supply-cost aggregate. Q20 is another:
the selected part domain may reduce the line-item `(part, supplier)` groups.
These are hypotheses to test, not measured speedups.

Only move predicates whose dependencies are already available. Preserve
missing-group behavior, exact decimal scaling, duplicate handling, and
future NULL semantics. Do not conflate a missing SUM/MIN group with zero;
COUNT needs its own default-row semantics. Charge the domain and retained
child output against the same statement budget. Fall back to the existing
plan when the domain is large or cannot be computed independently.

DuckDB's explanation of decorrelation illustrates replacing a correlated
scalar with an aggregate and hash join. The additional restricted-domain
proposal here is an engineering inference for PGAccel's current execution
order, not a claim about DuckDB's exact SF100 plan. See the primary
[DuckDB subquery article](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql).

Validate Q2/Q17/Q20 plus empty domains, unmatched keys, duplicate keys,
multiple scalar children, and mixed decimal scales. Record domain size,
inner rows admitted, aggregate groups, allocations, and wall time. Keep
only general transformations that improve the measured target cases
without an unacceptable regression on broad-domain queries.

## Stage 2: reduce base residency without hiding load costs

The current runner loads all eight native tables and all their columns.
The first SF100 attempt measured about 97.83 GiB resident immediately after
loading, before query working memory. File size and residency are different:
the native files occupy about 62.02 GiB. This is already a substantial
fraction of the requested 192 GiB envelope.

Use the optional field selection in `ColumnarTable::Load` as a starting
point for metadata-first, projection-aware loading. Resolve referenced
columns through joins, filters, grouping, ordering, CTEs, and subqueries.
Load missing columns on demand with stable ownership, or adopt a bounded
column cache if retaining every previously accessed column is too costly.
For example, a revenue scan needs price, discount, and relevant predicate
columns; it should not require the line-item comment payload.

Do not hardcode a list of columns for TPC-H. Any first-use loading must be
included in the corresponding cold query or reported as an explicit preload.
Measure all-column and on-demand modes separately. Validate late column
loads, aliases, repeated CTE references, and eviction lifetime safety.
Keep this change only if it reduces peak process and service memory with
correct results and acceptable cold and warm latency.

## Stage 3: make grouping state proportional to useful information

For low-cardinality keys, investigate dense aggregation arrays indexed by
validated dictionary codes. Q1's return/status groups are a useful case;
SUM, COUNT, and AVG should share a scan and avoid generic key allocation.
Codes local to different stripes require translation or stripe-local
partial states before merging: the same integer does not necessarily
identify the same string across stripes.

For high-cardinality grouping and scalar caches, measure key width, hash
table load factor, per-entry allocation, and retained output separately.
Extend compact representations only where measured overhead warrants it.
Maintain exact decimal accumulation and overflow checks. Benchmark both
skewed and broad-cardinality inputs; choose a bounded fallback instead of
allocating an array proportional to an unbounded key range.

Q19 is a separate execution target: it uses only about 0.188 GiB of query
allocations but takes about 321 seconds warm at SF100. Profile local
predicate evaluation, repeated expression reads, and scalar per-row work.
Evaluate applicable predicates in batches through existing vector kernels
before relational probing, with a semantic fallback. Measure the general
OR/AND and mixed-expression cases; do not add a hardcoded Q19 answer path.
Memory reduction alone does not address this latency.

## Stage 4: bound joins and introduce parallel execution carefully

The relational hash joins remain serial and in memory. A query allocation
limit currently produces a controlled failure; it is not a join-spilling
implementation. Profile build, probe, predicate evaluation, aggregation,
and conversion separately after the baseline. Use observed cardinalities
to choose build sides and test bounded partitioned joins when necessary.

Before parallelizing, examine composite equality edges and fanout estimates.
Q9 matches partsupp by both supplier and part, while the current index uses
one equality and checks the other as a residual. A composite physical key
could avoid unnecessary candidates. Sampling 4,096 apparently distinct keys
also does not prove uniqueness in a much larger build relation. Record the
chosen edge and actual candidate counts before diagnosing the SF100 slowdown.
Validate composite-key duplicates and decimal normalization independently.

Selection vectors currently retain 64-bit encoded row identities. Consider
32-bit global ordinals when table cardinality permits, with explicit mapping
to stripes and a 64-bit fallback. SF100 lineitem is below the 32-bit row-count
limit, but stripe/row encoding cannot simply be truncated. Check whether hash
node alignment would erase any saving before extending this to join entries.

Add partition-local build/probe and aggregation in a separate change from
spilling. Keep all worker allocations under one statement budget; multiplying
a nominal per-worker budget would defeat the memory work. Preserve duplicate
multiplicity and supported LEFT JOIN count semantics. Compare one and eight
workers, memory peak, skew, and total workload time before retaining it.

## Acceptance and publication

Each stage is a sequence of small contained commits with wrapped explanatory
bodies. Run semantic regressions and sanitizer checks before performance
measurements. Freeze binaries, use the same generated data and SQL hashes,
run engines sequentially, retain all trials, and check every answer against
the saved typed oracle. Report failures and load cost rather than removing
queries from a winning total. Repeat ClickBench before claiming its earlier
performance survives changes to shared execution paths.

Separate allocator peaks, process RSS, and service memory including charged
file cache. The Linux documentation describes the cgroup memory accounting
and hard-limit interface used here:
[cgroup v2 memory controller](https://docs.kernel.org/admin-guide/cgroup-v2.html).
Neither SF100 nor a compressed file size alone guarantees that a query fits
in 192 GiB. The resident representation plus concurrent working state is
what must fit; intermediate cardinality can dominate both.

Use original implementations or dependencies with suitable permissive
licenses. Algorithm references are not permission to copy implementation
code. This plan introduces no new engine dependency.
