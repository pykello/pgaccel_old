# Plan after reaching the ClickBench target

Historical checkpoint 3 plan. The four query losses below have since been
closed in [checkpoint 4](46-checkpoint4-all-queries.md). See the
[current staged plan](47-follow-up-plan.md) for remaining product work.

The [final checkpoint](36-checkpoint3-target.md) meets the requested target:
39/43 warm query wins and a 0.368585 paired shifted geometric latency ratio.
This plan describes unimplemented follow-up work. The next useful milestone
is a read-only analytics service over one immutable, preloaded fact table,
with a bounded SQL surface and explicit memory and concurrency limits.
It should serve repeated filtering, grouping and exact-distinct analytics
where preprocessing can be amortized across many queries.

## Rules for each stage

Implement one contained change per commit, with an explanatory body wrapped
at 72 characters. Before each change, record a hypothesis and comparison
baseline. Preserve rejected measurements and remove rejected production paths.
Run correctness and focused benchmarks first; run a complete paired comparison
when promoting a candidate release. Continue using all 43 unchanged queries,
all 129 answers per engine, warm trials two and three, and disclosed load costs.
Do not use cached query answers or silently replace exact aggregates with
approximations. Audit any new dependency's license before adoption; retain
original or permissively licensed implementations only.

For a performance change, require repeatable improvement beyond observed
run-to-run noise on its target queries, no correctness regression, and no
unexplained material regression in the complete suite. Record allocation,
resident memory, construction time and fallback behavior along with latency.
A gain on this one dataset is an experiment until tested on other distributions.

## Stage 1: make the current configuration operationally bounded

Unify accounting for immutable columns, retained indexes, build scratch and
concurrent query allocations. Currently the 64 GiB index budget and 32 GiB
per-query budget are separate; neither alone caps the complete process.
Define admission control and a reserve for the process and allocator. Reject
or queue work before allocation exceeds the configured service envelope.

Replace blanket experimental index construction with an explicit data-driven
policy using cardinality, column width, expected access patterns and memory
cost. Keep dictionaries, dense row postings and sparse repeat postings as
separate choices. Retain exact scan fallbacks when an index does not fit.
Index selection must not depend on recognizing benchmark SQL or column names.

Validate budget exhaustion during construction and queries, partial-build
publication, table lifetimes, cancellation, repeated loads, and bounded
concurrent readers. Measure admission behavior and RSS under pressure. Ship
this stage only when the documented memory limit is enforceable across the
whole service, not just individual PMR containers.

## Stage 2: improve the four remaining losses

Work through these experiments separately. The listed times are final warm
client measurements, not predictions for these proposals.

| Priority | Query | PGAccel / CH ms | Hypothesis |
| --- | --- | ---: | --- |
| 1 | Q29 | 10569.211 / 1948.511 | Avoid repeated regex/string expression work across stripe dictionaries |
| 2 | Q28 | 876.656 / 162.139 | Bind string-length reads and use compact dense numeric aggregate state |
| 3 | Q22 | 168.090 / 105.183 | Reduce exact substring checks and membership materialization |
| 4 | Q24 | 165.344 / 114.553 | Improve the same string-filter path without extra global preprocessing |

### Q29: selected-label expression evaluation

Profile time in regex evaluation, string allocation, dictionary translation,
hashing and merging separately. Prototype a query-local expression cache keyed
by exact global dictionary ID, evaluating only labels reached by selected rows.
Compare against the existing stripe-local cache. A global dictionary can reduce
repeated label evaluations, but materializing every global result can waste
work and expose errors in labels the query would never evaluate.

Test duplicate labels across stripes, empty and long strings, expression
errors, selected versus unselected labels, regex replacement semantics and
memory exhaustion. Include dense and highly selective filters. Keep a cache
only if its complete construction plus execution time improves the query and
its memory is charged; reject a win that depends on retaining query answers
between timed requests. Existing RE2 behavior must remain the semantic baseline.

### Q28: dense aggregation of string-derived numbers

Measure whether expression dispatch, repeated length computation or aggregate
lookup dominates. Bind physical readers once per stripe, and prototype direct
COUNT/SUM/AVG state indexed by a bounded exact group domain. Consider generic
per-label byte lengths only if their build and resident costs are justified.
Preserve the engine's defined string-length semantics, typed numeric results,
wide sums and empty-input behavior.

Compare low and high group cardinalities, long strings, varying dictionary
reuse and raw-column fallback. Charge temporary arrays and compare total
query time, not just the aggregate loop. Retain dense state only below a
measured memory/domain threshold; the generic executor remains necessary.

### Q22 and Q24: candidate rejection and row membership

Profile trigram signature false-positive rates, bytes read by exact matching,
local-label membership expansion and downstream grouping. Try smaller changes
such as avoiding repeated global-ID translation or changing signature layout
only after identifying the dominant cost. Test absent, common, rare, short
and non-ASCII needles, as well as negation and intersected predicates.

Do not reinstate the rejected global exact-label preparation unchanged:
[experiment 25](25-global-substring-rejected.md) made Q22 and Q24 slower.
Any new global preparation needs a different hypothesis and must count its
entire query-local setup time. Verify exact matches after signature candidates;
signatures cannot substitute for string equality or substring evaluation.

## Stage 3: persist indexes and measure amortization

Design a versioned optional index section in the native file, with bounds,
checksums, source identity and dictionary/row-position consistency checks.
Define invalidation and rebuild rules before adding writes. Retain a readable
base table if an optional index is absent or incompatible; fail safely on
corrupt metadata rather than exposing partially trusted pointers.

Measure fresh Parquet import, native conversion, index construction, persisted
size, cold native load, warm reload, and the first complete query suite. Report
break-even query counts for preprocessing using measured workload mixes.
Test truncation, version mismatch, wrong row counts, endian assumptions and
allocation failure. Keep persistence only if reload savings justify storage
and maintenance costs. Current warm wins alone do not establish that case.

## Stage 4: validate a useful SQL and data contract

Publish supported types, null behavior, date/timestamp interpretation, string
semantics, arithmetic overflow policy and unsupported-query behavior. Explicitly
resolve product expectations for Q4's wide average; do not disguise the current
arithmetic difference as identical output compatibility with ClickHouse.

Expand differential tests beyond ClickBench: independent random datasets,
all supported integer widths, signed extremes, negative timestamps, nulls
where supported, duplicate labels in unrelated dictionaries, skew, singleton
and empty groups, tied LIMIT boundaries and allocation failures. Exercise
scalar and available SIMD paths against the same reference results. Preserve
laziness for CASE and filtered expressions that could throw.

Run read-only concurrency and long-lived process tests after semantic coverage
is credible. Measure throughput and latency distributions for a realistic
query mix, not just independent best-of-two latency. Define a supported memory
and concurrency envelope. Release the limited service only after these gates
pass; do not imply general PostgreSQL compatibility from 43 queries.

## Stage 5: evaluate broader indexing and joins separately

The [bitmap probe](08-bitmap-probe.md) remains a useful experiment. Test
bit-sliced or range representations on actual filter distributions, including
construction, compressed size, decoding and mixed predicates. One-sided range
wins do not establish a default format advantage for arbitrary high-cardinality
ranges. Integrate only after a complete workload beats current SIMD scans and
postings within the same memory envelope. AVX-512 scatter/gather availability
alone is not a cost model.

Joins are a separate capability milestone after the single-table service is
bounded. Start with exact many-to-one joins against small immutable dimensions,
explicit key uniqueness checks, compact build-side hash tables and projection
of only needed payload columns. Then measure partitioned hash joins for larger
build sides, including skew and memory limits. Keep dictionary IDs scoped to
their actual dictionary identity or translate them into a shared exact domain;
equal local integer codes do not imply equal strings across tables.

This stage has no claimed ClickBench benefit: the measured 43-query suite does
not validate a general join engine. Add independent join correctness and
performance workloads before making a product claim.

## Sources and implementation boundary

The [research references](references.md) link primary material on aggregate
hash tables, Bloom signatures, bitmap indexes, wavelet representations and
ClickBench's indexing rules. The [dependency audit](dependencies.md) records
license decisions, including the public-domain hash finalizer and the removed
Abseil trial. Papers inform experiments; no third-party implementation should
be imported without a separate license review.
