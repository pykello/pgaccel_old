# Plan after all 43 ClickBench query wins

[Checkpoint 4](46-checkpoint4-all-queries.md) meets the extended target:
43/43 wins against a fresh ClickHouse run, with 129 validated answers per
engine and 95 passing tests in both native and sanitizer builds. There are
no remaining measured query losses in that comparison. The next useful goal
is a bounded read-only analytics service over one immutable, preloaded fact
table, where repeated queries can amortize loading and generic indexing.

The [checkpoint 3 plan](37-follow-up-plan.md) is historical. Its four query
losses have been closed by measured implementations; the product-readiness
work remains. The stages below are proposals, not completed features or
claims about unmeasured workloads.

## Stage 1: publish and enforce the supported contract

Document accepted SQL, types, string byte-length behavior, date/timestamp
interpretation, arithmetic overflow, ordering ties and unsupported operations.
Decide whether the product deliberately retains wide averages or must emulate
another engine's overflow behavior. The Q4 difference must stay explicit.
Define immutable table ownership and reject writes or unsupported queries
clearly. Scope the initial service to filtering, projection, grouped numeric
aggregates and exact distinct counts on one loaded fact table.

Exit gate: users can determine whether their dataset and query fall inside
the supported contract, and unsupported cases fail predictably. Extend
independent correctness cases for nulls and other types before advertising
support; the ClickBench dataset does not establish those semantics.

## Stage 2: bound total memory and concurrency

Unify accounting for native columns, retained indexes, index-build scratch,
allocator overhead and admitted concurrent queries. The current 64 GiB index
and 32 GiB per-query budgets do not cap the complete service. Add explicit
admission control and a reserve for the process. Test repeated requests,
cancellation, allocation exhaustion and table replacement lifetimes.

Make index selection data-driven under a resident budget. Dictionaries,
local signatures, dense postings and sparse repeat postings should be separate
choices with exact scan fallbacks. In particular, measure whether the local
signature duplicate is worth retaining for the intended workload mix.

Exit gate: enforce a documented whole-service memory envelope under bounded
concurrency, with no incomplete-index publication or incorrect fallback.
Report latency distributions and throughput, not only isolated warm minima.

## Stage 3: persist optional indexes and measure end-to-end costs

Design a versioned index format tied to the exact native table identity,
with bounds and consistency validation. Define rebuild/invalidation rules
before adding mutable data. Preserve a usable base table when optional
indexes are absent or incompatible; reject corrupt data safely.

Measure fresh import, native conversion, index construction, storage size,
cold startup, warm reload and the first complete query suite. Calculate the
preprocessing break-even point from actual query mixes. Current checkpoint 4
costs approximately 49 seconds to load and 142 seconds to build indexes;
its warm wins do not establish an ingestion or restart advantage.

Exit gate: persistence gives a measured end-to-end benefit worth its storage
and maintenance cost, including recovery and malformed-input tests.

## Stage 4: test robustness beyond this corpus and host

Use independent datasets with changed cardinality, skew, row order, dictionary
reuse, string lengths, Unicode/invalid bytes and timestamp ranges. Differentially
check scalar and SIMD filtering, sparse selections, top-K ties and offsets,
weighted aggregates and the restricted regex compiler. Preserve fallback for
unsupported regex syntax; do not widen the fast grammar without a proof and
new comparisons against RE2.

Repeat full paired measurements with fresh imports, multiple host classes and
several concurrency levels. Record all samples, construction and memory costs.
Keep each change only when the complete result is correct and gains exceed
observed noise without unexplained regressions. The 43/43 result is a measured
checkpoint, not a guarantee that every distribution or machine will win.

Exit gate: a limited release has a reproducible operating envelope, semantic
coverage and representative workload results beyond the benchmark fixture.

## Stage 5: evaluate larger capability changes separately

Keep the bitmap/range-index probe as research rather than a default format.
Any integration must beat current scans and postings after accounting for
construction, memory, decompression and mixed predicates on actual workloads.

For joins, start with exact many-to-one joins against small immutable dimensions,
with enforced key uniqueness and compact build-side payloads. Larger joins need
partitioning, skew handling and a shared memory budget. Dictionary IDs only
identify equal values inside the same exact dictionary domain; local stripe
codes cannot be compared across unrelated columns or tables without translation.
ClickBench's 43 single-table queries do not validate join performance.

Every implementation should remain a contained commit with an explanatory
body wrapped at 72 characters. Preserve rejected experiments and raw evidence.
Only original or separately verified permissively licensed implementations
may be retained; the [license audit](dependencies.md) remains the reference.
