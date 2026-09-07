# Staged improvements after ClickBench and TPC-H

Date: 2026-09-07. Based on `d237378..311112b` on
`feature/tpch-coverage-20260905`. This is a proposed implementation sequence,
not a list of completed capabilities. Read the [branch review](20260907-branch-review.md)
and [updated blog](../../../blogposts/pgaccel-execution/README.md) first.

## Intended product and priorities

Make PGAccel useful as an embedded, read-only analytics engine over immutable
columnar snapshots, with one fact table and a small number of dimensions,
non-null declared types, exact aggregates and supported joins/subqueries.
Expose a typed batch interface and predictable resource limits. Start with one
admitted query per shared snapshot; extend concurrency only after the resource
and cancellation contract is enforceable.

Preserve the compact scan/aggregation and verified join advantages. The next
major objective is predictable cost across data layouts and memory limits,
not a higher score on the same prepared benchmark. Do not add writes,
transactions, distributed execution or complete SQL compatibility to the first
release scope. Those require separate product decisions.

Current evidence:

| Workload | Measured achievement | Boundary to preserve |
|---|---|---|
| ClickBench | 43/43 indexed warm wins at `e7dd247` | Index build 142.189 s; total load/build 191.350 s; Q4 arithmetic qualification; warm minimum of trials 2–3 |
| TPC-H SF10 | Sum of medians 0.972 / 5.130 / 11.453 s | PGAccel / DuckDB / ClickHouse; native API; no optional indexes |
| TPC-H SF100 | Sum of medians 10.578 / 51.703 / 97.474 s | Same frozen `a7bccf3` module; all 22 queries; 3x/2x revised gates |
| SF100 process memory | High-water marks 100.2 / 23.6 / 15.7 GiB | Lifetime peaks, not isolated query allocations; PGAccel loaded RSS about 98.0 GiB |

Full [timing/memory evidence](../../benchmarks/tpch/simd-remaining-20260906/FINAL-ALL1.md)
is already complete. Do not repeat it merely to obtain missing memory fields.
Future runs below are acceptance tests for new implementation or a release
candidate, not a prerequisite for finishing this report.

## Stage sequence

| Stage | Outcome | Dependency | Priority |
|---|---|---|---|
| 0 | Trustworthy gates and explicit supported contract | None | Immediate |
| 1 | Explain execution and account for real costs | 0 | Immediate |
| 2 | Typed internal batches and stable API ownership | 0–1 | High |
| 3 | Enforced resident-memory and query envelope | 1–2 | High |
| 4 | General joins remain useful outside proved layouts | 1–3 | High |
| 5 | Reuse proofs and select physical paths by measured cost | 1, 4 | Medium |
| 6 | Amortize optional indexes and startup safely | 3, 5 | Medium |
| 7 | Portable, cancellable, bounded serving | 2–4 | Release gate |
| 8 | Validate the limited product beyond benchmark fixtures | All release features | Release gate |

Keep stages sequential for acceptance. A small infrastructure change needed by
an earlier stage may be pulled forward, but do not mix unrelated algorithm,
format and benchmark changes in one commit. Every commit needs a what/why body
with all message lines at most 72 characters.

## Stage 0 — Close correctness-gate gaps and publish the contract

Addresses review R1, R2, R5 and R10.

1. Change `compare_scale.py` to fail the process if a requested query fails,
   yields a wrong answer, has incomplete trials, or lacks a required oracle.
   Preserve raw records even on failure. Add an explicit collection-only mode
   only if a caller actually needs it.
2. Move one reusable all-query validation gate into the repository. Require
   exact query IDs/counts, expected repeats, every answer, frozen module hash,
   SQL hashes and declared settings. Separate correctness failure from missing
   performance target. Do not rely on assert statements that disappear under
   Python optimization for a supported gate command.
3. Add tests for wrong answer, partial output, missing oracle, timeout and stale
   module metadata. Use small/mocked inputs; no SF100 dependency for these tests.
4. Wire the existing native Python API test and a small all-22 TPC-H integration
   fixture into CI. Keep ClickBench correctness in both optional-index modes.
   Add scheduled sanitizer and larger fixture jobs with explicit host requirements.
5. Publish the supported type/operator matrix: non-null input, timestamp units,
   integer/decimal overflow policy, scalar AVG conversion, empty aggregates,
   string byte-length versus character slicing, collation/order ties, limited
   outer joins and correlation forms. State unsupported features clearly.
6. Store benchmark manifests with separate indexed/unindexed and CLI/native API
   configurations. Preserve the historical ClickBench and current TPC-H labels.

**Exit gate:** deliberate wrong results and missing trials fail CI; all existing
native/API and small-corpus cases pass; one command validates artifacts without
executing queries. A user can determine whether a workload is supported.

**Suggested first commits:** failure exit status; focused gate tests; common
artifact validator; TPC-H integration wiring; Python CI wiring; contract docs.

## Stage 1 — Expose execution plans and memory ownership

Addresses R3 and R6. Instrumentation precedes further tuning.

1. Add a stable selected-path name and structured rejection reasons for
   specialized executors: unsupported shape, unproved uniqueness/order, domain
   too large, arithmetic range unsafe, unsupported predicate or resource limit.
2. Expose EXPLAIN or a read-only planning API. Keep expensive data-proof execution
   clearly separate from an inexpensive syntactic explanation; report when a
   full proof has actually been performed.
3. Add optional counters for proof/prepare, filtering, payload construction,
   fact scan/probe, aggregation, merge, materialization and result conversion.
   Timing is opt-in; baseline execution must not pay per-row clock costs.
4. Inventory owned native bytes by table/column, optional index bytes,
   intermediate/query allocations and output bytes. Record loaded RSS and
   process/service peaks separately, with a clear statement of exclusions.
5. Establish a versioned metrics schema used by CLI, Python and benchmark tools.
   Use null for unavailable measurements, never zero as a stand-in for missing.

**Exit gate:** a Q9 or Q16 execution can explain its path, memory owners and main
phases; a shuffled-key variant explains its fallback. Disabled instrumentation
has no unexplained regression on representative small and large queries.

**Keep/reject rule:** if instrumentation itself changes hot-loop behavior,
move counters to stage boundaries or sampling. Do not benchmark instrumented
runs as the acceptance timing for an optimization.

## Stage 2 — Typed internal exchange and a typed result API

Addresses R9 and establishes ownership needed by stages 3 and 7.

1. Define an internal batch schema carrying logical type, integer/decimal scale,
   row count and ownership. Include a validity representation in the design,
   even while the initial loader rejects NULLs; do not silently enable NULL SQL.
2. Replace one general numeric materialized-source format/parse boundary with
   typed columns. Preserve exact SUM and the documented scalar AVG conversion.
   Add tests where rounding changes a predicate result, including the Q22 case.
3. Extend typed membership and aggregate-result exchange to the remaining
   supported internal consumers. Charge retained batches against the parent
   statement budget, and release them when the last consumer finishes.
4. Offer typed public result batches in addition to the existing string API.
   Define close/destruction behavior, outstanding-result lifetime and immutable
   data references before attempting zero-copy export.
5. Evaluate an Arrow C Data Interface adapter for interoperability. The format
   provides a small C ABI for in-process columnar exchange; this is a potential
   boundary, not a requirement to redesign all native storage. See the
   [official specification](https://arrow.apache.org/docs/format/CDataInterface.html).

**Exit gate:** old string results are unchanged under the existing policies;
typed round trips preserve values/scales; closing the database while a result
is retained follows the documented ownership contract. Materialized query tests
show reduced conversion/allocation cost without requiring a benchmark win claim.

**Risk:** “more exact” arithmetic can change established scalar SQL behavior.
Treat semantic changes as explicit versioned decisions, not incidental speedups.

## Stage 3 — Control residency and the whole execution envelope

Addresses R3. This is the largest practical memory opportunity.

1. Introduce a shared immutable snapshot handle rather than loading duplicate
   tables for each API connection. Track base columns, optional indexes and
   outstanding batches through that owner.
2. Add total-memory reservations for resident data, query scratch, results and a
   measured runtime reserve. Admission must use live reservations, not just a
   per-query maximum. Keep external cgroup limits as a backstop.
3. Support loading only the columns needed by a declared workload/schema subset.
   Reject unavailable columns clearly or load them through an explicit policy.
4. Prototype lazy stripe/column access with pinned buffers and eviction. Compare
   memory mapping, buffered reads and decoded-cache ownership using the current
   snapshot layout before proposing a storage-format rewrite. Version any layout
   change and retain corruption/bounds validation.
5. Make eviction respect borrowed string/key lifetimes. A batch or join holding
   a string view pins its backing data until that use ends. Avoid implicit dangling
   references when replacing eager permanent residency.
6. For actual deployments, expose a configured memory envelope and reserve for
   conversion/runtime. Budget cancellation and disk scratch together with it.

**Exit gate:** repeated queries and outstanding results stay inside the declared
service envelope or fail/admit predictably. Demonstrate materially lower resident
memory on a projected workload. As an initial experiment, target at least 25%
less SF100 loaded RSS with at most 10% warm-latency regression for the selected
workload; this is a proposed gate, not a measured promise. If the tradeoff fails,
retain the eager mode and publish the result.

**Why now:** the final SF100 process already holds about 98 GiB before querying.
A few MiB saved in a join cannot solve that baseline. Compression and buffer
residency should be evaluated with cold/first-query cost as well as warm latency.

## Stage 4 — A bounded general join path with skew and spill

Addresses R4 and the performance cliff behind failed structural proofs.

1. Keep the current positional/rank/repeated-run joins for proved layouts.
   Add a general compact build representation with exact keys, hash/fingerprint
   prefilters and duplicate chains/ranges. A fingerprint match is never equality.
2. Partition build/probe input by hash under the memory budget. Use measured
   partition sizes, bounded buffers and worker ownership. Handle an oversized
   repeated key explicitly; blindly repartitioning one heavy key cannot help.
3. Preserve all required duplicate pairs, semijoin existence, anti-join scope and
   outer-join unmatched rows. Add randomized comparisons against a simple nested
   loop/reference executor on small data before performance work.
4. Add external partitions with deterministic temporary-file cleanup and byte
   limits. Repartition or use an explicit bounded heavy-key strategy when a
   partition still cannot fit. Test disk-full and interrupted execution.
5. Reuse runtime membership/domain filters only when they cannot remove valid
   rows. For non-equality or unsupported composite keys, retain the existing
   explicit fallback/error behavior until separately implemented.
6. Evaluate SIMD tag/key probes after layout and partitioning are correct. Compare
   scalar, AVX2 and AVX-512 at equal semantics and memory accounting.

The [Saving Private Hash Join paper](https://vldb.org/pvldb/vol18/p2748-kuiper.pdf)
provides relevant primary research on adaptive external joins and temporary data
in a unified buffer pool. The [external aggregation paper](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf)
is useful for partitioned aggregation under memory pressure. These are design
references, not authorization to copy code or a claim those designs are installed.

**Exit gate:** correctness on unique, duplicate, sparse, unsorted, string and
composite keys; controlled behavior with 1 MiB/64 MiB and realistic budgets;
cleanup after injected failures. Unstructured variants improve over the current
general path without regressing the retained specialized paths beyond measured
noise. Report graceful degradation as memory falls, not just one in-memory time.

## Stage 5 — Reusable proofs and measured physical-plan selection

Addresses R6 after a credible fallback exists.

1. Introduce explicit proof objects for uniqueness, sequence/sorted domains,
   repeated runs, dictionary equivalence and functional dependencies. Record
   which columns, snapshot identity and predicate scope a proof covers.
2. Share a proof within a query instead of repeating scans across failed executor
   attempts. Persist immutable-table proofs only after defining validation and
   invalidation; snapshot identity alone must not bless unchecked claims.
3. Separate shape recognition, proof acquisition, cost estimation and execution.
   Start with the existing paths and a small rule registry; avoid a wholesale
   optimizer replacement in the same change.
4. Estimate work from actual selected rows, distinct counts/domain width, payload
   bytes, expected duplicates, output cardinality and preparation cost. Account
   for projection/formatting, which affected Q16 materially.
5. Centralize invariant helpers for arithmetic, scalar-result conversion,
   predicate consumption and child budget propagation. Keep consumer-specific
   proof checks visible and test them independently.

**Exit gate:** equivalent queries with renamed tables, changed constants, extra
predicates, reordered conjuncts and changed physical stripe order remain correct;
EXPLAIN identifies the same applicable rules or gives a valid reason not to.
Preparation work is measurably reduced where it was repeated. No query ID,
benchmark table name or measured answer may enter dispatch logic.

## Stage 6 — Budget optional indexes and make startup economics explicit

Build on stage 3 ownership and stage 5 planning; addresses the ClickBench tradeoff.

1. Separate global dictionaries, signatures, dense postings and sparse-repeat
   postings as optional components. Choose them by workload benefit, frequency,
   cardinality and available resident memory, with exact scan fallbacks.
2. Add a versioned optional-index sidecar tied to the validated native snapshot,
   column type and index algorithm version. Bounds-check contents and publish
   atomically. Missing/incompatible indexes should permit a base-table scan;
   corrupt data should fail safely or trigger an explicit validated rebuild.
3. Measure native import, index construction, reload, first query, warm queries,
   disk size and peak memory. The old indexed warm numbers exclude about 191 s
   of load/build; do not hide that cost in startup.
4. Calculate break-even for actual query frequencies:
   `build_time / sum(weight_i * (unindexed_i - indexed_i))`, using consistent
   units and only positive aggregate savings. Include memory/storage cost in
   the keep decision; there is no universal winning index set.
5. Revisit bit planes or compressed range trees only for a demonstrated predicate
   mix after a small corrected microbenchmark. Include construction, decode,
   projection, updates/rebuilds and mixed filters. The existing standalone probe
   is a starting point, not evidence to replace all forward vectors.

**Exit gate:** persisted indexes survive reload/invalid-input tests, stay inside
budget and give a documented end-to-end advantage for a defined repeated-query
mix. If a component fails its break-even or memory gate, keep it opt-in or drop it.

## Stage 7 — Cancellation, controlled concurrency and CPU portability

Addresses R7/R8. This stage gates a supported service or portable package.

1. Carry an execution context with worker budget, cancellation token, deadline,
   memory reservation and spill owner. Check cancellation at bounded stripe,
   partition and merge intervals; unwind every worker before releasing buffers.
2. Make the fixed eight-worker arena configurable without oversubscription across
   admitted requests. Begin with serialized admission, then test 2 and 4 clients.
3. Bound queued requests and retained results. Provide explicit rejection/backpressure
   behavior and a documented close/shutdown contract. Test disconnects and cancellation
   while loading, joining, spilling and converting results.
4. Build a portable baseline translation unit set and separate ISA-specific kernels.
   Select scalar/AVX2/AVX-512 by runtime CPU support. Do not assume `avx=false`
   compensates for global `-mavx512` compilation.
5. Measure NUMA placement, first-touch policy and worker topology as separate
   experiments. Keep a reproducible default; do not bake one Xeon’s layout into SQL.

**Exit gate:** no unsupported-instruction crash on a baseline host, matching
answers across compiled paths, cancellation completes within a declared tested
bound, and resource-limited multi-client runs show stable memory and useful
p50/p95/p99 latency plus throughput. No unbounded queue or background work after
request completion.

## Stage 8 — Release evidence and a limited supported workload

1. Construct distribution variants: dense/gapped/random keys, shuffled stripes,
   heavy hitters, repeated composite keys, missing foreign keys, different
   dictionary reuse, long/Unicode strings, date boundaries and selectivity extremes.
2. Differentially test both specialized and general execution. Add grammar-based
   predicate/expression generation and scalar/SIMD comparisons; retain failing
   seeds as small regression fixtures.
3. On a release candidate, freeze one source/module and run both corpora with
   explicit indexed/unindexed configurations. Preserve their existing semantics
   and add a consistent median-based report where new repeated trials justify it;
   never rewrite historical warm-minimum numbers as medians.
4. Measure startup, ingestion/reload and bounded concurrency for the intended
   product. Report complete query tables, arithmetic exceptions, memory scope,
   failures and all trials. Existing missing memory fields remain N/A.
5. Verify the final dependency manifest and exact licenses. Use original code or
   verified permissive implementations, including transitive dependencies and
   notices. Research-paper availability is not a software license.

**Exit gate:** a reproducible limited release with a supported SQL/data matrix,
resource envelope, platform list, installation instructions, ownership/cancellation
contract and independently validated workload results beyond the original fixture.
Accept performance regressions only as an explicit, measured tradeoff against
memory or broader capability—not by quietly changing the target or dropping queries.

## Work deliberately deferred

General nullable SQL requires validity propagation, three-valued predicates,
aggregate rules, join behavior and result encoding together. Prototype it after
the typed batch boundary, but do not advertise support until end-to-end tests pass.
Writes/MVCC, a PostgreSQL planner/extension integration, distributed joins and
transactional index maintenance each need a separate design and user use case.
The project name alone does not establish a PostgreSQL integration contract.

Do not start with a universal compressed bitmap tree, JIT compiler, learned index
or another benchmark-specific fused executor. Reconsider any of these only when
stage-1 measurements expose a bottleneck it plausibly resolves, and use the same
correctness, licensing, memory and whole-workload acceptance rules.

## Completion ledger template

For each implementation, record: problem and hypothesis; touched invariant;
small commit; tests and reference answers; baseline/final artifact hashes;
all timing samples; resident/query/output memory; keep/reject decision; known
limits. Keep failed experiments and raw evidence. Documentation-only changes
need link/content checks, not benchmark reruns.
