# Review of the ClickBench and TPC-H branch

Date: 2026-09-07. Base: `origin/main` at
`d23737898e26ee2449fb8747afa5896eb735d62e`. Reviewed head:
`311112be72dd84fe6eff725733e54d0d9812b6a8` on
`feature/tpch-coverage-20260905`; 524 commits, 452 changed files.

## Assessment

The branch has become a capable experimental read-only analytical executor.
The key improvement is compact state carried across operators, with structural
proofs before specialized execution. It is more than a collection of SIMD scans:
there is a general relational baseline, exact distinct/aggregation, scoped
subqueries, resource controls, optional indexes and a native API.

The measured achievements are substantial, but the next engineering priority
should be predictable behavior outside benchmark-friendly layouts and memory
budgets. The results justify a limited immutable analytics product; they do not
yet establish a general-purpose database or concurrent service.

No new wrong-answer defect was confirmed in this review. That is not a proof
that none exists. One benchmark failure-propagation defect is confirmed by source
inspection; several integration and product limitations are also confirmed.

## Scope and evidence

Reviewed all commit subjects and final changed-file families, then inspected
high-risk implementation paths: loading and types, query dispatch, dictionary
identity, aggregation/spill, general joins and materialization, verified join
indexes, independent-chain and paired-profit guards, Q21/Q22 semantics, native
API lifetime/locking, benchmark validation and CI. The source guide maps the
families to files and representative commits. Not every line of the 524 commits
received a fresh independent review.

Re-executed `build/run_tests`: **256 tests from 105 suites passed**, 3.918 seconds.
The log is retained beside this report as `20260907-native-review-tests.log`.
This used the existing built test binary; no code changed during review.
Historical sanitizer results were inspected, not rerun. No benchmark was rerun.

- [Final TPC-H sweep](../../benchmarks/tpch/simd-remaining-20260906/FINAL-ALL1.md): one frozen
  native module, all 22 queries, SF10/SF100, all recorded answers validated.
- [ClickBench checkpoint 4](../../benchmarks/clickbench/research-20260905/46-checkpoint4-all-queries.md):
  43/43 indexed warm wins, under its declared arithmetic and tie policy.
- [Blog source guide](../../../blogposts/pgaccel-execution/source-guide.md) and
  [full change register](../../../blogposts/pgaccel-execution/changes.md).

## Findings, in priority order

### R1 — High: standalone TPC-H runner does not fail on query failures

**Confirmed defect by source inspection.** In
`benchmarks/tpch/compare_scale.py:253–273`, records receive `correct=False`
(or `None` when an oracle is unavailable), are written and printed, and `main()`
returns normally. Exceptions inside individual trials are also recorded without
making the final process status fail. A caller using only `subprocess.run(...,
check=True)` can accept an unsuccessful or unvalidated run.

The final report is not invalidated: its wrapper/report scripts separately assert
query counts, every trial's correctness and the targets. The standalone scale
runner and other future callers should not depend on remembering that extra step.

**Action:** aggregate status and return nonzero for failure, incomplete repeats,
missing required oracle or incorrect results. If collection-only behavior is
needed, make it an explicitly named opt-in. Add tests using tiny fixtures or
mock engines for wrong answer, missing oracle, timeout and partial output.

### R2 — High: routine CI does not enforce full TPC-H/API integration

**Confirmed integration gap.** `ci/check.sh` builds/runs native tests, audit-gate
tests and schema tests. Its optional full job is ClickBench-specific. It does
not invoke the all-22 TPC-H oracle comparison or `python/test_native.py`; the
Python target is optional and not built by this script. Native unit fixtures
cover many TPC-H shapes, but that is a different gate.

**Action:** put an inexpensive all-22 integration fixture, native API tests and
both indexed/unindexed ClickBench correctness configurations into versioned CI.
Use larger scale performance jobs on scheduled capable hosts, not every change.
The purpose is detecting future shared-path regressions, not repeating the just
completed SF100 measurement to repair documentation.

### R3 — High for deployment: query budgets are not a whole-process envelope

**Confirmed architectural limit and measured cost.** `Budget` accounts aggregate
allocations, not the entire native dataset/runtime. `python/bindings.cc:24–43`
eagerly loads every native table. At SF100 the final run starts with about
98.0 GiB RSS and reaches a 100.2 GiB process high-water mark. DuckDB/ClickHouse
recorded 23.6/15.7 GiB respectively; these lifetime peaks are not isolated query
allocation peaks. PGAccel's service peak is higher again because it includes
charged cache. All sampled SF100 services had zero swap/OOM events.

The indexed ClickBench configuration separately retains 27.581 GiB of auxiliary
indexes; it is not the configuration used by final TPC-H. Making joins use fewer
scratch bytes does not bound resident snapshots or concurrent Python results.

**Action:** inventory resident bytes by table/column/index, then introduce an
explicit service budget, reservations and selective column/stripe residency.
Retain the eager path for workloads that deliberately fit in memory.

### R4 — High for broader joins: aggregate spill is not general join spill

**Confirmed capability boundary.** `src/executor.cc:61–117` routes multi-table
plans outside the single-table external-aggregation retry. `ExecuteExternal`
implements restricted sort/merge aggregation; it is not a spillable hash join.
Specialized relational paths generally convert allocation failure into an error.
A valid but unstructured or larger join can hit a performance/capacity cliff.

**Action:** add a bounded duplicate-preserving partitioned hash-join fallback,
with skew handling and spill, before promising larger-than-memory or arbitrary
key distributions. Test forced tiny memory and exceptional cleanup. Preserve
semantic errors; do not turn every failure into a retry or silently change plans.

### R5 — Medium: full ClickBench speed claim is from an older runtime

**Confirmed evidence gap, not a demonstrated regression.** ClickBench's 43/43
run uses `e7dd247` with optional indexes and warm minima from trials 2–3.
Final TPC-H uses `a7bccf3` without those indexes and warm medians from 15/9 trials.
Subsequent shared executor, string, memory and output changes have not received
an equivalent new full ClickBench performance confirmation in the published
record. Do not imply one binary now has both published outcomes.

ClickBench Q4 intentionally differs in overflow behavior; TPC-H's typed oracle
validation uses a different policy. Both are disclosed in their reports.

**Action:** retain distinct benchmark manifests and result labels. At the next
release candidate, run both corpora on one frozen source with their respective
explicit configurations. No rerun is needed merely to fill today's memory gaps.

### R6 — Medium: specialization selection is hard to explain or cost

**Confirmed maintainability limit; workload regression risk.** The ordered chain
of `Execute...` attempts in `src/executor.cc` mixes shape recognition, data proofs
and execution. A later attempt can repeat similar proofs after an earlier one
has allocated/scanned then declined. There is no supported EXPLAIN surface for
which path ran, why a proof failed, or how much preparation cost was paid.

The specialized tests are valuable: duplicate keys, shuffled stripes, missing
foreign keys, bounded arithmetic, different dictionaries and low budgets appear
throughout the newer suites. Nevertheless, the abundance of narrow eligibility
conditions means distribution coverage matters as much as SQL text coverage.

**Action:** expose path/rejection diagnostics first. Factor reusable immutable
proof results into planning next, without removing the independent semantic
checks that consumers need. Add costs only after measuring preparation and scan
phases; a major optimizer rewrite is not the first step.

### R7 — Medium: native API is serialized and has no cancellation contract

**Confirmed API limit.** `python/bindings.cc:45–88` locks one database during
parsing/execution, releases the lock before Python conversion, and returns
strings. Its GIL/lock ordering deliberately avoids a waiting-thread deadlock.
That is good, but there is no in-process deadline or cancellation API; the
benchmark metadata explicitly requires an external process deadline.

Output conversion is outside the database lock and query allocation budget.
Separate Database instances can also retain separate resident datasets. A service
must account for outstanding results and admission, not just concurrent execute
locks. No race or deadlock was demonstrated by this review.

**Action:** document serialized semantics now; add cooperative cancellation,
result ownership, bounded requests/results and shared immutable snapshot handles
before allowing unrestricted concurrent clients.

### R8 — Medium: portability is limited despite scalar execution options

**Confirmed build limit.** `CMakeLists.txt:32` globally enables AVX-512F/BW.
A scalar flag inside an executor does not make the binary safe on a CPU without
those instructions; the compiler can vectorize other code. `QueryArena()` uses
a fixed eight-worker arena. The TPC-H runner correctly rejects non-eight PGAccel
thread settings, so the published settings are honest.

**Action:** isolate ISA-specific kernels and add runtime feature selection plus
a genuinely baseline build. Make worker count a query/context setting, and
measure topology/NUMA separately. Do not infer portability from scalar-vs-SIMD
tests running on the same AVX-512 host.

### R9 — Medium: generic intermediate/results still cross string boundaries

**Confirmed performance/semantic maintenance cost.** Numeric membership paths
avoid many conversions, but `executor_materialized.cc:22–37` still constructs
numeric intermediate tables from formatted result rows. The Python API also
returns strings. Q22's fast scalar threshold intentionally reproduces existing
format/bind rounding, illustrating how internal presentation choices become
observable semantics.

**Action:** introduce typed internal batches and explicit aggregate-result
conversion rules. Preserve compatibility tests before removing format/parse
boundaries; doing so can change row selection even if the new arithmetic appears
more precise. A typed public batch API is a subsequent, separable change.

### R10 — Medium: corpus wins do not establish complete SQL semantics

**Confirmed scope limit; no new wrong-answer case found.** Import rejects NULLs;
second-resolution timestamps, bounded specialized grammars and supported
correlation/left-join forms are deliberate restrictions. Q11 is empty at the
larger measured scales, so its nonempty SF1 checks matter. General NOT IN/NULL
three-valued logic cannot be inferred from non-null TPC-H coverage.

**Action:** publish a feature/semantic matrix and a narrow initial product scope.
Test alternative cardinality, skew, row order, duplicate and missing keys,
Unicode, date ranges, filter selectivity, empty groups and overflow. Extend NULL
support only as a storage-to-result feature, not one parser patch.

## Review by area: retain versus revisit

| Area | Evidence supporting retention | Follow-up |
|---|---|---|
| Base encoding/date fixes | Logical identity and range checks; randomized and boundary tests | Nullable data model, portable kernels |
| Flat/radix exact aggregation | Explicit equality and allocator behavior; measured full-data gains | Skew, cancellation, bounded parallel scratch |
| Optional dictionaries/postings | Exact actual values and row positions; disclosed index costs | Budgeted selection, persistence and end-to-end break-even |
| Weighted strings/regex | Grammar recognition and generic fallback; UTF-8/tie tests | Differential grammar/distribution fuzzing |
| General joins/subqueries | Duplicate-preserving storage and explicit scopes | Spill, typed exchange, planner diagnostics |
| Positional/rank/repeated indexes | Actual data proofs; guarded widths and missing-key checks | Reusable proofs, adversarial distributions |
| Q8/Q9/Q10 | Graph eligibility plus exact multiplicity/dependency checks | Avoid repeated preparation; fallback performance |
| Q21 | Exhaustive small-summary/split-merge comparison | Disorder/skew fallback cost |
| Q22 | Explicit scalar conversion, prefix/anti-join guards | Centralize scalar result semantics |
| Benchmarking | Frozen hashes, all answers, preserved rejected trials | Fail-closed entry points and manifest completeness |

## Rejected work and licenses

The Abseil flat-set trial was removed after allocator-exception behavior failed
its gate. Several SIMD/layout/probe-order experiments were reverted after noisy
or negative results. The bit-sliced comparison remains a standalone experiment;
a compressed bitmap-tree storage format was not adopted.

The repository's ClickBench dependency audit records original retained executor
work, RE2 fallback and a public-domain SplitMix64 finalizer. The native Python
interface adds pybind11, recorded as BSD-3-Clause in its setup work. No new
third-party implementation was introduced by this documentation review. This
is an inspection of recorded provenance, not a new legal audit of every installed
package. Future algorithm adoption must retain the user's permissive-license
constraint and verify the exact implementation and transitive dependencies.

## Recommendation

Proceed through [the staged plan](20260907-staged-improvements.md). First close
failure propagation and integration gaps, then make planning and memory behavior
observable, then improve residency and general fallback behavior. Further
benchmark-specific tuning should serve that product envelope rather than replace
it. The current runtime remains unchanged by this review.
