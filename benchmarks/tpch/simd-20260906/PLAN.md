# CPU SIMD iteration: Q1, Q3, Q14

The target is end-to-end warm query latency no greater than one quarter
of **both** DuckDB and ClickHouse. Start at SF10 with Q1, then validate
SF100 when the SF10 gate passes. Follow with Q14 and Q3. Report misses
explicitly; no result caching, precomputed answers, or query-name dispatch.
Use eight workers and original SQL, with the previously documented
ClickHouse decimal-ratio adaptation for Q14. Preserve every trial and
check typed answers. Query execution includes building temporary indexes.

Q1 contains no join. Its present composite-group path hashes keys and
interprets arithmetic for every row. First try stripe-local dictionary
products and bounded integer expression blocks, with overflow-safe
fallback and checked wide final sums. Reuse identical SUM/AVG inputs.

For Q3 and Q14, first measure scan, build, and probe costs. Candidate
changes are compact integer key domains, batched probes, SIMD bucket
comparisons, early exact membership filters, and independent probe
partitions. Preserve duplicate fanout, decimal key scales, outer/semi/anti
semantics, allocation accounting, and general fallback paths.

## Research and implementation policy

- [Böther et al., vectorized hash tables (2023)](https://anakli.inf.ethz.ch/papers/vectorized_hash_tables_vldb23.pdf)
  compares linear probing, fingerprints, and bucket comparisons across
  architectures. SIMD wins depend on layout and load factor: gather alone
  is not sufficient. Use as research context, not copied code or figures.
- [Balkesen et al., sort versus hash (2013)](https://www.vldb.org/pvldb/vol7/p85-balkesen.pdf)
  motivates cache-conscious partitioning and comparing full algorithms,
  including partition costs, rather than isolated probe instructions.
- [DuckDB grouped aggregation](https://duckdb.org/2022/03/07/aggregate-hashtable)
  describes thread-local hash aggregation and partitioned combination.
  For very small group domains, a dense local accumulator is a simpler
  candidate than repeatedly looking up the same keys.
- [ClickHouse join evolution](https://clickhouse.com/blog/clickhouse-fast-joins)
  combines parallel hash joins, filtering, planning, and storage work.
  Whole-query speed requires addressing these stages together.

Implement original algorithms in this repository. Do not import research
artifacts with restrictive or unverified code licenses. Citation of a
paper does not license copying its code or diagrams.

Each retained engine change gets its own small commit with correctness
and benchmark evidence. Failed experiments remain documented separately.

## Join iteration after the Q1 scale gate

Start the join work with Q14, then Q3. Q14 separates build costs from the
larger multi-table planning problem in Q3. Explore a validated key-to-row
position mapping before a general SIMD hash table: if an integer build
column is exactly a consecutive sequence, prove that using SIMD and
compute row positions directly. Verify every key during query execution;
min/max metadata alone does not prove uniqueness or sequence order.
Filtered, duplicate, sparse, or reordered keys must keep exact fallback
semantics. This is a data-layout decision and must not recognize TPC-H
column names or infer primary keys from benchmark knowledge.

Related primary sources: [DuckDB perfect-hash join source](https://raw.githubusercontent.com/duckdb/duckdb/main/src/execution/operator/join/perfect_hash_join_executor.cpp)
and [ClickHouse direct joins](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4).
These motivate comparing bounded key domains and direct lookups with
hashing. A consecutive sequence proof is a narrower, independently
implemented opportunity in the resident column representation. No code
or diagrams from these sources are copied.

## Checkpoint status and subsequent work

Q1 and Q14 passed SF10 and SF100 at their frozen checkpoints. The final
three-query SF10 confirmation also passes the fourfold warm-median gate;
see [SF10-CONFIRM.md](SF10-CONFIRM.md). Q3 improved from seconds to tens
of milliseconds, with all temporary index construction inside query time.
The final three-query SF100 validation also passes; see
[Q3-SF100.md](Q3-SF100.md) for the complete same-binary comparison.

The target is a measured warm-median speedup, not a bound on every trial.
Preserve first trials and the complete warm range. Do not claim an entire
workload win from these three queries or combine measurements from
different frozen binaries into a new total.

Further join work should proceed in this order:

1. Re-run the complete SF1 coverage/correctness matrix and selected SF10
   join shapes before widening kernel eligibility. Existing unit and
   sanitizer suites cover numerical and key-domain edge cases, but do
   not substitute for a full-workload performance comparison.
2. Measure fallback joins with duplicates, wide integer domains, and
   strings. Try compact key fingerprints and SIMD bucket comparisons
   only where exact positional or bitmap-rank lookup does not apply.
   Keep full-key equality and duplicate fanout in the correctness path.
3. Compare radix partitioning and thread-local build/probe state for
   working sets beyond cache. Include partition and merge costs, memory
   peak, and skewed-key behavior in end-to-end timing. Do not assume
   gather/scatter is beneficial: the rejected experiments here show it
   can make the whole query slower.
4. Extend eligible arithmetic and join shapes individually, with a small
   commit, a forced-fallback reference test, repeated benchmarks, and a
   retained/rejected decision for each change. Keep permissive dependency
   licensing and original implementation as requirements.
