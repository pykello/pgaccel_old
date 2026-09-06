# CPU SIMD iteration: Q1, Q3, Q14

The target is end-to-end warm query latency no greater than one quarter
of **both** DuckDB and ClickHouse. Start at SF10 with Q1, then validate
SF100 when the SF10 gate passes. Follow with Q3 and Q14. Report misses
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
