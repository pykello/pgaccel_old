# Correlated-query work: Q20, Q17, Q2

The six-query checkpoint passes the fourfold target at both scales.
Sixteen queries remain. This next stage starts with Q20's allocation
failure, then addresses the similar correlated aggregates in Q17 and Q2.

1. Push a required outer IN domain into the corresponding correlated
   child key before grouping. Q20 should aggregate only the parts selected
   by `p_name LIKE 'forest%'`. Preserve the child aggregate's composite
   keys, duplicate handling, and empty-group semantics. Start with the
   existing membership executor so allocation accounting remains intact.
2. Measure SF10 correctness, latency, and allocation against the frozen
   six-query binary and both competitors. A capacity improvement can be
   retained without claiming that it passes the latency target.
3. Derive domains from independently filtered outer dimensions for Q17
   and Q2. Propagate them only through proven required equality edges.
   Preserve general execution for unsupported join or nullable shapes.
4. Replace repeated scalar membership hashing with bounded exact integer
   bitmaps and SIMD probes where the domain density justifies storage.
   Charge construction and storage to the statement, retain exact fallback,
   and measure the cost of construction as part of query time.
5. Avoid full fact-row identity retention and string conversion of grouped
   scalar results where profiling shows these remain bottlenecks. Stream
   selected probes into typed aggregate states; preserve arithmetic scale.
6. Validate SF100 capacity under the same 192 GiB service and 64 GiB query
   limits. Promote performance claims only after the fourfold SF10 gate,
   followed by the equivalent SF100 comparison and regression checks.

The Q20 capacity check may precede its latency gate because the previous
SF100 workload could not complete at all. Label this as a memory check,
not a performance-target success. Run engines sequentially without builds
or tests during timed phases. Keep small commits and document rejections.

Research: DuckDB describes turning correlated subqueries into grouped
aggregation and joins in [Correlated Subqueries in SQL](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql).
PGAccel already uses this broad execution form; the local opportunity is
to restrict the correlation key domain before constructing aggregate
state. This is an inference from PGAccel's current plan and code, not a
performance claim from the article. No external implementation is copied.
