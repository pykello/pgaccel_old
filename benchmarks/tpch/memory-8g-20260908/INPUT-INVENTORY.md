# TPC-H SF100 original input inventory

The schema-only inventory binds all 22 SQL files through the existing parser,
RequiredQueryColumns, PredicateOnlyColumns and DatePredicateOnlyColumns.
It executes no query and loads no column payload. Native column spans come
from consecutive metadata offsets and file size; schemas are validated by
the native header reader. Versioned SF100 and legacy SF1 metadata are supported.

The **53 required columns occupy 31.381 GiB**
of original native storage. All native files total
**62.022 GiB** excluding their short identity
headers. This is stored size, not decoded resident memory or measured I/O.
Preparation may filter rows, transform values, read columns more than once,
and issue aligned/speculative reads. The inventory does not infer row pruning
or certify a prepared column as safe to cache.

## Required original spans per query

The predicate-only columns below are eligible under existing binding proofs
for loss of their original values. Actual prepared data may also have a
query-specific row selection. A reusable cache must hold original source
columns, not these transformed or compacted outputs.

| Query | Original spans GiB | Required columns | Predicate-only values |
|---|---:|---:|---|
| Q1 | 6.793 | 7 | none |
| Q2 | 1.168 | 19 | part.p_type, region.r_name |
| Q3 | 7.861 | 10 | customer.c_mktsegment |
| Q4 | 5.640 | 6 | lineitem.l_commitdate, lineitem.l_receiptdate |
| Q5 | 8.758 | 16 | region.r_name |
| Q6 | 5.116 | 4 | none |
| Q7 | 9.662 | 13 | none |
| Q8 | 11.088 | 19 | part.p_type, region.r_name |
| Q9 | 13.173 | 17 | part.p_name |
| Q10 | 9.288 | 16 | lineitem.l_returnflag |
| Q11 | 1.048 | 8 | nation.n_name |
| Q12 | 7.103 | 7 | none |
| Q13 | 8.763 | 4 | orders.o_comment |
| Q14 | 6.328 | 6 | none |
| Q15 | 6.308 | 8 | none |
| Q16 | 0.795 | 8 | supplier.s_comment |
| Q17 | 5.701 | 6 | part.p_brand, part.p_container |
| Q18 | 5.721 | 8 | none |
| Q19 | 7.398 | 10 | lineitem.l_shipinstruct |
| Q20 | 8.390 | 15 | nation.n_name, part.p_name |
| Q21 | 7.602 | 11 | lineitem.l_commitdate, lineitem.l_receiptdate, nation.n_name, orders.o_orderstatus |
| Q22 | 0.964 | 4 | none |

## Largest required columns

| Column | Native GiB | Queries |
|---|---:|---|
| orders.o_comment | 7.589 | Q13 |
| lineitem.l_orderkey | 2.236 | Q3, Q4, Q5, Q7, Q8, Q9, Q10, Q12, Q18, Q21 |
| lineitem.l_partkey | 2.236 | Q8, Q9, Q14, Q17, Q19, Q20 |
| lineitem.l_suppkey | 2.236 | Q5, Q7, Q8, Q9, Q15, Q20, Q21 |
| lineitem.l_extendedprice | 2.236 | Q1, Q3, Q5, Q6, Q7, Q8, Q9, Q10, Q14, Q15, Q17, Q19 |
| lineitem.l_receiptdate | 1.204 | Q4, Q12, Q21 |
| lineitem.l_shipdate | 1.204 | Q1, Q3, Q6, Q7, Q12, Q14, Q15, Q20 |
| lineitem.l_commitdate | 1.202 | Q4, Q12, Q21 |
| lineitem.l_quantity | 1.118 | Q1, Q6, Q9, Q17, Q18, Q19, Q20 |
| customer.c_comment | 1.093 | Q10 |
| part.p_name | 0.722 | Q9, Q20 |
| lineitem.l_shipinstruct | 0.559 | Q19 |
| lineitem.l_shipmode | 0.559 | Q12, Q19 |
| lineitem.l_discount | 0.559 | Q1, Q3, Q5, Q6, Q7, Q8, Q9, Q10, Q14, Q15, Q19 |
| lineitem.l_tax | 0.559 | Q1 |

## Repeated smaller columns

The following columns each occupy at most 64 MiB on disk and appear in at
least two queries. Together they occupy 283.619 MiB.
This is a candidate set for measurement, not a proven decoded-cache budget.
String objects and allocations can make decoded sizes substantially larger.

| Column | Native MiB | Query count |
|---|---:|---:|
| customer.c_custkey | 57.227 | 8 |
| customer.c_nationkey | 14.309 | 4 |
| customer.c_acctbal | 57.227 | 2 |
| nation.n_nationkey | 0.000 | 9 |
| nation.n_name | 0.000 | 9 |
| nation.n_regionkey | 0.000 | 3 |
| part.p_brand | 19.165 | 3 |
| part.p_type | 20.154 | 4 |
| part.p_size | 19.079 | 3 |
| part.p_container | 19.212 | 2 |
| region.r_regionkey | 0.000 | 3 |
| region.r_name | 0.000 | 3 |
| supplier.s_suppkey | 3.815 | 10 |
| supplier.s_name | 22.888 | 4 |
| supplier.s_address | 29.561 | 3 |
| supplier.s_nationkey | 0.954 | 8 |
| supplier.s_phone | 20.027 | 2 |

## Work implied by this inventory

1. Prototype an explicitly byte-admitted cache of original snapshot columns,
   starting with repeatedly used small dimensions. Measure owned buffers,
   dictionary capacities and object overhead; on-disk size is not admission.
   Validate snapshot identity before reuse and keep row alignment with cached
   siblings. Release entries before replacements if the byte limit requires it.
2. Inject cached original columns before selection and value transformation,
   so consumers can apply their own SQL semantics. `LoadDirectTable` and
   `ColumnarTable::ScanDirect` are the insertion points; caching outputs of
   `PrepareDirectQuery` would keep the current statement-specific limitation.
   Cache hits must still execute the original joins and aggregates.
3. Benchmark meaningful transitions, including a repeated small dimension and
   a large fact scan. Include cache residency in the same external 8 GiB cap.
   Track hits and bytes avoided, and verify every answer after eviction and
   snapshot replacement. The read-volume gap remains even if small dimensions
   become free to reload.
4. Investigate a compact/searchable representation for `orders.o_comment`.
   It is 7.589 GiB on disk and used only by Q13 in this workload, so inter-query
   reuse cannot remove its first appearance's cost. Measure compression and
   exact predicate evaluation together; a persistent index must support the
   predicate generically and keep verification costs inside the measured call.

The first cache experiment and comments representation address different
costs. Neither alone is claimed to achieve the full timing goal. The
[latest full timings](TRANSITION-STRINGS.md) and
[bandwidth scenario](READ-VOLUME-GAP.md) remain the comparison boundaries.

## Reproduction and checks

Build `native_inventory` with CMake, then run:

```sh
python benchmarks/tpch/memory-8g-20260908/inventory_inputs.py \
  build/native_inventory /home/hadi/projects/tpch-work/sf100/native \
  /home/hadi/projects/tpch-work/queries-duckdb-1.4.3 \
  --output /tmp/native-input-inventory-sf100.json
```

[SF100 evidence](native-input-inventory-sf100.json) and
[SF1 evidence](native-input-inventory-sf1.json) record tool and SQL hashes,
snapshot metadata identities and all per-query/per-column spans. All 22
physical dependency and predicate-only classifications agree across scales.
Q6's four required columns and Q13's predicate-only comments were checked
explicitly; invalid SQL is rejected. Legacy snapshots have no versioned
identity, which a future cache must handle explicitly rather than treating
missing identity as a reusable key.

[Catalog script](inventory_inputs.py). The source CLI is
`benchmarks/native_inventory.cc`; this is diagnostic tooling, not a runtime
cache implementation or a new benchmark timing result.
