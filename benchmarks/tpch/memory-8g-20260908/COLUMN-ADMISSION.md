# Admission accounting for original native columns

`39ae18f` adds NativeColumnCharge for unmodified blocks obtained from the
native loader. It counts the concrete column object, shared-ownership/type
allowances, 512-byte-rounded value allocations, dictionary vector capacity
and heap-backed string capacity. Inline string characters are already inside
the string object. Fixed allocation allowances make this an admission model,
not an exact allocator or process-RSS measurement.

The caller must establish native-loader provenance. The helper does not
validate data contents or prove that rows and values are original. It refuses
unsupported concrete layouts, mismatched logical types, invalid dimensions,
external dictionary/signature/id views and nonzero global row offsets. Limit
checks use overflow-safe additions and return no charge when the budget is
insufficient. Cache keys, containers and active query state need their own
accounting. All 296 native tests pass, including exact-limit failure/success,
retained string/vector capacity and unsupported auxiliary views.

## Measured candidate set

The [inventory](INPUT-INVENTORY.md) selected 17 columns used by at least two
TPC-H queries, each at most 64 MiB on disk. The diagnostic retains every
original decoded block, preventing scratch-slot recycling, and holds all
columns simultaneously. It runs under MemoryMax=8G and MemorySwapMax=0.

- Native column spans: **283.619 MiB**.
- Block admission charges: **479.175 MiB**.
- Retained pointer vector capacity: **32.0 KiB**.
- Process peak RSS: **479.496 MiB**.
- Live cgroup peak, including the measurement wrapper: **485.645 MiB**.
- Recorded direct-read bytes: **472.022 MiB**.

All recorded limit and OOM event counts are zero. Read bytes exceed original
spans because of aligned/speculative reads. No query is executed in this
measurement; it does not establish a query-transition memory peak or speedup.

| Original column | Native MiB | Admission MiB | Blocks |
|---|---:|---:|---:|
| customer.c_acctbal | 57.227 | 57.270 | 229 |
| customer.c_custkey | 57.227 | 57.270 | 229 |
| customer.c_nationkey | 14.309 | 14.352 | 229 |
| nation.n_name | 0.000 | 0.002 | 1 |
| nation.n_nationkey | 0.000 | 0.001 | 1 |
| nation.n_regionkey | 0.000 | 0.001 | 1 |
| part.p_brand | 19.165 | 19.415 | 306 |
| part.p_container | 19.212 | 19.555 | 306 |
| part.p_size | 19.079 | 19.137 | 306 |
| part.p_type | 20.154 | 24.740 | 306 |
| region.r_name | 0.000 | 0.001 | 1 |
| region.r_regionkey | 0.000 | 0.001 | 1 |
| supplier.s_address | 29.561 | 107.196 | 16 |
| supplier.s_name | 22.888 | 123.030 | 16 |
| supplier.s_nationkey | 0.954 | 0.957 | 16 |
| supplier.s_phone | 20.027 | 32.431 | 16 |
| supplier.s_suppkey | 3.815 | 3.818 | 16 |

The candidate set's charged blocks fit within 512 MiB, with room remaining
for cache-entry metadata. That is a useful prototype budget, not proof that
adding the cache to every query stays under 8 GiB. Future transition runs must
include both cached and active references, allocator retention, read buffers
and query state. An eviction removes cache ownership but may not free a block
still referenced by the active query; admission must not confuse those cases.

## Next implementation

1. Capture and validate versioned snapshot identity before cache lookup. Keep
   legacy/unidentified snapshots on the direct fallback until an explicit
   identity policy exists. A cache hit must not bypass header validation.
2. Add byte-admitted ownership and eviction for original columns or blocks,
   charging entry/container storage in addition to NativeColumnCharge.
   Establish provenance at the loader boundary before any consumer can
   compact rows, transform values or attach external indexes.
3. Supply hits to the existing direct preparation path before selection and
   transforms. Verify row alignment, retained-reference safety, eviction,
   snapshot replacement and exact query answers, then benchmark transitions
   with the same enforced total cap. No query result or aggregate is cached.

The runtime cache is not wired in yet. This commit establishes and measures
its admission foundation; the broader performance goal remains open.

Reproduce with `native_column_charge` built by CMake and:

```sh
python benchmarks/tpch/memory-8g-20260908/measure_column_charge.py \
  build/native_column_charge \
  benchmarks/tpch/memory-8g-20260908/native-input-inventory-sf100.json
```

[Evidence](native-column-charge-sf100.json) records the tool/inventory hashes,
all charges, native spans, process RSS, live cgroup peak and limits.
[Measurement harness](measure_column_charge.py).
