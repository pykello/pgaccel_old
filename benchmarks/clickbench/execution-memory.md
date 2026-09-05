# Exact aggregation memory and spill

Hash maps, aggregate vectors, and exact sets use a shared per-query PMR
resource. Its high-water mark includes actual allocator requests for nodes
and buckets; temporary dense arrays and result buffers are charged separately.
It excludes the resident table, allocator metadata/fragmentation, thread
stacks, and library runtime allocations. It is not a process RSS limit.

Grouped counts store one 64-bit counter per key. Other grouped queries omit
projection slots from aggregate state. Worker merges partition keys by hash
owner and transfer unique nodes. A single global exact distinct partitions
values themselves so owners can merge independently without approximation.

On an aggregate memory-limit failure, spill restarts the query as an external
sort of resident row references. Runs sort by the grouping key and, when
present, the exact distinct argument. A bounded 16-way leveled merge feeds
one group at a time to the final aggregation. Temporary directories are private
and removed on success or exceptions. Disk write failures return errors.

Spill currently supports at most one distinct argument per query and requires
at least 1 MiB of query memory. Several exact distinct aggregates still work
in memory; an oversized such query returns an explicit error. Results without
a restrictive LIMIT must themselves fit the query budget. Spill uses the
system temporary directory and limits cumulative bytes written, including
merge passes, to 64 GiB by default. No approximate fallback is used.

The REPL accepts `set memory_mb N`, `set spill on|off`, and `set spill_mb N`.
The resident query runner accepts `PGACCEL_QUERY_MEMORY_BYTES` and
`PGACCEL_SPILL_BYTES`. Its JSON includes `query_peak_allocation_bytes`,
`spill_bytes_written`, and `process_peak_rss_bytes`. The last is the process
lifetime high-water RSS, including native loading and earlier queries; use a
fresh process per query when comparing isolated process peaks. A zero query
allocation metric on a specialized scalar path means it does not allocate
tracked aggregate state, not that the process uses zero memory.
