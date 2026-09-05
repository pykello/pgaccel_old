# Experiment 1: direct Abseil numeric sets — rejected

Hypothesis: replacing per-value Cell hash nodes with contiguous uint64 keys
reduces allocator contention and memory traffic for exact distinct counts.
The prototype used Apache-2.0 Abseil `flat_hash_set` with the query PMR allocator.
The source diff is preserved in `rejected-abseil-flat-set.patch`.

The release build succeeded and 32/33 native tests passed. The existing
`Aggregation.DistinctUsesValuesAcrossDictionaries` test aborted with
`free(): invalid pointer` when its one-byte query budget made allocation throw.
This failure prevents benchmarking or accepting the implementation. Successful
large-memory runs would not establish that memory pressure remains safe.

The diagnosis matches the upstream [allocator-exception issue](https://github.com/abseil/abseil-cpp/issues/1581)
and current [header contract](https://github.com/abseil/abseil-cpp/blob/master/absl/container/flat_hash_set.h),
which explicitly says the container is not exception-safe. Pgaccel relies on
exceptions to release aggregate state and restart supported queries with spill.
The new dependency is removed. No Abseil container remains in execution.

Next experiment: original flat integer storage whose replacement allocation
is complete before its existing table changes. Check a forced growth failure
as well as scalar/parallel exactness and signed key boundaries.

Raw test log: `../docs/clickbench/research-20260905/rejected-abseil-tests.log`.
No performance claim is made for the rejected implementation.
