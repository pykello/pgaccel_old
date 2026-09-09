# Original-block reuse across SQL changes

The direct scan now accepts a `NativeBlockCache`. The Python database exposes
an optional `cache_bytes` constructor argument, defaulting to zero. This is an
explicit admission budget for original blocks, not a total memory limit. The
TPC-H transition experiment requests 512 MiB while the whole worker remains
inside an 8 GiB cgroup with swap disabled.

## Scan behavior

A versioned snapshot UUID and opened-descriptor stamp identify the source.
Separately opened column streams must match the header descriptor. Column start,
stripe number and logical type complete the lookup key. Legacy input stays
uncached. Admission is limited to physical columns with native spans of at most
64 MiB; this is a source-size heuristic rather than a query-name whitelist.

A miss decodes and validates the original stripe using the existing direct
reader. Successful admission retains that block and the stream's ending offset.
A hit seeks to this offset without rereading or revalidating unchanged payload.
This lets a scan mix hits and misses even for variable-length string stripes.
Normal cross-column row-count validation and callback early-stop behavior remain.

The public scan callback can mutate its columns. Therefore admitted sources and
hits are copied with `CompactNativeColumn` over all row IDs before reaching the
callback. Cached dictionaries never become predicate representatives or date
comparison outcomes. This deliberately adds a copy and extra transient memory;
the experiment must show whether avoided reading and decoding outweigh it.

The database retains its cache across SQL changes but still releases its previous
statement-specific prepared tables. Identical SQL reuses those prepared tables
and executes the query again. It is not a result cache. A changed SQL statement
still performs binding, selection and transformation; preparation remains inside
first-call timing. Closing the database clears both prepared tables and cache.

Each Python result includes `cache_hits`, `cache_misses`, and `cache_bytes`.
Hits and misses count eligible source stripes during that call's preparation;
bytes report current cache admission charge. Exact repeated SQL therefore has
zero cache lookups despite retaining cache contents. `direct_load_bytes` counts
actual direct reads, including headers and read-ahead amplification.

## Validation and reproduction

All 299 native tests pass, including partial hits after an early stop, variable
string lengths, consumer mutation, snapshot replacement and disabled admission.
Python integration verifies that predicate-only preparation followed by another
SQL query sees original strings. The default-path all-query SF1 gate also passes:
[auto-projection-sf1-cache-scan-gate.json](auto-projection-sf1-cache-scan-gate.json).

Run the existing transition harness with `--cache-mib 512`; omit the flag for a
zero-cache control. Both use one database, Q1 through Q22 in order and nine calls
per query. The frozen module is
`pgaccel_native-input-cache.cpython-312-x86_64-linux-gnu.so`, SHA-256
`f3a884fabfcec469822428890d817b25a3a014d39b92a1ec08710cdfbe5b8a08`.

The cache remains disabled by default pending the transition comparison.

## Enabled-cache result

All 198 SF100 answers pass. Total live cgroup peak is **7.385 GiB**, with zero
sampled file charge, zero swap allowance and no memory-limit/OOM events. The
cache charge peaks at 511.988 MiB. There are 5,341 stripe hits and 2,547 eligible
misses across the sequence. Total direct reads are 145.996 GiB, versus 146.878
GiB in the preceding no-cache sweep: only 0.883 GiB avoided.

The first-call sum is **153.500 seconds**, versus 146.768 seconds in that preceding
sweep. The warm median sum is 6.469 seconds; every query remains below the saved
pgaccel baseline. The cache is not justified as a default by this result.
[TRANSITION-CACHE.md](TRANSITION-CACHE.md) contains every query and saved peer time.
[native-transitions-input-cache-sf100.json](native-transitions-input-cache-sf100.json)
retains all measurements and the complete raw-answer location.

Per-column streams also change reading on misses compared with the existing
single-stream resident loader. Q2 reads 112 MiB more, and Q3/Q4 each read 16 MiB
more, than the preceding no-cache sweep. The observed deltas include both cache
savings and this read-ahead amplification; hit counts are not avoided byte counts.
The next control uses the same frozen binary with the cache disabled.

## Same-build control and decision

The zero-cache control also passes all 198 answers. Its first-call sum is
**148.678 seconds**, warm median sum is **6.289 seconds**, and peak memory is
**6.988 GiB**, with no sampled file charge or memory-limit/OOM events.
The enabled cache is 3.24% slower on the first-call sum and 2.86% slower on the
warm median sum, using 0.397 GiB more peak memory. Read savings are 0.60%.
These are two complete sequential runs, not an interleaved statistical study;
they establish no performance benefit that justifies enabling this prototype.

**Decision: retain the tested opt-in implementation, keep it disabled by default,
and do not count it as a performance improvement.** The memory/correctness
contracts work, but this policy spends too much memory and copying work for its
read reduction. Do not tune it by hiding preparation or selecting favorable
per-query samples. Before any further cache experiment, address miss read
amplification and mutable-output copying. The large uncached first-use inputs
remain the more substantial obstacle to end-to-end parity.

[TRANSITION-CACHE-CONTROL.md](TRANSITION-CACHE-CONTROL.md) is the latest complete
comparison for the default configuration against saved pgaccel, ClickHouse and
DuckDB results. Every warm median meets the saved pgaccel baseline. Raw control
evidence is [native-transitions-input-cache-control-sf100.json](native-transitions-input-cache-control-sf100.json).

| Query | No-cache first call s | Cache first call s | No-cache warm ms | Cache warm ms |
|---|---:|---:|---:|---:|
| Q1 | 8.466 | 8.346 | 377.521 | 385.851 |
| Q2 | 1.951 | 2.440 | 69.353 | 67.300 |
| Q3 | 7.478 | 7.707 | 227.005 | 226.758 |
| Q4 | 9.943 | 10.105 | 159.279 | 162.571 |
| Q5 | 7.696 | 7.921 | 178.861 | 176.616 |
| Q6 | 2.071 | 2.280 | 5.801 | 6.322 |
| Q7 | 7.816 | 7.729 | 377.239 | 381.828 |
| Q8 | 7.656 | 7.866 | 296.969 | 285.340 |
| Q9 | 16.691 | 16.844 | 1421.228 | 1458.379 |
| Q10 | 10.875 | 11.040 | 374.576 | 377.192 |
| Q11 | 2.812 | 2.811 | 19.485 | 18.977 |
| Q12 | 5.696 | 6.025 | 169.928 | 175.102 |
| Q13 | 13.018 | 13.420 | 506.340 | 509.293 |
| Q14 | 2.349 | 2.470 | 98.719 | 99.176 |
| Q15 | 2.657 | 2.774 | 65.555 | 65.694 |
| Q16 | 1.105 | 1.401 | 201.917 | 208.266 |
| Q17 | 4.886 | 5.418 | 208.411 | 212.080 |
| Q18 | 6.978 | 7.546 | 529.843 | 565.997 |
| Q19 | 8.168 | 8.313 | 103.409 | 105.141 |
| Q20 | 5.944 | 6.269 | 127.534 | 133.797 |
| Q21 | 12.380 | 12.565 | 560.546 | 653.233 |
| Q22 | 2.043 | 2.210 | 209.649 | 194.361 |
