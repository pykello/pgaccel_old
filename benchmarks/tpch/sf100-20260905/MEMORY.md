# SF100 storage, loading, and memory evidence

Run RESULTS.md together with this resource report. The benchmark uses a
192 GiB service hard limit and no swap. The host actually reports 251.35 GiB
RAM; an existing ClickHouse service occupies roughly 15 GiB outside the
benchmark service. Only one benchmark query engine runs at a time.

## Data and loading

SF100 contains 600,037,902 line-item rows and
150,000,000 orders. All eight table row counts were checked during import.

| Representation | Size (GiB) | Meaning |
|---|---:|---|
| Shared Parquet | 38.439 | Source file bytes |
| DuckDB native | 25.715 | Persistent database bytes |
| PGAccel native | 62.022 | Native data files, excluding tiny sidecars |
| ClickHouse MergeTree | 44.803 | system.tables total_bytes; active-part disk sizes are in the layout report |

DuckDB data generation took 1090.668 seconds before exports.
PGAccel's separate native imports totaled 1034.401 seconds.
The corrected PGAccel benchmark then preloaded all native tables in 207.608 seconds,
using 97.826 GiB RSS before queries.
ClickHouse's measured Parquet insert times totaled 369.168 seconds;
its server RSS before queries was 0.767 GiB.
DuckDB opens its persistent database and loads required blocks through its
buffer manager. Its database-open time is not an equivalent all-table preload.
First-use I/O remains inside its query timings. The report uses trials 2–3
for warm minima; source layouts and loading costs are explicitly different.

## Memory accounting

Process high-water RSS is in RESULTS.md. The following cgroup peaks also
include charged file cache and service child processes. They are not added
on top of RSS: the counters overlap. Peaks cover loading and query execution.

| Engine | Recorded service peak (GiB) | Limit (GiB) | Sampled swap peak (GiB) | max / oom / oom_kill events |
|---|---:|---:|---:|---|
| pgaccel | 163.876 | 192 | 0.000 | 0 / 0 / 0 |
| clickhouse | 150.306 | 192 | 0.000 | 0 / 0 / 0 |

DuckDB process VmHWM is the capacity evidence used here. Its systemd exit
log reported a 400.0K service peak, inconsistent with the process readings.
That log is retained, but this report does not treat its peak as reliable.
No unverified explanation is assigned to the discrepancy.

PGAccel's largest reported successful-trial query-allocation peak was 43.609 GiB.
This excludes resident input tables and differs from process RSS. Its
configured 64 GiB allowance is shared with nested execution through remaining
parent accounting. The separate 192 GiB service cap bounds the whole service.

PGAccel reported 0 spill bytes across the recorded successful trials.
DuckDB query-phase process wchar deltas totaled 0 bytes; the largest
retained temporary-file snapshot after a trial was 0 bytes.
These are diagnostics, not a standardized spill or disk-throughput benchmark.
An after-query temporary-file snapshot alone cannot detect files already deleted.
Process I/O can include protocol output or server background logging for the
other engines, so their wchar values are not directly equivalent spill counters.

## Interpretation and reproducibility

Compact join keys were retained before this comparison after focused SF10
memory measurements and complete SF1 correctness checks. The corrected run
also fixes propagation of explicit runner limits to child plans. The earlier
partial attempt is archived separately and excluded from final timings.

Whether this workload fits and performs acceptably must be judged from all
22 statuses and measured peaks, not SF100 naming or compressed file size.
See FOLLOW-UP.md for staged changes that still require implementation and
measurement. No future speedup is included in the comparison.

Raw evidence: data-manifest.json, native-manifest.json, clickhouse-load.json,
resource-samples.jsonl, phases.json, the three engine JSONL files, and logs.
The isolated ClickHouse instance is stopped after its phase; its working
memory is released. Shared Parquet and all three persistent native formats remain.
