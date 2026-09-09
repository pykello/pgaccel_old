# Direct preparation: measure CPU work before adding asynchronous I/O

Temporary instrumentation times each synchronous `pread` and the selected
loader's selector, compaction and transformation phases. The
[patch](preparation-profile.patch) applies to `ada25af`; it is not retained in
production. The frozen module hash is recorded with each trial. Clock calls
and diagnostic output perturb these measurements, so they identify phases
rather than replace the uninstrumented full comparison.

Both queries pass nine typed SF100 answers in fresh processes capped at
8 GiB total memory with zero swap. All payload reads remain `O_DIRECT`.

| Query | First call s | Summed pread wall time s | Peak total GiB |
|---|---:|---:|---:|
| Q1 | 28.534 | 2.887 | 6.939 |
| Q13 | 36.032 | 3.695 | 1.852 |

Q1's selected lineitem load takes 28.041 seconds: selection 1.068,
compaction 20.087, and transforms less than 0.001. The remainder includes
synchronous reads, native decoding, allocation and bookkeeping. Direct read
time is summed over all input objects, including schema reads; it must not be
added to the loader time, which already includes its payload reads.

Q13's selected orders load takes 35.357 seconds: selection 0.032,
compaction 0.164, and transformations 5.254. The large remaining cost includes
native string decoding and allocation as well as reads. This first profile
does not separately isolate each of those remaining CPU operations.

The next experiment parallelizes independent Q1 column copies within one
stripe using the existing eight-worker arena. Selection and transformation
callbacks remain serial because they may share mutable state. This addresses
the measured dominant phase without additional read buffers. Q13 needs a
separate decoding/transform investigation; compaction is not its bottleneck.
An asynchronous reader alone cannot remove either query's CPU preparation.

Evidence: [Q1](native-direct-prep-profile-q1-sf100.json),
[Q13](native-direct-prep-profile-q13-sf100.json). These records retain diagnostic
stderr and hashes/locations of every complete answer. Ignore the post-exit
systemd memory summary; the table uses the live cgroup peak read by the worker.

## First retained experiment: parallel column compaction

`290bcb5` runs independent column copies in the existing eight-worker task
arena when the caller enables parallel execution, the selection has at least
8192 rows, and there is more than one column. Full-row selections still retain
source buffers directly. Small selections use serial copies. Each task owns
its output; the caller waits for all tasks before transforming or releasing
the stripe. Selectors and transformations remain on the calling thread.
Python and query_runner propagate their existing parallel-execution setting.

All 281 native tests, all 22 SF1 answers, and Python integration pass. A new
large-stripe test compares serial and parallel selected rows, checks numeric
and dictionary columns together, and checks callback thread affinity.

| Query | First call s | Warm median ms | Peak total GiB |
|---|---:|---:|---:|
| Q1 | 25.615 | 357.745 | 6.869 |
| Q12 | 11.016 | 208.356 | 1.293 |

Both [Q1](native-direct-parallel-compact-q1-sf100.json) and
[Q12](native-direct-parallel-compact-q12-sf100.json) pass all nine SF100 answers
under 8 GiB with zero swap. Q1's earlier uninstrumented dense-active first
call was 28.184 seconds (28.390 in the complete sweep). This is a modest
preparation improvement; these few observations do not establish a stable
percentage gain. Q12's highly selective stripes largely retain the serial
policy. These isolated measurements do not replace the complete-sweep table.
The next check covers allocator retention across different SQL statements.

## Query transitions after parallel compaction

The [five-step transition run](native-transitions-parallel-compact-sf100.json)
executes Q1, Q9, Q12, Q21, then Q1 again in one long-lived database. All 45
answers pass; every SQL change rereads input and every repeated call reuses
only prepared columns. The cumulative live cgroup peak is **6.890 GiB**,
with zero sampled file charge, zero swap and no memory-limit or OOM events.

| Step | Query | First call s | Warm median ms |
|---|---|---:|---:|
| 1 | Q1 | 25.845 | 356.755 |
| 2 | Q9 | 20.191 | 1565.388 |
| 3 | Q12 | 11.150 | 171.459 |
| 4 | Q21 | 17.089 | 640.237 |
| 5 | Q1 | 26.170 | 369.369 |

This proves bounded residency for this transition sequence, not every query
or arbitrary input. Q9 and Q21 warm times are slower than the previous full
sweep but remain below the saved fully resident pgaccel medians. Keep these
observations intact. The complete 22-query report still refers to the earlier
module; a later complete sweep must use one final build after further work.

## Decode dictionaries in final storage

`bfdc927` resizes the dictionary from its validated entry count and decodes
values directly into those slots. Previously every string was decoded into a
temporary, copied into the growing vector, then destroyed. Long strings could
therefore require two heap allocations and a payload copy per entry. The new
path constructs their storage once. Numeric dictionary entries are read as a
contiguous block. Dictionary ordering, uniqueness, code bounds, coverage,
string length and truncation checks remain in place; the disk format is unchanged.

All 281 native tests, all 22 SF1 answers and Python integration pass. A fresh
Q13 control and candidate run were performed sequentially with eight workers,
8 GiB total memory, zero swap and nine typed-checked answers each.

| Run | First call s | Warm median ms | Peak total GiB |
|---|---:|---:|---:|
| Q13 control | 35.907 | 541.957 | 1.851 |
| Q13 final-slot decoding | 30.708 | 544.051 | 1.859 |
| Q1 final-slot decoding | 25.824 | 360.103 | 6.874 |

The Q13 first call improves by about 14%; warm execution is essentially
unchanged. Q1 also passes all nine answers and remains close to its preceding
parallel-compaction measurements. This reduces preparation CPU work without
adding buffers or asynchronous I/O. It does not solve changing-query latency.

Evidence: [Q13 control](native-direct-dict-slots-control-q13-sf100.json),
[Q13 candidate](native-direct-dict-slots-q13-sf100.json),
[Q1 candidate](native-direct-dict-slots-q1-sf100.json),
[SF1 gate](auto-projection-sf1-dict-slots-gate.json).
