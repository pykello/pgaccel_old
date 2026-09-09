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
