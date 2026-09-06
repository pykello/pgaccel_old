# Q20 capacity recheck — complete

Q20 also failed at an 80 GiB query-allocation allowance. It reported
`Membership subquery: Scalar subquery: Query memory budget exceeded`
before completing a trial. This was an allocator rejection, not a service
OOM. The driver exit code is zero because query failures are recorded in
its JSONL; it does not mean Q20 succeeded.

The frozen binary, original SQL, native data, and 192 GiB service cap were
unchanged. The service peaked at 184.68 GiB including file cache, with
zero cgroup max/OOM/OOM-kill events and no swap. The observed query process
VmHWM was 166.42 GiB. No validated latency is available.

The 64 GiB baseline remains 21/22 for PGAccel. Raising the allowance again
would leave too little capacity headroom; restrict correlated aggregate
key domains before repeating this experiment.

Execution: 2026-09-06 03:22:16–03:32:57 UTC. Raw evidence:
`q20-capacity-80g.jsonl`, `q20-capacity-80g.log`,
`q20-capacity-resources.jsonl`, and `q20-capacity-phase.json` in the external
SF100 report folder.
