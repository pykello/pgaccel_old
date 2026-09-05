# Experiment 32: remove redundant serial buffer initialization

Keep the overwrite-buffer change. Large radix scatter buffers previously
used vector value-initialization, zeroing their entire allocation on the
calling thread before workers overwrote every record. The new budgeted
buffer default-constructs trivial objects without initializing their values.
Every scatter destination is assigned before any read.

Filtered inverse distinct still needs zero for unselected rows. Each worker
now initializes only its own stripes before writing selected keys. Unfiltered
input needs no zero pass because every row receives a group identity.

The buffer uses standard C++ object-lifetime operations and the existing
query memory resource. It accepts only trivially default-constructible and
trivially destructible types. It checks allocation-size overflow, transfers
ownership on move, and releases the exact allocation at destruction.
No uninitialized element is intentionally read.

## Full-data isolated follow-up

Engine 904d1de. Full data, eight threads, original SQL, minimum client
milliseconds of trials two and three. The preceding subset is the baseline
where available; Q19, Q34, and Q35 use indexed checkpoint 2.

| Query | Before overwrite buffers | After | Fresh ClickHouse |
| ---: | ---: | ---: | ---: |
| 9 | 469.521 | 298.610 | 670.641 |
| 10 | 722.902 | 564.103 | 762.971 |
| 11 | 279.841 | 99.210 | 173.796 |
| 12 | 307.295 | 103.030 | 219.490 |
| 14 | 542.777 | 358.880 | 618.364 |
| 17 | 1724.775 | 1016.195 | 1364.565 |
| 19 | 2095.656 | 1369.182 | 2420.274 |
| 31 | 471.183 | 291.934 | 292.493 |
| 34 | 390.431 | 287.893 | 2269.064 |
| 35 | 393.995 | 278.389 | 2331.246 |

Q11, Q12, and Q17 become clear wins. Q31 is almost tied with ClickHouse;
the 0.56 ms margin is too small to rely on for the final objective without
another full comparison. Other measured queries improve existing wins.

These gains are consistent with eliminating a large serial initialization
pass and distributing required first writes. No hardware-counter attribution
is claimed. The buffer contents and exact query answers remain unchanged.

All 30 trial answers pass validation. All 77 native tests pass, including
allocation overflow, ownership transfer and release, exact scatter results,
filtered empty selections, and mixed decimal aggregation. The previous full
checkpoint's sanitizer result predates this change; a new sanitizer pass
is required before the next final checkpoint.

Evidence: overwrite-buffers.jsonl, overwrite-buffers.meta.json,
overwrite-buffers-gate.json. Frozen binary:
../clickbench-work/research-20260905/bin/overwrite-buffers-query-runner.
This original implementation adds no dependency.
