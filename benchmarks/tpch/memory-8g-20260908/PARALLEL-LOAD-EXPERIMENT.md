# Rejected experiment: concurrent stripe-column loading

Based on `30bbb18`, this experiment passes the existing parallel preparation
flag to the direct scanner. Each column loads and validates through its own
stream in the existing eight-worker arena. Completion joins all columns
before the serial selector/consumer runs. It adds no prefetch buffer or
io_uring dependency and retains O_DIRECT. API defaults remain serial.

The experiment passes 289 native tests, Python integration, all 22 SF1
answers and all 18 SF100 answers. Tests cover projected buffer accounting,
consumer errors and mismatched column lengths in both execution modes.
The initial malformed-input fixture was rejected by the fixture helper;
the saved patch corrects it to write a malformed snapshot directly.

| Query/build | First call s | Warm median ms | Total peak GiB |
|---|---:|---:|---:|
| Q1 byte-usage | 11.465 | 419.480 | 6.904 |
| Q1 parallel-load | 11.240 | 413.471 | 6.906 |
| Q13 byte-usage | 29.776 | 548.904 | 1.866 |
| Q13 parallel-load | 76.008 | 544.749 | 1.817 |

These are successive fresh-process runs, nine calls each, with a total
8 GiB cap and zero permitted swap. Controls are the preceding byte-flag
runs. The candidate gives only a 2% Q1 first-call improvement but makes
Q13 about 2.55 times slower. Reject the implementation and restore serial
column loading. Warm samples remain recorded without substitution.

The result does not isolate storage concurrency from thread scheduling,
allocation or cache costs. It does not establish that io_uring would be
slow. A later reader experiment should overlap bounded reads while leaving
decoding on the caller, and separately measure read-wait time. Do not repeat
this column-task design as evidence for asynchronous-I/O performance.

The [patch](parallel-load-experiment.patch) includes source and tests.
Evidence: [Q1](native-direct-parallel-load-q1-sf100.json),
[Q13](native-direct-parallel-load-q13-sf100.json),
[SF1 gate](auto-projection-sf1-parallel-load-gate.json).
The experimental module is frozen and hashed in those records. Production
source and the Python module are restored to the validated byte-flag build.
The complete warm milestone and the changing-query latency gap remain as
reported in [TRANSITION-CHAINS.md](TRANSITION-CHAINS.md).
