# Benchmark protocol evaluation

The protocol revision is pinned in `protocol-upstream-commit.txt`, separately
from the older pinned SQL revision. The [official rules](https://github.com/ClickHouse/ClickBench/blob/ad1b81af3183be9a4de72e9497c21125aea71f86/README.md)
require three sequential trials per query; the hot value is the faster of
trials two and three. End-to-end query delivery and result transfer count,
and result caching is disabled. True-cold runs require process restarts and
OS-cache clearing. Published results also report loading time and data size.
The shared [driver](https://github.com/ClickHouse/ClickBench/blob/ad1b81af3183be9a4de72e9497c21125aea71f86/lib/benchmark-common.sh)
includes reload time in cold trials for memory-resident systems. It skips the
supplementary concurrency test for single-process engines without a shared
server. A scoreboard uses geometric means of per-query ratios shifted by
10 milliseconds and applies penalties to missing results.

## Local measurement

`warm.py` measures a persistent pgaccel runner over its pipe and ClickHouse
over a persistent local HTTP connection. Both produce complete JSON responses.
Client timers include SQL parsing, execution, serialization, and response
transfer. JSON decoding and independent answer validation occur after the
timer. Separate `kernel_ms` values retain the executors' internal timers.
Every trial's rows and status are preserved, along with process/allocation
metrics where the runner exposes them. Metadata includes the source revision,
runner and SQL hashes, resident load time, and engine version.

These measurements use eight workers/threads and a 32 GiB query allocation
budget on the existing full tables. They measure resident warm execution;
no OS caches are cleared and engines are not restarted between queries.
The first value is therefore an uncontrolled warm-up, not a cold measurement.
There is no query-result cache in pgaccel; ClickHouse's result cache is
explicitly disabled. Run the two engines sequentially, with compilation and
other benchmark jobs stopped, to reduce interference.

```sh
python3 benchmarks/clickbench/warm.py --engine pgaccel \
  --native /path/to/hits.pgaccel --oracle /path/to/clickhouse.jsonl \
  --output /path/to/pgaccel-warm.jsonl
python3 benchmarks/clickbench/warm.py --engine clickhouse \
  --oracle /path/to/clickhouse.jsonl --output /path/to/clickhouse-warm.jsonl
```

## Remaining work for a cold/submission setup

A dedicated-host reproduction should expose restart/load/query hooks for the
resident runner, measure native reloading within the first trial, collect
cold-cache results, and record fresh import time and on-disk size. This local
run does not establish those numbers or a concurrent-service result. The Q4
wide-average policy and independently verified tied outputs must accompany
any correctness claim. No result is submitted to the public leaderboard by
this workflow.
