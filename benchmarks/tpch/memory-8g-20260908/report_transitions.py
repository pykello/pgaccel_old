#!/usr/bin/env python3
"""Report one frozen-build, long-lived SF100 database sweep."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
record = json.loads((root / 'native-transitions-full-scope-filter-sf100.json').read_text())
assert hashlib.sha256(Path(record['raw_file']).read_bytes()).hexdigest() == record['raw_sha256']
assert record['sequence'] == list(range(1, 23))
assert all(record[key] for key in ['complete', 'correct', 'memory_verified', 'reuse_verified'])
trials = record['measurements']['trials']
assert len(trials) == 198
samples = [t[phase] for t in trials for phase in ['before', 'after']]
assert all(m['max'] == 8 * 2**30 and m['swap.max'] == 0 and m['peak'] <= 8 * 2**30 for m in samples)
baseline = json.loads((root.parent / 'simd-remaining-20260906/FINAL-ALL1.json').read_text())['scales']['100']['engines']
engines = {name: {q['id']: q for q in engine['queries']} for name, engine in baseline.items()}
lines = ['# SF100: all 22 queries in one 8 GiB database', '',
    'One frozen native module ran Q1 through Q22 in one long-lived Database, with nine calls per query. All 198 answers pass the typed DuckDB oracle. The live cgroup enforced 8 GiB total memory and zero swap throughout. This supersedes the representative transition test for workload coverage, while preserving the older evidence.', '',
    f"Module SHA-256: `{record['module_sha256']}`.", '',
    'Changing SQL releases the previous input and performs new direct reading, filtering and compaction. Warm calls reuse only that statement-specific input; every call reparses and executes the SQL. First calls include preparation and client conversion. Saved peer measurements are not reruns or equivalent cold-read comparisons.', '',
    '| Query | Previous pgaccel ms | Warm pgaccel ms | Saved ClickHouse ms | Saved DuckDB ms | First call s | Ending memory GiB |',
    '|---|---:|---:|---:|---:|---:|---:|']
slower = []
for step in record['steps']:
    query = step['query']
    ts = [t for t in trials if t['step'] == step['step']]
    assert len(ts) == 9
    old, warm = engines['pgaccel-native'][query]['median_client_ms'], step['warm_client_ms']
    if warm > old:
        slower.append(query)
    lines.append(f"| Q{query} | {old:.3f} | {warm:.3f} | {engines['clickhouse'][query]['median_client_ms']:.3f} | {engines['duckdb'][query]['median_client_ms']:.3f} | {step['first_client_ms']/1000:.3f} | {ts[-1]['after']['current']/2**30:.3f} |")
lines += ['', f"Cumulative memory peak: **{max(m['peak'] for m in samples)/2**30:.3f} GiB**. Maximum sampled file-cache charge: {max(m['file'] for m in samples)} bytes. Maximum recorded memory-limit events: {max(m['events']['max'] for m in samples)}; OOM kills: {max(m['events']['oom_kill'] for m in samples)}. Ending memory is live total cgroup usage after the ninth call, not a per-query peak; the peak counter accumulates across the workload.", '',
    'Warm medians above the saved pgaccel baseline: ' + ', '.join(f'Q{q}' for q in slower) + '.', '',
    'The same-performance objective is not complete. These warm timings exclude SQL-specific preparation, and the rows retained for one query are not a general-purpose 8 GiB database cache. Internal admission and spill behavior for arbitrary inputs remain unfinished. Reported peer RSS and cgroup memory have different accounting boundaries.', '',
    '[Evidence](native-transitions-full-scope-filter-sf100.json) includes all trial timings, output hashes, module and SQL hashes, live memory samples, and the complete raw-answer location. [Harness](run_direct_transitions.py).', '']
(root / 'TRANSITION-SWEEP.md').write_text('\n'.join(lines))
print(json.dumps(dict(queries=22, slower=slower, peak_gib=max(m['peak'] for m in samples)/2**30)))
