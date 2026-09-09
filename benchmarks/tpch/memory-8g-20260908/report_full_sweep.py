#!/usr/bin/env python3
"""Verify a complete fixed-build sweep while keeping failures and follow-ups."""
import hashlib
import json
from pathlib import Path

root=Path(__file__).resolve().parent
baseline_path=root.parent/'simd-remaining-20260906/FINAL-ALL1.json'
baseline=json.loads(baseline_path.read_text())['scales']['100']['engines']
engines={name:{r['id']:r for r in engine['queries']} for name,engine in baseline.items()}
def read(path):
    record=json.loads(path.read_text())
    raw=Path(record['raw_file']);assert hashlib.sha256(raw.read_bytes()).hexdigest()==record['raw_sha256']
    result=dict(query=record['query'],module_sha256=record['module_sha256'],source=str(path),returncode=record['returncode'])
    if record['returncode']==0:
        assert record['correct'] and record['memory_verified'] and record['reuse_verified'],path
        m=record['measurements'];assert len(m['trials'])==9 and m['memory_max']==8*2**30 and m['swap_max']==0
        assert m['cgroup_peak_bytes']<=8*2**30
        result.update(warm_client_ms=record['median_client_ms'],first_client_ms=record['first_client_ms'],
            peak_cgroup_bytes=m['cgroup_peak_bytes'],peak_query_bytes=max(t['peak_memory_bytes'] for t in m['trials']),
            correct=True,memory_verified=True)
    else:
        result.update(correct=False,memory_verified=False,error=record['stderr'])
    return result
fixed=[read(root/f'native-direct-full-membership-q{n}-sf100.json') for n in range(1,23)]
assert len({r['module_sha256'] for r in fixed})==1
followup=read(root/'native-direct-common-filter-q19-sf100.json');assert followup['correct']
latest=[followup if r['query']==19 else r for r in fixed]
assert all(r['correct'] for r in latest)
summary=dict(note='Fixed-build sweep retained, including its Q19 failure. Q19 has a separately identified corrected-build follow-up. Nine native Python calls per successful query, each in a fresh 8 GiB zero-swap cgroup. Warm median excludes first preparation. Peers are saved measurements, not reruns; this is not a query-transition test.',
    baseline=str(baseline_path),baseline_sha256=hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
    fixed_build=fixed,followups=[followup],latest=latest,previous=engines)
(root/'full-membership-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
lines=['# SF100 direct-memory sweep and Q19 follow-up','',summary['note'],'',
       'The fixed build passed 21 queries and was OOM-killed on Q19. Its follow-up uses a newer preparation rule for literals shared by every OR branch. All 198 successful trials pass the typed oracle and verify live memory limits. The 768 MiB query allowance stays inside the 8 GiB total cap.','',
       '| Query | Previous pgaccel ms | Direct warm ms | ClickHouse ms (saved) | DuckDB ms (saved) | Peak cgroup GiB | First call s |',
       '|---|---:|---:|---:|---:|---:|---:|']
for r in latest:
    n=r['query'];label=f'Q{n}'+(' (follow-up)' if n==19 else '')
    lines.append(f"| {label} | {engines['pgaccel-native'][n]['median_client_ms']:.3f} | {r['warm_client_ms']:.3f} | {engines['clickhouse'][n]['median_client_ms']:.3f} | {engines['duckdb'][n]['median_client_ms']:.3f} | {r['peak_cgroup_bytes']/2**30:.3f} | {r['first_client_ms']/1000:.3f} |")
slower=[r['query'] for r in latest if r['warm_client_ms']>engines['pgaccel-native'][r['query']]['median_client_ms']]
lines+=['',f'Warm medians above the previous pgaccel measurement: {", ".join("Q"+str(n) for n in slower)}. No new tolerance threshold is being used to declare completion.',
        '',f"Sum of latest warm medians: {sum(r['warm_client_ms'] for r in latest):.3f} ms, previously {sum(r['median_client_ms'] for r in engines['pgaccel-native'].values()):.3f} ms. This arithmetic sum combines the identified builds and is not a measured workload-transition runtime.",
        '', 'The full objective remains unproven. First calls include substantial query-specific preparation, cache transitions and admission are unfinished, and regressions need investigation. Saved peer RSS values are retained in the JSON but exclude file cache and are not interchangeable with cgroup peaks.','']
(root/'FULL-SWEEP.md').write_text('\n'.join(lines))
print(json.dumps(dict(queries=len(latest),slower=slower,peak_gib=max(r['peak_cgroup_bytes'] for r in latest)/2**30)))
