from pathlib import Path
import datetime,hashlib,json,statistics
root=Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906')
mirror=Path('benchmarks/tpch/simd-remaining-20260906')
checkpoint=Path('/home/hadi/projects/tpch-work/checkpoints/final-all-v1/CHECKPOINT.json')
twofold={7,8,9,10,21,22}
evidence={'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checkpoint':json.loads(checkpoint.read_text()),'target_by_query':{str(q):2 if q in twofold else 3 for q in range(1,23)},'scales':{}}
module_hashes=set();totals={};tables={};smallest={}
for scale,repeats in [(10,15),(100,9)]:
 engines={};times={}
 for engine in ['pgaccel-native','duckdb','clickhouse']:
  path=root/f'{engine}-final-all1-sf{scale}.jsonl';raw=path.read_bytes();data=list(map(json.loads,raw.splitlines()));metadata=data[0]
  assert metadata['settings']['threads']==8
  assert len(data)==23 and {q['id'] for q in data[1:]}==set(range(1,23))
  if engine=='pgaccel-native':module_hashes.add(metadata['module_sha256'])
  records=[];times[engine]={}
  for q in data[1:]:
   assert q['correct'] and len(q['trials'])==repeats and all(t['correct'] for t in q['trials']), (scale,engine,q['id'])
   values=[t['client_ms'] for t in q['trials']];median=statistics.median(values[1:]);times[engine][q['id']]=median
   records.append({'id':q['id'],'sql_sha256':q['sql_sha256'],'correct':True,'rows':len(q['trials'][0]['rows']),'median_client_ms':median,'trial_client_ms':values,
      'trial_execution_ms':[t.get('execution_ms') for t in q['trials']],
      'trial_query_peak_bytes':[t.get('query_peak_allocation_bytes') for t in q['trials']],
      'trial_spill_bytes':[t.get('spill_bytes_written') for t in q['trials']],
      'peak_process_rss_bytes':max(t.get('rss',{}).get('VmHWM',0) for t in q['trials'])})
  engines[engine]={'metadata':metadata,'raw_file':str(path),'raw_sha256':hashlib.sha256(raw).hexdigest(),'queries':records}
 totals[scale]={engine:sum(values.values()) for engine,values in times.items()}
 table=['| Query | Target | PGAccel ms | DuckDB ms | ClickHouse ms | DuckDB / PG | CH / PG |','|---|---:|---:|---:|---:|---:|---:|'];ratios=[]
 for q in range(1,23):
  a,b,c=[times[e][q] for e in ['pgaccel-native','duckdb','clickhouse']];target=2 if q in twofold else 3
  assert min(b,c)>=target*a,(scale,q,a,b,c,target)
  ratios.append(min(b,c)/a)
  table.append(f'| Q{q} | {target}x | {a:.3f} | {b:.3f} | {c:.3f} | {b/a:.3f}x | {c/a:.3f}x |')
 tables[scale]=table;smallest[scale]=min(ratios)
 evidence['scales'][str(scale)]={'engines':engines,'sum_warm_medians_ms':totals[scale],'all_targets_pass':True}
assert len(module_hashes)==1 and next(iter(module_hashes))==evidence['checkpoint']['module_sha256']
runtime=evidence['checkpoint']['runtime_revision'];module_hash=next(iter(module_hashes))
lines=['# Full TPC-H comparison on one frozen native module','','All 22 queries meet their assigned speed target against both DuckDB and','ClickHouse at SF10 and SF100. Q7, Q8, Q9, Q10, Q21 and Q22 require at','least 2x; the other 16 require at least 3x. Every recorded trial passes','answer validation. This replaces the earlier mixed-checkpoint status as','the current full-workload comparison.','','PGAccel uses the native Python API for every query at both scales. Complete','client calls include parsing, execution, fetching/converting results and','native result cleanup. These are warm resident-data timings; initial data','loading is reported separately in the raw metadata and is not included.','','## Workload totals','','Sums of the 22 per-query warm medians, not the elapsed duration of one','continuous workload run. No failed or slow query is excluded.','','| Scale | PGAccel s | DuckDB s | ClickHouse s | DuckDB / PG | CH / PG |','|---|---:|---:|---:|---:|---:|']
for scale in [10,100]:
 a,b,c=[totals[scale][e]/1000 for e in ['pgaccel-native','duckdb','clickhouse']]
 lines.append(f'| SF{scale} | {a:.3f} | {b:.3f} | {c:.3f} | {b/a:.3f}x | {c/a:.3f}x |')
for scale in [10,100]:lines+=['',f'## SF{scale}','','Client-observed milliseconds; warm medians exclude only trial one.','']+tables[scale]
lines+=['','## Method and limits','','- Fifteen trials per query/engine at SF10, nine at SF100. The first trial','  is excluded only from timing medians; all trials are validated.','- Engines run sequentially with eight workers. Builds and tests do not','  overlap timed phases. The CPU governor and affinity are unchanged.','- SF100 services use a 192 GiB cgroup cap with swap disabled. PGAccel','  uses a 64 GiB query allocation budget. Cgroup peaks include charged','  cache and differ from process RSS or query allocations.','- Integer/decimal values and strings/dates are compared exactly. Floating','  results use max(1e-6 absolute, 1e-12 relative) tolerance. DuckDB supplies','  the typed oracle; its repeated answers are checked for consistency.','- ClickHouse uses MergeTree and the documented dialect adapters for','  floating ratios in Q8/Q14/Q17. Settings and SQL hashes are retained.','- No result cache or optional PGAccel index build is used. All join proofs,','  payload construction, temporary allocations and cleanup remain timed.','- Q11 returns no rows at SF10 and SF100. Its nonempty SF1 case is also','  validated; its scale timings should be interpreted with that result size.','- These are engineering measurements, not audited TPC Benchmark Results.','','## Provenance','','Runtime source revision: `'+runtime+'`.','','Module SHA256: `'+module_hash+'`.','','[Compact trial evidence](FINAL-ALL1.json) contains metadata, every client','timing, query memory/spill diagnostics, raw-file paths and raw-file hashes.','The full answers remain in the six `*-final-all1-sf*.jsonl` files. SF100','commands and resource samples are in `final-all1-sf100-phases.json` and','`final-all1-sf100-resources.jsonl`. Reproduction scripts and test logs are','in [final-all1-reproduce](final-all1-reproduce/README.md).','','[Revised targets](ALL-QUERY-GOAL.md) · [Q8 design](Q8-DESIGN.md) ·','[Q9 design](Q9-DESIGN.md) · [Q10 design](Q10-DESIGN.md) ·','[Q21 design](Q21-DESIGN.md) · [Q22 design](Q22-DESIGN.md)','']
for p in [root,mirror]:
 (p/'FINAL-ALL1.md').write_text('\n'.join(lines));(p/'FINAL-ALL1.json').write_text(json.dumps(evidence,indent=2)+'\n')
print(json.dumps({'totals_ms':totals,'minimum_speedup_over_faster_peer':smallest,'runtime':runtime,'module_sha256':module_hash},indent=2))
