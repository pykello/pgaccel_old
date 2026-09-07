from pathlib import Path
import json
root=Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906')
mirror=Path('/home/hadi/projects/pgaccel_old/benchmarks/tpch/simd-remaining-20260906')
engines=['pgaccel-native','duckdb','clickhouse']; labels=['PGAccel','DuckDB','ClickHouse']
data={}; overview=[]; details=[]; memory={}
def fmt(n,unit=2**30):return 'N/A' if n is None else f'{n/unit:.3f}'
def maximum(values):return max((x for x in values if x is not None),default=None)
resources=[json.loads(l) for l in (root/'final-all1-sf100-resources.jsonl').read_text().splitlines()]
for scale in [10,100]:
 data[scale]={};memory[str(scale)]={}
 for e,label in zip(engines,labels):
  rows=[json.loads(l) for l in (root/f'{e}-final-all1-sf{scale}.jsonl').read_text().splitlines()]
  meta=rows[0];records=rows[1:];data[scale][e]={q['id']:q for q in records}
  cg=[];swap=[];events=[]
  if scale==100:
   for sample in resources:
    if sample['phase']!=f'{e}-final-all1-sf100':continue
    props=dict(line.split('=',1) for line in sample['properties'].splitlines() if '=' in line)
    if props.get('MemoryPeak','').isdigit():cg.append(int(props['MemoryPeak']))
    if sample.get('memory.swap.current','').strip().isdigit():swap.append(int(sample['memory.swap.current']))
    if 'memory.events' in sample:events.append(sample['memory.events'])
  entry={'loaded_rss_bytes':meta.get('loaded_rss',{}).get('VmRSS'),'process_high_water_bytes':maximum(t.get('rss',{}).get('VmHWM') for q in records for t in q['trials']),'sampled_service_peak_bytes':maximum(cg),'sampled_swap_max_bytes':maximum(swap),'memory_events':list(dict.fromkeys(events)),'queries':{}}
  for q in records:
   entry['queries'][str(q['id'])]={'post_query_rss_bytes':[t.get('rss',{}).get('VmRSS') for t in q['trials']],'process_high_water_bytes':[t.get('rss',{}).get('VmHWM') for t in q['trials']],'query_peak_allocation_bytes':[t.get('query_peak_allocation_bytes') for t in q['trials']],'spill_bytes_written':[t.get('spill_bytes_written') for t in q['trials']]}
  memory[str(scale)][e]=entry
  overview.append(f'| SF{scale} | {label} | {fmt(entry["loaded_rss_bytes"])} | {fmt(entry["process_high_water_bytes"])} | {fmt(entry["sampled_service_peak_bytes"])} |')
 details+=['',f'### SF{scale} memory by query','','RSS columns are the largest post-query sample across all trials, in GiB.','PG allocation is the largest tracked query allocation peak, in MiB.','','| Query | PG RSS GiB | DuckDB RSS GiB | CH RSS GiB | PG allocation MiB |','|---|---:|---:|---:|---:|']
 for q in range(1,23):
  rss=[maximum(t.get('rss',{}).get('VmRSS') for t in data[scale][e][q]['trials']) for e in engines]
  alloc=maximum(t.get('query_peak_allocation_bytes') for t in data[scale]['pgaccel-native'][q]['trials'])
  details.append(f'| Q{q} | '+ ' | '.join(fmt(v) for v in rss)+f' | {fmt(alloc,2**20)} |')
lines=['','## Recorded memory usage','','These measurements come exclusively from the completed timing sweep;','no query was rerun to collect memory. N/A means not recorded.','','- Process RSS includes resident data, runtime state and retained allocator','  memory. Post-query samples can miss temporary peaks during execution.','- VmHWM is the process lifetime high-water mark, not an isolated query','  peak. Later queries inherit earlier peaks. The SF100 ClickHouse process','  includes server startup and preparation before timed queries.','- Service MemoryPeak includes charged file cache and all service processes;','  the reported maximum is from existing periodic samples. It is not','  interchangeable with RSS or the query allocation tracker.','- PGAccel query allocations exclude its resident dataset. Equivalent','  per-query allocation peaks were not recorded for DuckDB or ClickHouse,','  so no three-engine query-memory ranking is claimed.','- ClickHouse SF10 had no server PID sampling; its process memory is N/A.','  PGAccel loads its dataset eagerly, while DuckDB reads on demand, so','  memory immediately after opening is not an equal-work comparison.','','| Scale | Engine | RSS after open GiB | Process HWM GiB | Sampled service peak GiB |','|---|---|---:|---:|---:|']+overview+details+['']
for d in [root,mirror]:
 p=d/'FINAL-ALL1.md';s=p.read_text();s=s.split('\n## Recorded memory usage')[0];p.write_text(s+'\n'.join(lines))
 p=d/'FINAL-ALL1.json'; obj=json.loads(p.read_text());obj['recorded_memory']=memory;p.write_text(json.dumps(obj,indent=2)+'\n')
print(json.dumps({s:{e:{k:v for k,v in x.items() if k!='queries'} for e,x in entries.items()} for s,entries in memory.items()},indent=2))
