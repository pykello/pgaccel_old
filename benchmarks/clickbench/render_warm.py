#!/usr/bin/env python3
"""Render complete or explicitly provisional client-timed comparisons."""
import argparse
import datetime
import json
import math
import pathlib

p=argparse.ArgumentParser()
p.add_argument('--pg',required=True)
p.add_argument('--ch',required=True)
p.add_argument('--gate',required=True)
p.add_argument('--output',required=True)
p.add_argument('--summary')
p.add_argument('--allow-partial',action='store_true')
a=p.parse_args()
def records(path):
    path=pathlib.Path(path)
    return {r['id']:r for r in map(json.loads,path.read_text().splitlines())} if path.exists() else {}
pg,ch=records(a.pg),records(a.ch)
gate_path=pathlib.Path(a.gate)
gate=json.loads(gate_path.read_text()) if gate_path.exists() else None
complete=(set(pg)==set(ch)==set(range(1,44)) and gate is not None and not gate['failures'])
complete &= all(len(r.get('runs',[]))==3 and all(t['status']=='success' for t in r['runs']) for r in ch.values())
if not a.allow_partial and not complete:
    raise SystemExit('Complete trials and a passing gate are required')
statuses={r['id']:r['status'] for r in gate['trials'][-1]['queries']} if gate and not gate['failures'] else {}
def hot(record,field):
    values=record.get(field,[])
    return min(values[1:3]) if len(values)==3 and all(v is not None for v in values) else None
label=lambda value:'—' if value is None else f'{value:.3f}'
lines=['# Current pgaccel / ClickHouse runtime comparison','',
    ('Completed resident warm measurements.' if complete else f'**Measurement in progress:** pgaccel {len(pg)}/43; ClickHouse {len(ch)}/43.'),'',
    'Full 99,997,497-row ClickBench dataset on the Xeon Gold 6152 host (22 cores,',
    '44 logical CPUs). Both engines use eight workers/threads. pgaccel loads its',
    'native snapshot into memory; ClickHouse uses the existing MergeTree table.',
    'The engines run sequentially. CPU affinity and frequency are not pinned.','',
    '**Hot ms** is the faster of trials two and three. Client timing includes',
    'request delivery, parsing, execution, serialization, and the complete response.',
    'Kernel columns show separate internal timers. Loading is excluded, caches',
    'are not reset, and these measurements establish resident warm performance.',
    'They do not establish cold-start or public-leaderboard results.','',
    'Ratio is pgaccel / ClickHouse client latency; below 1 favors pgaccel.','',
    '| Query | pgaccel hot ms | CH hot ms | Ratio | PG kernel ms | CH kernel ms | Validation |',
    '| --- | ---: | ---: | ---: | ---: | ---: | --- |']
summary=[]
for q in range(1,44):
    x,y=pg.get(q,{}),ch.get(q,{})
    pt,ct=hot(x,'ms'),hot(y,'ms');pk,ck=hot(x,'kernel_ms'),hot(y,'kernel_ms')
    status=statuses.get(q,x.get('status','pending'))
    status={'success':'direct match','correct_order_refinement':'verified ties','correct_wide_average':'wide-average policy','answer_mismatch':'trial gate pending'}.get(status,status)
    ratio=pt/ct if pt is not None and ct else None
    lines.append(f'| Q{q} | {label(pt)} | {label(ct)} | '+('—' if ratio is None else f'{ratio:.2f}×')+f' | {label(pk)} | {label(ck)} | {status} |')
    summary.append(dict(id=q,pg_hot_ms=pt,ch_hot_ms=ct,ratio=ratio,pg_kernel_ms=pk,ch_kernel_ms=ck,validation=status,
        pg_peak_allocation_bytes=x.get('query_peak_allocation_bytes'),pg_process_peak_rss_bytes=x.get('process_peak_rss_bytes')))
if complete:
    wins=sum(r['pg_hot_ms']<r['ch_hot_ms'] for r in summary)
    ratio=math.exp(sum(math.log((r['pg_hot_ms']+10)/(r['ch_hot_ms']+10)) for r in summary)/43)
    lines+=['',f'pgaccel has lower hot client latency on **{wins}/43** queries. The paired',
        f'geometric mean latency ratio with a 10 ms shift is **{ratio:.2f}×** (pgaccel /',
        'ClickHouse). This is a local comparison, including the documented Q4 policy.']
    if a.summary:pathlib.Path(a.summary).write_text(json.dumps(dict(pg_faster_queries=wins,shifted_geomean_ratio=ratio,queries=summary),indent=2)+'\n')
lines+=['','All trial answers are retained. Q4 matches the independent wide sum/count',
    'oracle while differing from unchanged ClickHouse AVG(UserID). Verified ties',
    'use separate deterministic ClickHouse order refinements; raw mismatches remain',
    'visible. Process peak RSS includes the resident input and earlier queries,',
    'and is not isolated per-query memory.','']
out=pathlib.Path(a.output)
for name,path in [('pgaccel trials',a.pg),('ClickHouse trials',a.ch),('three-trial correctness gate',a.gate)]:
    import os
    lines.append(f'[{name}]({os.path.relpath(path,out.parent)})  ')
lines+=['','Updated '+datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')+'.','']
out.write_text('\n'.join(lines))
