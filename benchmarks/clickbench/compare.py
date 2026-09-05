#!/usr/bin/env python3
"""Run pinned stage-3 SQL in ClickHouse; retain typed answers and timings."""
import argparse,json,pathlib,subprocess,time,os
os.environ["TZ"]="UTC"
P=argparse.ArgumentParser();P.add_argument('--queries',required=True);P.add_argument('--output',required=True);P.add_argument('--threads',type=int,default=8);P.add_argument('--repeats',type=int,default=3);P.add_argument('--database',default='clickbench_audit');a=P.parse_args()
ids={1,2,3,4,5,6,7,8,9,10,11,13,14,16,20,26,30,34,35,37,38}
queries=pathlib.Path(a.queries).read_text().splitlines()
with open(a.output,'w') as out:
 for qid,sql in enumerate(queries,1):
  if qid not in ids:continue
  samples=[];record={'id':qid,'sql':sql,'threads':a.threads}
  for run in range(a.repeats):
   start=time.monotonic()
   try:
    p=subprocess.run(['clickhouse-client','--database',a.database,'--max_threads',str(a.threads),'--max_memory_usage','34359738368','--format','JSONCompact','--query',sql],capture_output=True,text=True,timeout=300)
    if p.returncode:record.update(status='execution_failure',error=p.stderr);break
    data=json.loads(p.stdout);samples.append(data['statistics']['elapsed']*1000);record.update(status='success',rows=data['data'],meta=data['meta'],statistics=data['statistics'])
   except subprocess.TimeoutExpired:record.update(status='timeout');break
  record['ms']=samples;out.write(json.dumps(record)+'\n');out.flush()
