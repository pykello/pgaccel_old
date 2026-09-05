#!/usr/bin/env python3
"""Run unchanged SQL through the resident runner with per-query deadlines.

Output preserves failures and mismatches; passing syntax alone is not coverage.
The caller supplies oracle JSONL from compare.py. Float columns alone permit
relative tolerance; integer and string columns compare exactly.
"""
import argparse,json,math,os,pathlib,selectors,subprocess,time
P=argparse.ArgumentParser();P.add_argument('--runner',required=True);P.add_argument('--native',required=True);P.add_argument('--queries',required=True);P.add_argument('--oracle',required=True);P.add_argument('--output',required=True);P.add_argument('--threads',type=int,choices=[1,8],default=8);P.add_argument('--avx',type=int,choices=[0,1],default=1);P.add_argument('--timeout',type=float,default=300);P.add_argument('--memory-gib',type=int,default=32);a=P.parse_args()
oracles={r['id']:r for r in map(json.loads,pathlib.Path(a.oracle).read_text().splitlines())}
from answer_validation import matches
class Runner:
 def __init__(self):
  env=dict(os.environ,PGACCEL_QUERY_MEMORY_BYTES=str(a.memory_gib<<30))
  self.p=subprocess.Popen([a.runner,'native',a.native,'hits','-',str(a.threads==8 and 1 or 0),str(a.avx),'1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
  self.selector=selectors.DefaultSelector();self.selector.register(self.p.stdout,selectors.EVENT_READ)
  self.loaded=self.read(a.timeout)
  if self.loaded.get('status')!='loaded':raise RuntimeError(self.loaded)
 def read(self,timeout):
  if not self.selector.select(timeout):raise TimeoutError
  line=self.p.stdout.readline()
  if not line:raise RuntimeError(self.p.stderr.read())
  return json.loads(line)
 def close(self):
  if self.p.poll() is None:self.p.kill()
  self.p.wait();self.selector.close()
runner=None
try:
 with open(a.output,'w') as out:
  for qid,sql in enumerate(pathlib.Path(a.queries).read_text().splitlines(),1):
   if not sql.strip():continue
   if runner is None:runner=Runner()
   record={'id':qid,'sql':sql,'threads':a.threads,'avx':a.avx}
   try:
    runner.p.stdin.write(sql+'\n');runner.p.stdin.flush();result=runner.read(a.timeout);result.pop('id',None);record.update(result)
    if record['status']=='success':
     oracle=oracles.get(qid)
     if not oracle or oracle['status']!='success':record['status']='unverified'
     elif not matches(record['rows'],oracle['rows'],oracle['meta'],'order by' in sql.lower()):record['status']='answer_mismatch'
   except TimeoutError:
    record['status']='timeout';runner.close();runner=None
   except (RuntimeError,BrokenPipeError) as e:
    record.update(status='execution_failure',error=str(e));runner.close();runner=None
   out.write(json.dumps(record)+'\n');out.flush()
finally:
 if runner:runner.close()
