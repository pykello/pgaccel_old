#!/usr/bin/env python3
"""Sequential, resident, client-timed trials; no claim of cold-cache results."""
import argparse
import datetime
import hashlib
import http.client
import json
import os
import pathlib
import selectors
import subprocess
import time
import urllib.parse
from answer_validation import matches

p = argparse.ArgumentParser()
p.add_argument('--engine', choices=['pgaccel','clickhouse'], required=True)
p.add_argument('--runner', default='build/query_runner')
p.add_argument('--native')
p.add_argument('--oracle', required=True)
p.add_argument('--output', required=True)
p.add_argument('--queries', default=str(pathlib.Path(__file__).with_name('queries.sql')))
p.add_argument('--ids', default='all')
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--threads', type=int, choices=[1,8], default=8)
p.add_argument('--timeout', type=int, default=900)
p.add_argument('--database', default='clickbench_audit')
a = p.parse_args()
if a.repeats < 1:
    p.error('repeats must be positive')
output = pathlib.Path(a.output)
output.parent.mkdir(parents=True, exist_ok=True)
queries = pathlib.Path(a.queries).read_text().splitlines()
ids = set(range(1,44)) if a.ids == 'all' else set(map(int,a.ids.split(',')))
oracles = {r['id']:r for r in map(json.loads,pathlib.Path(a.oracle).read_text().splitlines())}
metadata = dict(engine=a.engine, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    cache_mode='resident; no cache resets', timing='request through complete response',
    threads=a.threads, repeats=a.repeats,
    queries_sha256=hashlib.sha256(pathlib.Path(a.queries).read_bytes()).hexdigest(),
    source_commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip())
process = None
selector = None
connection = None
stderr = None
try:
    if a.engine == 'pgaccel':
        if not a.native:
            p.error('--native is required for pgaccel')
        metadata['runner_sha256'] = hashlib.sha256(pathlib.Path(a.runner).read_bytes()).hexdigest()
        stderr = output.with_suffix('.stderr').open('w')
        process = subprocess.Popen([a.runner,'native',a.native,'hits','-',str(int(a.threads==8)),'1','1'],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,
            env=dict(os.environ,PGACCEL_QUERY_MEMORY_BYTES=str(32<<30)))
        selector = selectors.DefaultSelector()
        selector.register(process.stdout,selectors.EVENT_READ)
        def receive():
            if not selector.select(a.timeout):
                raise TimeoutError('Resident runner deadline exceeded')
            line = process.stdout.readline()
            if not line:
                raise RuntimeError('Resident runner exited; see stderr artifact')
            return line
        loaded = json.loads(receive())
        if loaded.get('status') != 'loaded' or loaded.get('rows') != 99997497 or loaded.get('columns') != 105:
            raise RuntimeError(f'Incomplete fixture: {loaded}')
        metadata['load'] = loaded
        def execute(sql):
            start = time.perf_counter()
            process.stdin.write(sql+'\n')
            process.stdin.flush()
            line = receive()
            elapsed = (time.perf_counter()-start)*1000
            result = json.loads(line)
            result['client_ms'] = elapsed
            result['kernel_ms'] = (result.get('ms') or [None])[0]
            return result
    else:
        connection = http.client.HTTPConnection('localhost',8123,timeout=a.timeout)
        params = urllib.parse.urlencode(dict(database=a.database,max_threads=a.threads,
            max_memory_usage=32<<30,use_query_cache=0,default_format='JSONCompact'))
        def execute(sql):
            start = time.perf_counter()
            connection.request('POST','/?'+params,body=sql.encode(),headers={'Content-Type':'text/plain; charset=utf-8'})
            response = connection.getresponse()
            body = response.read()
            elapsed = (time.perf_counter()-start)*1000
            if response.status != 200:
                return dict(status='execution_failure',error=body.decode(),client_ms=elapsed,kernel_ms=None,rows=[])
            data = json.loads(body)
            return dict(status='success',rows=data['data'],meta=data['meta'],client_ms=elapsed,
                        kernel_ms=data['statistics']['elapsed']*1000,statistics=data['statistics'])
        check = execute('SELECT count(*),version() FROM hits')
        if check['status'] != 'success' or str(check['rows'][0][0]) != '99997497':
            raise RuntimeError(f'Incomplete ClickHouse fixture: {check}')
        metadata['version'] = check['rows'][0][1]
    output.with_suffix('.meta.json').write_text(json.dumps(metadata,indent=2)+'\n')
    with output.open('w') as out:
        for qid,sql in enumerate(queries,1):
            if qid not in ids:
                continue
            runs = []
            for trial in range(a.repeats):
                result = execute(sql)
                result.pop('id',None)
                if result['status'] == 'success':
                    oracle = oracles.get(qid)
                    if not oracle or oracle['status'] != 'success':
                        result['status'] = 'unverified'
                    elif not matches(result['rows'],oracle['rows'],oracle['meta'],'order by' in sql.lower()):
                        result['status'] = 'answer_mismatch'
                runs.append(result)
                if result['status'] not in {'success','answer_mismatch'}:
                    break
            status = 'success' if all(r['status']=='success' for r in runs) else next(r['status'] for r in runs if r['status']!='success')
            record = dict(id=qid,sql=sql,threads=a.threads,status=status,runs=runs,
                rows=runs[-1].get('rows',[]),ms=[r['client_ms'] for r in runs],
                kernel_ms=[r.get('kernel_ms') for r in runs])
            for name in ['query_peak_allocation_bytes','process_peak_rss_bytes','spill_bytes_written']:
                if name in runs[-1]:record[name]=max(r.get(name,0) for r in runs)
            if 'meta' in runs[-1]:record['meta']=runs[-1]['meta']
            out.write(json.dumps(record)+'\n')
            out.flush()
finally:
    if process:
        if process.poll() is None:process.kill()
        process.wait()
    if selector:selector.close()
    if connection:connection.close()
    if stderr:stderr.close()
