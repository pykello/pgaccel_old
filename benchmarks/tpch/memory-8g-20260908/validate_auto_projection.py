#!/usr/bin/env python3
import hashlib
import json
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit import equivalent
root=Path(__file__).resolve().parent
work=Path('/home/hadi/projects/tpch-work')
oracle_path=Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906/duckdb-min-chain-sf1.jsonl')
oracles={r['id']:r for l in oracle_path.open() if 'id' in (r:=json.loads(l))}
queries=[' '.join((work/f'queries-duckdb-1.4.3/q{n:02}.sql').read_text().split()) for n in range(1,23)]
runner=Path(sys.argv[1]).resolve()
tag="-"+sys.argv[2] if len(sys.argv)>2 else ""
command=['systemd-run','--user','--unit=pgaccel-auto-direct-sf1','--property=MemoryMax=8G','--property=MemorySwapMax=0','--wait','--pipe',str(runner),'direct-dir',str(work/'native-sf1'),'ignored','-','1','1','1']
run=subprocess.run(command,input='\n'.join(queries)+'\n',capture_output=True,text=True,timeout=300)
raw=Path('/home/hadi/projects/docs/tpch/memory-8g-20260908/auto-direct-sf1'+tag+'.jsonl');raw.parent.mkdir(parents=True,exist_ok=True);raw.write_text(run.stdout)
records=[json.loads(l) for l in run.stdout.splitlines()]
gates=[]
for r in records:
    if 'id' not in r:continue
    oracle=oracles[r['id']]
    gates.append({'id':r['id'],'status':r['status'],'correct':r['status']=='success' and equivalent(r['rows'],oracle['trials'][0]['rows'],oracle['oracle_types']),
                  'prepare_ms':r.get('prepare_ms'),'execution_ms':r.get('ms'),'error':r.get('error')})
report={'command':command,'returncode':run.returncode,'stderr':run.stderr,'runner_sha256':hashlib.sha256(runner.read_bytes()).hexdigest(),
        'oracle':str(oracle_path),'raw_file':str(raw),'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),'queries':gates}
(root/('auto-projection-sf1'+tag+'-gate.json')).write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(gates),flush=True)
if run.returncode or len(gates)!=22 or not all(g['correct'] for g in gates):raise SystemExit('Projection validation failed')
