#!/usr/bin/env python3
"""Retain the inventory's small repeated original columns under a total cap."""
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parent
if len(sys.argv)>1 and sys.argv[1]=='--worker':
    runner, inventory = map(Path, sys.argv[2:4])
    data = json.loads(inventory.read_text())
    candidates = [c for c in data['columns'] if len(c['queries'])>=2 and c['bytes']<=64*2**20]
    lines = ''.join(f"{c['table']} {c['column']}\n" for c in candidates)
    run = subprocess.run([str(runner), data['snapshot']], input=lines, capture_output=True, text=True)
    group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::',1)[1].lstrip('/')
    memory = {name: (group/('memory.'+name)).read_text().strip()
              for name in ['max','peak','swap.max','events']}
    print(json.dumps(dict(returncode=run.returncode, stderr=run.stderr,
                         records=[json.loads(line) for line in run.stdout.splitlines()],
                         candidates=candidates, memory=memory)))
    raise SystemExit(run.returncode)

runner, inventory = map(lambda p: Path(p).resolve(), sys.argv[1:3])
command = ['systemd-run','--user','--unit=pgaccel-column-charge-'+str(time.time_ns()),
           '--property=MemoryMax=8G','--property=MemorySwapMax=0','--wait','--pipe',
           sys.executable,str(Path(__file__).resolve()),'--worker',str(runner),str(inventory)]
run = subprocess.run(command,capture_output=True,text=True)
if run.returncode:
    raise SystemExit(run.stderr+'\n'+run.stdout)
report = json.loads(run.stdout)
report.update(command=command, runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
              inventory_sha256=hashlib.sha256(inventory.read_bytes()).hexdigest())
assert report['returncode']==0
assert int(report['memory']['max'])==8*2**30 and int(report['memory']['swap.max'])==0
assert int(report['memory']['peak'])<=8*2**30
assert len(report['records'])==len(report['candidates'])+1
(root/'native-column-charge-sf100.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(dict(summary=report['records'][-1],memory=report['memory'])))
