#!/usr/bin/env python3
"""Same native Python interface, with direct preparation and a total 8 GiB cap."""
import hashlib
import importlib.util
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parent
work = Path('/home/hadi/projects/tpch-work')
if sys.argv[1] == '--worker':
    module, query = Path(sys.argv[2]), int(sys.argv[3])
    spec = importlib.util.spec_from_file_location('pgaccel_native', module)
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    sql = (work/f'queries-duckdb-1.4.3/q{query:02}.sql').read_text()
    cgroup = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
    trials = []
    with native.Database(str(work/'sf100/native'), direct=True,
                         memory_bytes=512*2**20) as db:
        for _ in range(9):
            start = time.perf_counter()
            result = db.execute(sql)
            rows = result['rows']
            elapsed = (time.perf_counter()-start)*1000
            trials.append(dict(result, client_ms=elapsed))
        report = dict(trials=trials, cgroup_peak_bytes=int((cgroup/'memory.peak').read_text()),
                      memory_max=int((cgroup/'memory.max').read_text()),
                      swap_max=int((cgroup/'memory.swap.max').read_text()))
        print(json.dumps(report))
    raise SystemExit()

sys.path.insert(0, str(root.parent))
from audit import equivalent
from evidence import save_report
module = Path(sys.argv[1]).resolve()
queries = list(map(int, sys.argv[2:])) or [1,2,4,6,11,12,14,15,16,17,18,19,22]
oracle_path = Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906/duckdb-final-all1-sf100.jsonl')
oracles = {r['id']:r for line in oracle_path.open() if 'id' in (r:=json.loads(line))}
inventory = json.loads((root/'column-inventory.json').read_text())
for query in queries:
    command = ['systemd-run', '--user', '--unit=pgaccel-native-direct-'+str(query)+'-'+str(time.time_ns()),
               '--property=MemoryAccounting=yes', '--property=MemoryMax=8G', '--property=MemorySwapMax=0',
               '--wait', '--pipe', sys.executable, str(Path(__file__).resolve()), '--worker', str(module), str(query)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=300)
    report = dict(query=query, command=command, returncode=run.returncode, stderr=run.stderr,
                  stdout=run.stdout, module_sha256=hashlib.sha256(module.read_bytes()).hexdigest(),
                  oracle=str(oracle_path), baseline_client_ms=inventory['queries'][query-1]['baseline']['median_client_ms'],
                  note='Same native Python API; nine trials, warm median excludes first preparation. Each query runs in a fresh capped process.')
    if run.returncode == 0:
        data = json.loads(run.stdout)
        report['measurements'] = data
        report['correct'] = all(equivalent(t['rows'], oracles[query]['trials'][0]['rows'], oracles[query]['oracle_types']) for t in data['trials'])
        report['median_client_ms'] = statistics.median(t['client_ms'] for t in data['trials'][1:])
        report['first_client_ms'] = data['trials'][0]['client_ms']
        report['memory_verified'] = data['memory_max'] == 8*2**30 and data['swap_max'] == 0 and data['cgroup_peak_bytes'] <= 8*2**30
        report['reuse_verified'] = data['trials'][0]['direct_load_bytes'] > 0 and all(t['direct_load_bytes'] == 0 for t in data['trials'][1:])
    save_report(root, f'native-direct-q{query}-sf100.json', report)
    print(json.dumps({k:v for k,v in report.items() if k not in ['stdout','stderr','measurements','command']}), flush=True)
    if run.returncode or not all(report.get(k) for k in ['correct','memory_verified','reuse_verified']):
        raise SystemExit('Native direct validation failed')
