#!/usr/bin/env python3
"""Bounded multi-table projection sweep; keep failures and skip oversized sets."""
import hashlib
import json
import statistics
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit import equivalent
from evidence import save_report
root = Path(__file__).resolve().parent
probe = Path(sys.argv[1]).resolve()
work = Path('/home/hadi/projects/tpch-work')
oracle_path = Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906/duckdb-final-all1-sf100.jsonl')
oracles = {r['id']: r for line in oracle_path.open() if (r := json.loads(line)).get('id')}
inventory = json.loads((root/'column-inventory.json').read_text())
reports = []
for n in [2,4,11,12,14,15,16,17,18,19,21,22]:
    q = inventory['queries'][n-1]
    fields = [inventory['columns'][c]['table']+'.'+c for c in q['columns']]
    command = ['systemd-run', '--user', '--unit=pgaccel-projected-sweep-q'+str(n),
               '--property=MemoryAccounting=yes', '--property=MemoryMax=8G', '--property=MemorySwapMax=0',
               '--wait', '--pipe', '/usr/bin/time', '-v', str(probe),
               str(work/'sf100/native'), 'auto', str(work/f'queries-duckdb-1.4.3/q{n:02}.sql')] + fields
    run = subprocess.run(command, capture_output=True, text=True, timeout=300)
    report = {'query': n, 'command': command, 'returncode': run.returncode,
              'probe_sha256': hashlib.sha256(probe.read_bytes()).hexdigest(),
              'oracle': str(oracle_path), 'stderr': run.stderr, 'stdout': run.stdout,
              'baseline_client_ms': q['baseline']['median_client_ms'],
              'note': 'Explicit projected direct loads, then nine existing-executor trials; query budget 512 MiB, global 8 GiB/no swap. Load and JSON serialization excluded from execution timing. Not yet native Database integration.'}
    if run.returncode == 0:
        r = json.loads(run.stdout);report['measurements'] = r
        report['correct'] = all(equivalent(t['rows'], oracles[n]['trials'][0]['rows'], oracles[n]['oracle_types']) for t in r['trials'])
        report['median_execution_ms'] = statistics.median(t['execution_ms'] for t in r['trials'][1:])
        report['memory_verified'] = r['memory_max'] == 8*2**30 and r['swap_max'] == 0 and r['cgroup_peak_bytes'] <= 8*2**30
    save_report(root, f'projected-q{n}-sf100.json', report)
    reports.append({k:v for k,v in report.items() if k not in ['stdout','stderr','measurements']})
    (root/'projected-sweep-summary.json').write_text(json.dumps(reports,indent=2)+'\n')
    print(json.dumps(reports[-1]), flush=True)
