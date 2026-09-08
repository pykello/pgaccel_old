#!/usr/bin/env python3
"""Compare projected resident direct loads with the saved SF100 oracle."""
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
for n in [1, 6]:
    q = inventory['queries'][n-1]
    command = ['systemd-run', '--user', '--unit=pgaccel-resident-direct-q'+str(n),
               '--property=MemoryAccounting=yes', '--property=MemoryMax=8G', '--property=MemorySwapMax=0',
               '--wait', '--pipe', '/usr/bin/time', '-v', str(probe),
               str(work/'sf100/native/lineitem.pgaccel'), 'lineitem',
               str(work/f'queries-duckdb-1.4.3/q{n:02}.sql')] + q['columns']
    run = subprocess.run(command, capture_output=True, text=True)
    report = {'query': n, 'command': command, 'returncode': run.returncode,
              'probe_sha256': hashlib.sha256(probe.read_bytes()).hexdigest(),
              'oracle': str(oracle_path), 'stderr': run.stderr, 'stdout': run.stdout,
              'baseline_client_ms': q['baseline']['median_client_ms'],
              'note': 'Projected direct load followed by nine existing-executor trials; load separate, query time excludes JSON serialization. This is not yet the general native Database interface.'}
    if run.returncode == 0:
        r = json.loads(run.stdout);report['measurements'] = r
        expected = oracles[n]['trials'][0]['rows']
        report['correct'] = all(equivalent(t['rows'], expected, oracles[n]['oracle_types']) for t in r['trials'])
        report['median_execution_ms'] = statistics.median(t['execution_ms'] for t in r['trials'][1:])
        report['memory_verified'] = r['memory_max'] == 8*2**30 and r['swap_max'] == 0 and r['cgroup_peak_bytes'] <= 8*2**30
    save_report(root, f'resident-direct-q{n}-sf100.json', report)
    print(json.dumps({k:v for k,v in report.items() if k not in ['stdout','stderr','measurements']}), flush=True)
    if run.returncode or not report.get('correct') or not report.get('memory_verified'):raise SystemExit('Comparison failed')
