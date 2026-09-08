#!/usr/bin/env python3
"""Run the direct-SUM diagnostic under an 8 GiB no-swap cgroup."""
import argparse
import hashlib
import json
import subprocess
from decimal import Decimal
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--probe', type=Path, required=True)
p.add_argument('--native', type=Path, required=True)
p.add_argument('--sql', type=Path, required=True)
p.add_argument('--oracle', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--repeats', type=int, default=3)
a = p.parse_args()
oracle = next(json.loads(line) for line in a.oracle.open() if json.loads(line).get('id') == 6)
expected = oracle['trials'][0]['rows'][0][0]
report = {'probe_sha256': hashlib.sha256(a.probe.read_bytes()).hexdigest(),
          'sql_sha256': hashlib.sha256(a.sql.read_bytes()).hexdigest(),
          'oracle': str(a.oracle), 'expected': expected, 'trials': [],
          'note': 'Single-reader direct SUM diagnostic. Includes loading/validation/compute, not the native Database interface. No cold/warm minima substitution.'}
for trial in range(a.repeats):
    command = ['systemd-run', '--user', '--unit=pgaccel-direct-sum-'+str(trial),
               '--property=MemoryAccounting=yes', '--property=MemoryMax=8G', '--property=MemorySwapMax=0',
               '--wait', '--pipe', '/usr/bin/time', '-v', str(a.probe.resolve()),
               str(a.native/'lineitem.pgaccel'), 'lineitem', str(a.sql),
               'l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice']
    result = subprocess.run(command, capture_output=True, text=True)
    entry = {'command': command, 'returncode': result.returncode,
             'stdout': result.stdout, 'stderr': result.stderr}
    if result.returncode == 0:
        entry['measurement'] = json.loads(result.stdout)
        entry['correct'] = Decimal(entry['measurement']['answer']) == Decimal(expected)
        m = entry['measurement']
        entry['memory_verified'] = (m['memory_max'] == 8*2**30 and m['swap_max'] == 0 and m['cgroup_peak_bytes'] <= 8*2**30)
    report['trials'].append(entry)
    a.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(entry), flush=True)
    if result.returncode or not entry.get('correct') or not entry.get('memory_verified'): raise SystemExit('Direct SUM failed')
