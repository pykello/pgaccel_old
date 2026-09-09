#!/usr/bin/env python3
"""Measure changing SQL in one Database under a total cgroup limit."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

root = Path(__file__).resolve().parent
work = Path('/home/hadi/projects/tpch-work')

if len(sys.argv) > 1 and sys.argv[1] == '--worker':
    module = Path(sys.argv[2])
    cache_mib = int(sys.argv[3])
    queries = list(map(int, sys.argv[4:]))
    spec = importlib.util.spec_from_file_location('pgaccel_native', module)
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
    def memory():
        result = {key: int((group / ('memory.' + key)).read_text())
                  for key in ['current', 'peak', 'max', 'swap.max']}
        stats = dict(line.split() for line in (group / 'memory.stat').read_text().splitlines())
        result.update({key: int(stats[key]) for key in ['anon', 'file', 'kernel']})
        result['events'] = {key: int(value) for key, value in
                            (line.split() for line in (group / 'memory.events').read_text().splitlines())}
        return result
    cache_options = dict(cache_bytes=cache_mib * 2**20) if cache_mib else {}
    with native.Database(str(work / 'sf100/native'), direct=True,
                         memory_bytes=768 * 2**20, **cache_options) as db:
        for step, query in enumerate(queries):
            sql = (work / f'queries-duckdb-1.4.3/q{query:02}.sql').read_text()
            # Flush progress before preparation so an OOM identifies the step.
            print(json.dumps(dict(event='start', step=step, query=query,
                                  memory=memory())), flush=True)
            for trial in range(9):
                before = memory()
                start = time.perf_counter()
                result = db.execute(sql)
                elapsed = (time.perf_counter() - start) * 1000
                print(json.dumps(dict(event='trial', step=step, query=query,
                                      trial=trial, before=before, after=memory(),
                                      client_ms=elapsed, **result)), flush=True)
                del result
    raise SystemExit()

sys.path.insert(0, str(root.parent))
from audit import equivalent
from evidence import save_report

parser = argparse.ArgumentParser()
parser.add_argument('module', type=Path)
parser.add_argument('queries', nargs='+', type=int)
parser.add_argument('--tag', required=True)
parser.add_argument('--cache-mib', type=int, default=0)
args = parser.parse_args()
if args.cache_mib < 0: parser.error("cache size must be nonnegative")
module = args.module.resolve()
oracle_path = Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906/duckdb-final-all1-sf100.jsonl')
oracles = {r['id']: r for line in oracle_path.open() if 'id' in (r := json.loads(line))}
command = ['systemd-run', '--user', '--unit=pgaccel-transitions-' + str(time.time_ns()),
           '--property=MemoryAccounting=yes', '--property=MemoryMax=8G',
           '--property=MemorySwapMax=0', '--wait', '--pipe', sys.executable,
           str(Path(__file__).resolve()), '--worker', str(module), str(args.cache_mib), *map(str, args.queries)]
run = subprocess.run(command, capture_output=True, text=True)
events = [json.loads(line) for line in run.stdout.splitlines() if line.strip()]
trials = [event for event in events if event['event'] == 'trial']
report = dict(cache_mib=args.cache_mib, command=command, returncode=run.returncode, stderr=run.stderr,
              sequence=args.queries, module_sha256=hashlib.sha256(module.read_bytes()).hexdigest(),
              oracle=str(oracle_path), oracle_sha256=hashlib.sha256(oracle_path.read_bytes()).hexdigest(),
              sql_sha256={q: hashlib.sha256((work / f'queries-duckdb-1.4.3/q{q:02}.sql').read_bytes()).hexdigest()
                          for q in args.queries},
              starts=[e for e in events if e['event'] == 'start'],
              measurements=dict(trials=trials),
              complete=run.returncode == 0 and len(trials) == 9 * len(args.queries),
              note='One long-lived Database, nine calls per step; first call includes re-preparation. Live memory peaks are cumulative across the sequence.')
report['correct'] = all(equivalent(t['rows'], oracles[t['query']]['trials'][0]['rows'],
                                   oracles[t['query']]['oracle_types']) for t in trials)
report['memory_verified'] = bool(trials) and all(
    m['max'] == 8 * 2**30 and m['swap.max'] == 0 and m['peak'] <= 8 * 2**30
    for t in trials for m in [t['before'], t['after']])
report['reuse_verified'] = all(
    (t['direct_load_bytes'] > 0) == (t['trial'] == 0 and
        (t['step'] == 0 or args.queries[t['step']] != args.queries[t['step'] - 1]))
    for t in trials)
report['steps'] = []
for step, query in enumerate(args.queries):
    ts = [t for t in trials if t['step'] == step]
    report['steps'].append(dict(step=step, query=query, trials=len(ts),
        first_client_ms=ts[0]['client_ms'] if ts else None,
        warm_client_ms=statistics.median(t['client_ms'] for t in ts[1:]) if len(ts)>1 else None))
save_report(root, f'native-transitions-{args.tag}-sf100.json', report)
print(json.dumps({k: v for k, v in report.items() if k not in ['measurements', 'command', 'stderr']}, indent=2))
if not all(report[k] for k in ['complete', 'correct', 'memory_verified', 'reuse_verified']):
    raise SystemExit('Transition validation failed; partial evidence retained')
