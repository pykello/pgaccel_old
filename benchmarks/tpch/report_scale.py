#!/usr/bin/env python3
"""Render a complete three-engine comparison without dropping failed queries."""
import argparse
import hashlib
import json
from pathlib import Path


def read(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    if len({r['id'] for r in records[1:]}) != len(records)-1:
        raise ValueError(f'Duplicate query ids: {path}')
    return records[0], {r['id']: r for r in records[1:]}


def warm(record):
    if not record or record.get('correct') is not True or len(record.get('trials', [])) != 3:
        return None
    return min(t['client_ms'] for t in record['trials'][1:]) / 1000


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--duckdb', type=Path, required=True)
    p.add_argument('--pgaccel', type=Path, required=True)
    p.add_argument('--clickhouse', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--title', default='TPC-H SF100 comparison')
    args = p.parse_args()
    names = ['duckdb', 'pgaccel', 'clickhouse']
    phases = {name: read(getattr(args, name)) for name in names}
    for i in range(1, 23):
        hashes = {phase[1][i]['sql_sha256'] for phase in phases.values() if i in phase[1]}
        if len(hashes) > 1:
            raise ValueError(f'Q{i}: SQL differs between engines')
    common = [i for i in range(1, 23) if all(warm(phases[name][1].get(i)) is not None for name in names)]
    lines = [f'# {args.title}', '',
             'Times below are **client-observed seconds**, using the smaller of trials 2–3.',
             'Each query has three consecutive trials. A failed or unvalidated query is',
             'shown explicitly and is not silently removed from a full-workload total.', '',
             '| Query | DuckDB (s) | PGAccel (s) | ClickHouse (s) | Fastest |',
             '|---|---:|---:|---:|---|']
    wins = dict.fromkeys(names, 0)
    for i in range(1, 23):
        values = {name: warm(phases[name][1].get(i)) for name in names}
        fastest = min(names, key=lambda name: values[name]) if i in common else None
        if fastest:
            wins[fastest] += 1
        cells = []
        for name in names:
            record = phases[name][1].get(i)
            if values[name] is not None:
                cells.append(f'{values[name]:.3f}')
            elif not record:
                cells.append('not run')
            elif record.get('status') != 'success':
                cells.append(record.get('status', 'failed'))
            else:
                cells.append('unvalidated' if record.get('correct') is None else 'mismatch/incomplete')
        lines.append(f'| Q{i} | '+ ' | '.join(cells) + f' | {fastest or "—"} |')
    lines += ['', '| Engine | Validated/consistent queries | Sum of all 22 warm minima (s) | Fastest on common queries | Peak process RSS (GiB) |',
              '|---|---:|---:|---:|---:|']
    summary = {'title': args.title, 'common_validated_queries': common, 'engines': {}}
    for name in names:
        metadata, records = phases[name]
        passed = sum(warm(records.get(i)) is not None for i in range(1, 23))
        total = sum(warm(records[i]) for i in range(1, 23)) if passed == 22 else None
        hwm = max([metadata.get('loaded_rss', {}).get('VmHWM', 0)] +
                  [t.get('rss', {}).get('VmHWM', 0) for r in records.values() for t in r.get('trials', [])] +
                  [r.get('rss', {}).get('VmHWM', 0) for r in records.values()])
        total_text = f'{total:.3f}' if total is not None else 'incomplete'
        lines.append(f'| {name} | {passed}/22 | {total_text} | {wins[name]}/{len(common)} | {hwm/2**30:.2f} |')
        path = getattr(args, name)
        summary['engines'][name] = {'raw_path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'metadata': metadata, 'passed': passed, 'sum_warm_min_s': total, 'wins_on_common': wins[name], 'peak_rss_bytes': hwm,
            'queries': {str(i): {'status': records.get(i, {}).get('status', 'not_run'),
                                'correct': records.get(i, {}).get('correct'), 'warm_min_s': warm(records.get(i)),
                                'client_ms': [t['client_ms'] for t in records.get(i, {}).get('trials', [])],
                                'server_settings': records.get(i, {}).get('server_settings'),
                                'error': records.get(i, {}).get('error')} for i in range(1, 23)}}
    lines += ['', 'DuckDB supplies the saved typed oracle; its validation column includes',
              'consistency across its trials. PGAccel and ClickHouse are checked against',
              'those answers. Fastest-query counts use only queries validated in all three',
              'phases. A sum of per-query warm minima is a workload summary, not the wall',
              'time of one continuous benchmark execution.', '',
              'RSS is the process high-water mark including loaded data and all preceding',
              'queries. It differs from budgeted query allocations and from a cgroup limit',
              'that also includes charged file cache. See PLAN.md for memory limits, storage',
              'representations, SQL semantics, timing boundaries, and remaining limitations.', '',
              '## Raw evidence', '']
    for name in names:
        path = getattr(args, name).resolve()
        lines.append(f'- [{name} trials and answers]({path})')
    lines += ['', 'See CHECKPOINT.json for compact results, source versions, settings, and raw-file hashes.',
              'These engineering measurements are not audited TPC Benchmark Results.', '']
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text('\n'.join(lines))
    args.output.with_name('CHECKPOINT.json').write_text(json.dumps(summary, indent=2)+'\n')


if __name__ == '__main__':
    main()
