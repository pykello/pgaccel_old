#!/usr/bin/env python3
"""Quiesce benchmark MergeTree tables and archive layout, statistics, and plans."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database', required=True)
    p.add_argument('--port', type=int, default=19000)
    p.add_argument('--queries', type=Path, required=True)
    p.add_argument('--report', type=Path, required=True)
    args = p.parse_args()
    if not re.fullmatch('[a-z][a-z0-9_]*', args.database):
        p.error('invalid database name')
    if args.report.exists():
        p.error('report must not exist')
    base = ['clickhouse-client', '--port', str(args.port), '--database', args.database]

    def query(sql):
        return subprocess.check_output(base+['--query', sql], text=True).strip()

    def rows(sql):
        return json.loads(query(sql+' FORMAT JSON'))['data']

    db = args.database
    tables = rows(f"SELECT name, engine, total_rows, total_bytes, data_paths FROM system.tables WHERE database='{db}' ORDER BY name")
    for table in tables:
        if not re.fullmatch('[a-z][a-z0-9_]*', table['name']) or table['engine'] != 'MergeTree':
            raise ValueError('Expected ordinary benchmark MergeTree tables')
        query(f"SYSTEM STOP MERGES {db}.{table['name']}")
    for attempt in range(900):
        if query(f"SELECT count() FROM system.merges WHERE database='{db}'") == '0':
            break
        time.sleep(2)
    else:
        raise RuntimeError('Timed out waiting for existing merges')
    parts = rows(f"SELECT table, count() AS active_parts, sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk FROM system.parts WHERE database='{db}' AND active GROUP BY table ORDER BY table")
    paths = rows(f"SELECT table, path FROM system.parts WHERE database='{db}' AND active")
    report = {'database': db, 'merges': 'stopped for the read-only query phase',
              'tables': tables, 'parts': parts, 'statistics_files': {}}
    for table in tables:
        roots = [Path(part['path']) for part in paths if part['table'] == table['name']]
        # Inspect only active parts after merges finish; inactive parts can be deleted.
        files = [f for root in roots for f in root.rglob('*')
                 if f.is_file() and ('statistic' in f.name or f.suffix == '.stat')]
        report['statistics_files'][table['name']] = {
            'count': len(files), 'bytes': sum(f.stat().st_size for f in files),
            'example_names': sorted({f.name for f in files})[:8]}
    args.report.write_text(json.dumps(report, indent=2)+'\n')
    flags = ['--max_threads=8', '--max_memory_usage=68719476736',
             '--join_use_nulls=1', '--aggregate_functions_null_for_empty=1', '--use_query_cache=0']
    for number in (5, 9):
        sql = (args.queries/f'q{number:02}.sql').read_text()
        plan = subprocess.check_output(base+flags+['--query', 'EXPLAIN PLAN '+sql], text=True)
        args.report.with_name(f'clickhouse-mergetree-q{number:02}-plan.txt').write_text(plan)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
