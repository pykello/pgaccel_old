#!/usr/bin/env python3
"""Run the unchanged TPC-H corpus in ClickHouse and verify every trial."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
from audit import equivalent


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--queries', type=Path, required=True)
    p.add_argument('--parquet', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--database', required=True)
    p.add_argument('--url', default='http://127.0.0.1:8123')
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--only', type=int, nargs='*')
    args = p.parse_args()
    if args.repeats < 1:
        p.error('--repeats must be positive')
    import duckdb
    con = duckdb.connect(); con.execute('SET threads=8')
    for path in args.parquet.glob('*.parquet'):
        con.read_parquet(str(path)).create_view(path.stem)
    settings = {'database': args.database, 'max_threads': 8, 'join_use_nulls': 1,
                'use_query_cache': 0, 'max_execution_time': 300, 'wait_end_of_query': 1}
    url = args.url + '/?' + urllib.parse.urlencode(settings)

    def query(sql):
        request = urllib.request.Request(url, data=(sql.rstrip().removesuffix(';')+' FORMAT JSONCompact').encode())
        with urllib.request.urlopen(request, timeout=330) as response:
            return json.loads(response.read(), parse_float=Decimal)

    metadata = {'status': 'loaded', 'engine': 'ClickHouse', 'settings': settings,
                'version': query('SELECT version()')['data'][0][0], 'duckdb_version': duckdb.__version__,
                'tables': query('SELECT name,engine,total_rows FROM system.tables WHERE database=currentDatabase() ORDER BY name')['data']}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w') as out:
        out.write(json.dumps(metadata, default=str)+'\n'); out.flush()
        for number in args.only or range(1, 23):
            sql = (args.queries/f'q{number:02}.sql').read_text()
            record = {'id': number, 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest(),
                      'status': 'success', 'ms': [], 'client_ms': [], 'trials': []}
            # Oracle work stays outside the consecutive engine timing trials.
            cursor = con.execute(sql)
            types = [str(column[1]) for column in cursor.description]
            expected = [[str(v) if v is not None else 'NULL' for v in row] for row in cursor.fetchall()]
            for _ in range(args.repeats):
                try:
                    start = time.perf_counter(); result = query(sql)
                    record['client_ms'].append((time.perf_counter()-start)*1000)
                    record['ms'].append(float(result['statistics']['elapsed'])*1000)
                    rows = [[str(v) if v is not None else 'NULL' for v in row] for row in result['data']]
                    record['trials'].append({'correct': equivalent(rows, expected, types), 'rows': rows})
                except urllib.error.HTTPError as error:
                    record['status'] = 'execution_failure'; record['error'] = error.read().decode(); break
            record['correct'] = len(record['trials']) == args.repeats and all(t['correct'] for t in record['trials'])
            record['oracle_rows'] = expected
            record['oracle_types'] = types if expected is not None else None
            out.write(json.dumps(record, default=str)+'\n'); out.flush()
            print(number, record['status'], record['correct'], flush=True)


if __name__ == '__main__':
    main()
