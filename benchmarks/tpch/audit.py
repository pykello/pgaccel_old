#!/usr/bin/env python3
"""Audit unmodified DuckDB TPC-H SQL against PGAccel and a Parquet oracle."""
import argparse
import decimal
import hashlib
import json
from pathlib import Path
import subprocess
import select
import time


def equivalent(actual, expected, types=None):
    if len(actual) != len(expected):
        return False
    for left, right in zip(actual, expected):
        if len(left) != len(right):
            return False
        for column, (a, b) in enumerate(zip(left, right)):
            if a == b:
                continue
            kind = str(types[column]).upper() if types else 'DOUBLE'
            numeric = kind.startswith(('DECIMAL', 'DOUBLE', 'FLOAT', 'REAL', 'HUGEINT', 'UHUGEINT',
                'BIGINT', 'UBIGINT', 'INTEGER', 'UINTEGER', 'SMALLINT', 'USMALLINT', 'TINYINT', 'UTINYINT'))
            if not numeric:
                return False
            try:
                x, y = decimal.Decimal(a), decimal.Decimal(b)
                if x == y:
                    continue
                if kind in ('DOUBLE', 'FLOAT', 'REAL') and abs(x-y) <= max(decimal.Decimal('0.000001'), abs(y)*decimal.Decimal('1e-12')):
                    continue
            except decimal.InvalidOperation:
                pass
            return False
    return True


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runner', type=Path, required=True)
    p.add_argument('--native', type=Path, required=True)
    p.add_argument('--parquet', type=Path, required=True)
    p.add_argument('--queries', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repeats', type=int, default=1)
    p.add_argument('--timeout', type=float, default=300)
    p.add_argument('--only', type=int, nargs='*')
    args = p.parse_args()
    if args.repeats < 1:
        p.error('--repeats must be positive')
    import duckdb
    con = duckdb.connect()
    con.execute('SET threads=8')
    for path in sorted(args.parquet.glob('*.parquet')):
        con.read_parquet(str(path)).create_view(path.stem)
    queries = [(i, (args.queries/f'q{i:02}.sql').read_text())
               for i in (args.only or range(1, 23))]
    proc = subprocess.Popen([str(args.runner.resolve()), 'native-dir', str(args.native),
                             'lineitem', '-', '1', '1', '1'],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with args.output.open('w') as out:
            loaded = json.loads(proc.stdout.readline())
            loaded['source_revision'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
            loaded['source_dirty'] = bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True))
            loaded['runner_sha256'] = hashlib.sha256(args.runner.read_bytes()).hexdigest()
            loaded['duckdb_version'] = duckdb.__version__
            loaded['native_path'] = str(args.native.resolve()); loaded['parquet_path'] = str(args.parquet.resolve())
            loaded['repeats'] = args.repeats
            loaded['validation'] = 'Exact integers and decimals; float tolerance max(1e-6, 1e-12 relative); every trial'
            out.write(json.dumps(loaded)+'\n'); out.flush()
            for i, sql in queries:
                trials = []
                for repeat in range(args.repeats):
                    start = time.perf_counter()
                    proc.stdin.write(' '.join(sql.splitlines())+'\n'); proc.stdin.flush()
                    if not select.select([proc.stdout], [], [], args.timeout)[0]:
                        proc.kill(); proc.wait()
                        out.write(json.dumps({'id': i, 'status': 'timeout', 'seconds': args.timeout})+'\n')
                        raise SystemExit(f'Q{i} timed out; remaining queries were not audited')
                    line = proc.stdout.readline()
                    if not line:
                        raise SystemExit(f'runner exited during Q{i}: {proc.wait()}')
                    trial = json.loads(line); trial['id'] = i; trial['trial'] = repeat + 1
                    trial['client_ms'] = (time.perf_counter()-start)*1000
                    trials.append(trial)
                    if trial['status'] != 'success':
                        break
                record = dict(trials[-1]); record.pop('trial', None)
                record['sql_sha256'] = hashlib.sha256(sql.encode()).hexdigest()
                record['ms'] = [t['ms'][0] for t in trials if t.get('ms')]
                record['client_ms'] = [t['client_ms'] for t in trials]
                if record['status'] == 'success':
                    start = time.perf_counter()
                    cursor = con.execute(sql)
                    types = [str(column[1]) for column in cursor.description]
                    expected = [[str(v) if v is not None else 'NULL' for v in row] for row in cursor.fetchall()]
                    record['oracle_ms'] = (time.perf_counter()-start)*1000
                    for trial in trials:
                        trial['correct'] = equivalent(trial['rows'], expected, types)
                    record['correct'] = len(trials) == args.repeats and all(t['correct'] for t in trials)
                    record['oracle_rows'] = expected; record['oracle_types'] = types
                record['trials'] = trials
                out.write(json.dumps(record)+'\n'); out.flush()
                print(i, record['status'], record.get('correct', ''), record.get('error', ''), flush=True)
        proc.stdin.close()
        if proc.wait() != 0:
            raise SystemExit('runner failed')
    finally:
        if proc.poll() is None:
            proc.terminate(); proc.wait()


if __name__ == '__main__':
    main()
