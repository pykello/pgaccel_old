#!/usr/bin/env python3
"""Audit unmodified DuckDB TPC-H SQL against PGAccel and a Parquet oracle."""
import argparse
import decimal
import hashlib
import json
from pathlib import Path
import subprocess
import time


def equivalent(actual, expected):
    if len(actual) != len(expected):
        return False
    for left, right in zip(actual, expected):
        if len(left) != len(right):
            return False
        for a, b in zip(left, right):
            if a == b:
                continue
            try:
                x, y = decimal.Decimal(a), decimal.Decimal(b)
                if abs(x-y) <= max(decimal.Decimal('0.000001'), abs(y)*decimal.Decimal('1e-12')):
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
    p.add_argument('--only', type=int, nargs='*')
    args = p.parse_args()
    import duckdb
    con = duckdb.connect()
    con.execute('SET threads=8')
    for path in sorted(args.parquet.glob('*.parquet')):
        con.read_parquet(str(path)).create_view(path.stem)
    queries = [(i, (args.queries/f'q{i:02}.sql').read_text())
               for i in (args.only or range(1, 23))]
    proc = subprocess.Popen([str(args.runner.resolve()), 'native-dir', str(args.native),
                             'lineitem', '-', '1', '1', str(args.repeats)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w') as out:
        loaded = json.loads(proc.stdout.readline())
        loaded['source_revision'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        loaded['runner_sha256'] = hashlib.sha256(args.runner.read_bytes()).hexdigest()
        loaded['duckdb_version'] = duckdb.__version__
        out.write(json.dumps(loaded)+'\n'); out.flush()
        for i, sql in queries:
            proc.stdin.write(' '.join(sql.splitlines())+'\n'); proc.stdin.flush()
            record = json.loads(proc.stdout.readline()); record['id'] = i
            record['sql_sha256'] = hashlib.sha256(sql.encode()).hexdigest()
            if record['status'] == 'success':
                start = time.perf_counter()
                expected = [[str(v) if v is not None else 'NULL' for v in row]
                            for row in con.execute(sql).fetchall()]
                record['oracle_ms'] = (time.perf_counter()-start)*1000
                record['correct'] = equivalent(record['rows'], expected)
                record['oracle_rows'] = expected
            out.write(json.dumps(record)+'\n'); out.flush()
            print(i, record['status'], record.get('correct', ''), record.get('error', ''), flush=True)
    proc.stdin.close()
    if proc.wait() != 0:
        raise SystemExit('runner failed')


if __name__ == '__main__':
    main()
