#!/usr/bin/env python3
"""Run one engine per phase; consume saved oracle answers without a second engine."""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import select
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from audit import equivalent


def rss(pid):
    try:
        fields = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in line)
        return {key: int(fields[key].split()[0]) * 1024 for key in ('VmRSS', 'VmHWM')}
    except (FileNotFoundError, KeyError, ProcessLookupError):
        return {}


def process_io(pid):
    try:
        return {key: int(value) for key, value in
                (line.split(':', 1) for line in Path(f'/proc/{pid}/io').read_text().splitlines())}
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return {}


def strings(rows):
    return [[str(v) if v is not None else 'NULL' for v in row] for row in rows]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--engine', choices=['duckdb', 'pgaccel', 'clickhouse'], required=True)
    p.add_argument('--queries', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--oracle', type=Path)
    p.add_argument('--database', help='DuckDB file or ClickHouse database name')
    p.add_argument('--runner', type=Path)
    p.add_argument('--native', type=Path)
    p.add_argument('--url', default='http://127.0.0.1:18123')
    p.add_argument('--server-pid-file', type=Path)
    p.add_argument('--threads', type=int, default=8)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--timeout', type=int, default=1800)
    p.add_argument('--query-memory-gib', type=int, default=64)
    p.add_argument('--duckdb-memory-gib', type=int, default=176)
    p.add_argument('--temp-directory', type=Path)
    p.add_argument('--only', type=int, nargs='*')
    args = p.parse_args()
    if min(args.threads, args.repeats, args.timeout, args.query_memory_gib, args.duckdb_memory_gib) < 1:
        p.error('thread, trial, timeout, and memory limits must be positive')
    if args.engine != 'duckdb' and not args.oracle:
        p.error('non-DuckDB phases require saved --oracle answers')
    if args.engine == 'pgaccel' and (not args.runner or not args.native):
        p.error('PGAccel requires --runner and --native')
    if args.engine != 'pgaccel' and not args.database:
        p.error('DuckDB and ClickHouse require --database')
    if args.engine == 'pgaccel' and args.threads != 8:
        p.error('this PGAccel runner has an eight-worker fast-path setting')
    if args.output.exists():
        p.error('output must not exist; use a fresh checkpoint name')
    oracle = {}
    if args.oracle:
        oracle = {r['id']: r for r in map(json.loads, args.oracle.read_text().splitlines()) if 'id' in r}
    metadata = {'status': 'loaded', 'engine': args.engine, 'settings': vars(args).copy(),
                'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                'source_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True)),
                'timing': 'client_ms includes statement execution, fetch, and answer conversion; no oracle runs in this phase',
                'validation': 'Exact integers/decimals and strings/dates; floats max(1e-6 absolute, 1e-12 relative)'}
    proc = None; con = None; engine_pid = os.getpid()
    query_bytes = args.query_memory_gib << 30

    def pg_start():
        nonlocal proc, engine_pid
        start = time.perf_counter()
        environment = dict(os.environ, PGACCEL_QUERY_MEMORY_BYTES=str(query_bytes),
                           PGACCEL_SPILL_BYTES=str(512 << 30), PGACCEL_BUILD_DICTIONARIES='0')
        proc = subprocess.Popen([str(args.runner.resolve()), 'native-dir', str(args.native), 'lineitem', '-', '1', '1', '1'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=environment)
        engine_pid = proc.pid
        if not select.select([proc.stdout], [], [], 900)[0]:
            proc.kill(); proc.wait(); raise RuntimeError('PGAccel load timed out')
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError(f'PGAccel load failed: {proc.wait()}')
        loaded = json.loads(line); loaded['client_load_ms'] = (time.perf_counter()-start)*1000
        return loaded

    ch_settings = {'database': args.database, 'max_threads': args.threads, 'join_use_nulls': 1,
                   'use_query_cache': 0, 'aggregate_functions_null_for_empty': 1, 'max_execution_time': args.timeout,
                   'max_memory_usage': query_bytes, 'wait_end_of_query': 1}

    def ch_query(sql):
        url = args.url + '/?' + urllib.parse.urlencode(ch_settings)
        request = urllib.request.Request(url, data=(sql.rstrip().removesuffix(';')+' FORMAT JSONCompact').encode())
        try:
            with urllib.request.urlopen(request, timeout=args.timeout+30) as response:
                return json.loads(response.read(), parse_float=Decimal)
        except urllib.error.HTTPError as error:
            raise RuntimeError(error.read().decode()) from error

    try:
        load_start = time.perf_counter()
        if args.engine == 'pgaccel':
            metadata['load'] = pg_start()
            metadata['runner_sha256'] = hashlib.sha256(args.runner.read_bytes()).hexdigest()
        elif args.engine == 'duckdb':
            import duckdb
            con = duckdb.connect(args.database, read_only=True)
            con.execute('SET threads = ?', [args.threads])
            con.execute('SET memory_limit = ?', [f'{args.duckdb_memory_gib}GiB'])
            con.execute("SET max_temp_directory_size='512GiB'")
            if args.temp_directory:
                args.temp_directory.mkdir(parents=True, exist_ok=True)
                con.execute('SET temp_directory = ?', [str(args.temp_directory)])
            metadata['version'] = duckdb.__version__
            metadata['tables'] = con.execute('SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY table_name').fetchall()
        else:
            metadata['version'] = ch_query('SELECT version()')['data'][0][0]
            metadata['tables'] = ch_query('SELECT name, engine, total_rows, total_bytes FROM system.tables WHERE database=currentDatabase() ORDER BY name')['data']
            metadata['settings'] = dict(metadata['settings'], server_settings=ch_settings)
            engine_pid = int(args.server_pid_file.read_text()) if args.server_pid_file else 0
        metadata['open_ms'] = (time.perf_counter()-load_start)*1000
        metadata['loaded_rss'] = rss(engine_pid)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('w') as out:
            out.write(json.dumps(metadata, default=str)+'\n'); out.flush()
            for number in args.only or range(1, 23):
                sql = (args.queries/f'q{number:02}.sql').read_text()
                digest = hashlib.sha256(sql.encode()).hexdigest()
                expected = oracle.get(number)
                if expected and expected.get('sql_sha256') != digest:
                    raise RuntimeError(f'Q{number} oracle SQL hash mismatch')
                valid_oracle = bool(expected and expected.get('correct'))
                record = {'id': number, 'sql_sha256': digest, 'status': 'success', 'trials': []}
                if valid_oracle:
                    expected_rows, types = expected['oracle_rows'], expected['oracle_types']
                else:
                    expected_rows = types = None
                for trial in range(args.repeats):
                    timer = None
                    try:
                        if args.engine == 'pgaccel' and proc.poll() is not None:
                            record.setdefault('reloads', []).append(pg_start())
                        io_before = process_io(engine_pid)
                        start = time.perf_counter()
                        if args.engine == 'pgaccel':
                            proc.stdin.write(' '.join(sql.splitlines())+'\n'); proc.stdin.flush()
                            if not select.select([proc.stdout], [], [], args.timeout)[0]:
                                proc.kill(); proc.wait(); raise TimeoutError('PGAccel query timed out')
                            line = proc.stdout.readline()
                            if not line:
                                raise RuntimeError(f'PGAccel exited: {proc.wait()}')
                            value = json.loads(line)
                            if value['status'] != 'success':
                                raise RuntimeError(value.get('error', value['status']))
                            rows = value['rows']
                            diagnostics = {'execution_ms': value['ms'][0], 'query_peak_allocation_bytes': value['query_peak_allocation_bytes'],
                                           'spill_bytes_written': value['spill_bytes_written']}
                        elif args.engine == 'duckdb':
                            timer = threading.Timer(args.timeout, con.interrupt); timer.start()
                            cursor = con.execute(sql)
                            types = [str(column[1]) for column in cursor.description]
                            rows = strings(cursor.fetchall())
                            diagnostics = {}
                        else:
                            value = ch_query(sql); rows = strings(value['data'])
                            diagnostics = {'server_ms': float(value['statistics']['elapsed'])*1000,
                                           'statistics': value['statistics']}
                        elapsed = (time.perf_counter()-start)*1000
                        io_after = process_io(engine_pid)
                        diagnostics['process_io_delta'] = {key: io_after[key]-value for key, value in io_before.items() if key in io_after}
                        if args.engine == 'duckdb' and args.temp_directory:
                            diagnostics['temp_file_bytes_after'] = sum(path.stat().st_size for path in args.temp_directory.rglob('*') if path.is_file())
                        if expected_rows is None:
                            expected_rows = rows
                        record['trials'].append(dict(diagnostics, client_ms=elapsed, rows=rows,
                                                     correct=equivalent(rows, expected_rows, types), rss=rss(engine_pid)))
                    except Exception as error:
                        record['status'] = 'timeout' if isinstance(error, TimeoutError) else 'execution_failure'
                        record['error'] = str(error); record['rss'] = rss(engine_pid)
                        break
                    finally:
                        if timer:
                            timer.cancel(); timer.join()
                record['correct'] = record['status'] == 'success' and len(record['trials']) == args.repeats and all(t['correct'] for t in record['trials'])
                if args.oracle and not valid_oracle:
                    record['correct'] = None
                    record['validation'] = 'oracle_unavailable'
                record['oracle_rows'] = expected_rows; record['oracle_types'] = types
                out.write(json.dumps(record, default=str)+'\n'); out.flush()
                print(number, record['status'], record['correct'], record.get('error', '')[:200], flush=True)
    finally:
        if proc and proc.poll() is None:
            proc.stdin.close()
            try: proc.wait(timeout=60)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        if con:
            con.close()


if __name__ == '__main__':
    main()
