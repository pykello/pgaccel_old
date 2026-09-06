#!/usr/bin/env python3
"""Create a new database and load all generated Parquet tables into ClickHouse."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument('--engine', choices=['Memory', 'MergeTree'], default='MergeTree')
    parser.add_argument('--statistics', action='store_true',
                        help='build minmax, uniq, and countmin statistics for MergeTree columns')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9000)
    parser.add_argument('--client', default='clickhouse-client')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.statistics and args.engine != 'MergeTree':
        parser.error('column statistics require MergeTree')
    if not re.fullmatch('[a-z][a-z0-9_]*', args.database):
        parser.error('database must contain lowercase letters, digits, or underscores')
    command = [args.client, '--host', args.host, '--port', str(args.port)]
    if args.statistics:
        command += ['--materialize_statistics_on_insert=1',
                    '--materialize_statistics_on_insert_max_table_size=0']

    def query(sql, **kwargs):
        return subprocess.check_output(command+['--query', sql], **kwargs).decode().strip()

    manifest = json.loads((args.data/'manifest.json').read_text())
    # Plain CREATE deliberately fails for an existing database: no duplicate inserts.
    query(f'CREATE DATABASE {args.database}')
    report = {'database': args.database, 'engine': args.engine,
              'statistics': 'minmax,uniq,countmin' if args.statistics else None,
              'version': query('SELECT version()'), 'tables': {}}
    for table, info in manifest['tables'].items():
        if not re.fullmatch('[a-z][a-z0-9_]*', table):
            raise ValueError('Invalid table name in manifest')
        path = args.data/f'{table}.parquet'
        schema = pq.read_schema(path)
        columns = []
        for field in schema:
            if not re.fullmatch('[a-z][a-z0-9_]*', field.name):
                raise ValueError('Invalid column name')
            typ = field.type
            if pa.types.is_int32(typ):
                ch = 'Int32'
            elif pa.types.is_int64(typ):
                ch = 'Int64'
            elif pa.types.is_decimal(typ):
                ch = f'Decimal({typ.precision},{typ.scale})'
            elif pa.types.is_date32(typ):
                ch = 'Date'
            elif pa.types.is_string(typ):
                ch = 'String'
            else:
                raise ValueError(f'Unsupported type: {field}')
            columns.append(f'{field.name} {ch}')
        engine = 'Memory' if args.engine == 'Memory' else 'MergeTree ORDER BY tuple()'
        if args.statistics:
            engine += " SETTINGS auto_statistics_types = 'minmax,uniq,countmin'"
        ddl = f'CREATE TABLE {args.database}.{table} ({", ".join(columns)}) ENGINE = {engine}'
        query(ddl)
        start = time.monotonic()
        with path.open('rb') as source:
            query(f'INSERT INTO {args.database}.{table} FORMAT Parquet', stdin=source)
        elapsed = time.monotonic()-start
        rows = int(query(f'SELECT count() FROM {args.database}.{table}'))
        if rows != info['rows']:
            raise RuntimeError(f'{table}: {rows} != {info["rows"]}')
        report['tables'][table] = {'rows': rows, 'load_wall_seconds': elapsed, 'ddl': ddl}
        print(table, rows, round(elapsed, 3), flush=True)
    if args.report:
        args.report.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
