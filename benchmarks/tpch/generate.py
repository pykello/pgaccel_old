#!/usr/bin/env python3
"""Generate a native DuckDB database and matching nonnullable Parquet tables."""
import argparse
import hashlib
import json
from pathlib import Path
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

TABLES = ('region', 'nation', 'supplier', 'customer', 'part', 'partsupp', 'orders', 'lineitem')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sf', type=float, default=1)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--memory-limit', default='64GiB')
    parser.add_argument('--threads', type=int, default=8)
    args = parser.parse_args()
    if args.sf <= 0:
        parser.error('--sf must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('output directory must be empty (prevents accidental overwrite)')
    if args.database.exists():
        parser.error('database must not exist (prevents duplicate generation)')
    if args.threads < 1:
        parser.error('threads must be positive')
    args.database.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.database))
    con.execute("SET threads = ?", [args.threads])
    con.execute("SET memory_limit = ?", [args.memory_limit])
    con.execute("SET temp_directory = ?", [str(args.database.parent / 'temp')])
    con.execute('INSTALL tpch; LOAD tpch;')
    started = time.monotonic()
    con.execute('CALL dbgen(sf=?)', [args.sf])
    manifest = {'scale_factor': args.sf, 'duckdb': duckdb.__version__,
                'pyarrow': pa.__version__, 'database': str(args.database.resolve()),
                'memory_limit': args.memory_limit, 'threads': args.threads, 'generation_seconds': time.monotonic()-started,
                'tables': {}}
    con.execute('CHECKPOINT')
    (args.output / 'generation.json').write_text(json.dumps(manifest, indent=2)+'\n')
    for name in TABLES:
        started = time.monotonic()
        columns = con.execute(f'DESCRIBE {name}').fetchall()
        # Precision 15 preserves values and forces the INT64 decimal physical
        # storage required by the native importer; smaller physical types reject.
        select = ', '.join(f'CAST({c} AS DECIMAL(15,2)) AS {c}'
                           if typ.startswith('DECIMAL') else c
                           for c, typ, *_ in columns)
        reader = con.execute(f'SELECT {select} FROM {name}').fetch_record_batch(131072)
        schema = pa.schema([pa.field(f.name, f.type, nullable=False) for f in reader.schema])
        path = args.output / f'{name}.parquet'
        rows = 0
        with pq.ParquetWriter(path, schema, compression='snappy',
                             store_decimal_as_integer=True) as writer:
            for batch in reader:
                if any(col.null_count for col in batch.columns):
                    raise ValueError(f'{name}: PgAccel cannot load nulls')
                table = pa.Table.from_batches([batch]).cast(schema)
                writer.write_table(table, row_group_size=131072)
                rows += len(batch)
        metadata = pq.read_metadata(path)
        for i in range(metadata.num_columns):
            col = metadata.schema.column(i)
            if col.physical_type not in ('INT32', 'INT64', 'BYTE_ARRAY'):
                raise ValueError(f'Unsupported physical type: {col}')
        digest = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
        manifest['tables'][name] = {'rows': rows, 'bytes': path.stat().st_size,
                                    'sha256': digest, 'export_seconds': time.monotonic()-started}
        print(name, rows, path.stat().st_size, flush=True)
    con.close()
    manifest['database_bytes'] = args.database.stat().st_size
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
