#!/usr/bin/env python3
"""Bind TPC-H SQL and inventory original native column spans, without execution."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('runner', type=Path)
parser.add_argument('snapshot', type=Path)
parser.add_argument('queries', type=Path)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
runner, snapshot = args.runner.resolve(), args.snapshot.resolve()
queries = [(args.queries / f'q{q:02}.sql').read_text() for q in range(1, 23)]
run = subprocess.run([str(runner), str(snapshot)], input='\n'.join(' '.join(q.split()) for q in queries)+'\n', capture_output=True, text=True, check=True)
records = [json.loads(line) for line in run.stdout.splitlines()]
assert [r['query'] for r in records] == list(range(1, 23))
columns, snapshots = {}, []
for meta in sorted(snapshot.glob('*.pgaccel.metadata')):
    data = meta.with_suffix('')
    lines = meta.read_text().splitlines()
    versioned = lines[0].startswith('PGACCEL_SNAPSHOT ')
    count_line = 1 if versioned else 0
    entries = [line.split() for line in lines[count_line+1:]]
    assert len(entries) == int(lines[count_line])
    stat = data.stat()
    snapshots.append(dict(table=data.stem, bytes=stat.st_size, mtime_ns=stat.st_mtime_ns,
                          identity_header=lines[0] if versioned else None, metadata_sha256=hashlib.sha256(meta.read_bytes()).hexdigest()))
    for i, entry in enumerate(entries):
        offset = int(entry[0])
        end = int(entries[i+1][0]) if i+1 < len(entries) else stat.st_size
        assert 0 <= offset <= end <= stat.st_size
        key = (data.stem, entry[2])
        assert key not in columns
        columns[key] = dict(table=key[0], column=key[1], offset=offset, bytes=end-offset,
                            groups=int(entry[1]), type_num=int(entry[3]), queries=[])
for record in records:
    for column in record['columns']:
        stored = columns[column['table'], column['column']]
        column['native_bytes'] = stored['bytes']
        stored['queries'].append(record['query'])
    record['full_column_bytes'] = sum(c['native_bytes'] for c in record['columns'])
    record['row_set'] = 'not inferred; preparation may compact rows'
report = dict(runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
              snapshot=str(snapshot), snapshots=snapshots,
              sql_sha256={q: hashlib.sha256(sql.encode()).hexdigest() for q, sql in enumerate(queries, 1)},
              queries=records, columns=list(columns.values()),
              note='Original physical column spans, not decoded RSS, actual I/O, cache eligibility or query-plan selection. Predicate flags use existing binding proofs; row pruning is not inferred.')
args.output.write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(dict(queries=len(records), required_columns=sum(bool(c['queries']) for c in columns.values()),
                     unique_required_bytes=sum(c['bytes'] for c in columns.values() if c['queries']),
                     total_native_bytes=sum(s['bytes'] for s in snapshots))))
