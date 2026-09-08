#!/usr/bin/env python3
"""Column-byte inventory, not an executor trace or measured I/O forecast."""
import argparse
import json
import re
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--native', type=Path, required=True)
p.add_argument('--queries', type=Path, required=True)
p.add_argument('--baseline', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
columns = {}
for metadata in sorted(a.native.glob('*.pgaccel.metadata')):
    data = Path(str(metadata).removesuffix('.metadata'))
    lines = metadata.read_text().splitlines()
    if lines[0].startswith('PGACCEL_SNAPSHOT'):
        lines = lines[1:]
    count = int(lines[0])
    entries = [line.split() for line in lines[1:]]
    assert len(entries) == count
    for i, entry in enumerate(entries):
        start = int(entry[0])
        end = int(entries[i + 1][0]) if i + 1 < count else data.stat().st_size
        assert 0 <= start <= end <= data.stat().st_size
        name = entry[2]
        assert name not in columns
        columns[name] = {'table': data.stem, 'bytes': end - start,
                         'offset': start, 'stripes': int(entry[1])}
baseline = json.loads(a.baseline.read_text())['scales']['100']['engines']['pgaccel-native']['queries']
queries = []
for n in range(1, 23):
    sql = (a.queries / f'q{n:02}.sql').read_text().lower()
    # TPC-H's physical column names are globally distinct. This inventory
    # intentionally ignores selectivity and counts each named column once.
    tokens = set(re.findall(r'\b[a-z_][a-z_0-9]*\b', sql))
    selected = sorted(tokens & columns.keys())
    size = sum(columns[c]['bytes'] for c in selected)
    queries.append({'query': n, 'columns': selected, 'serialized_bytes': size,
                    'full_columns_at_4GBps_ms': size / 4_000_000,
                    'baseline': baseline[str(n)] if isinstance(baseline, dict) else baseline[n-1]})
a.output.write_text(json.dumps({'note': 'Static once-per-column inventory; not actual reads. Includes dictionaries and headers; excludes repeated scans. 4 GB/s is the user-supplied assumption.',
                                'columns': columns, 'queries': queries}, indent=2) + '\n')
for q in queries:
    print(f"Q{q['query']:02}: {q['serialized_bytes']/2**30:6.2f} GiB, {q['full_columns_at_4GBps_ms']:8.1f} ms at 4 GB/s")
