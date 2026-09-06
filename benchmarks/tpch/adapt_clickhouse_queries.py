#!/usr/bin/env python3
"""Adapt decimal ratios in the external DuckDB 1.4.3 TPC-H SQL corpus."""
import argparse
import hashlib
import json
from pathlib import Path


def replace_once(sql, source, replacement):
    if sql.count(source) != 1:
        raise ValueError(f'Expected exactly one dialect anchor: {source!r}')
    return sql.replace(source, replacement, 1)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        p.error('output must not exist; keep earlier adaptations reproducible')
    replacements = {
        8: [('sum(\n        CASE', 'CAST(sum(\n        CASE'),
            ('END) / sum(volume)',
             'END) AS DOUBLE) / CAST(sum(volume) AS DOUBLE)')],
        14: [('100.00 * sum(', 'CAST(100.00 * sum('),
             ('END) / sum(l_extendedprice * (1 - l_discount))',
              'END) AS DOUBLE) / CAST(sum(l_extendedprice * (1 - l_discount)) AS DOUBLE)')],
        17: [('sum(l_extendedprice) / 7.0',
              'CAST(sum(l_extendedprice) AS DOUBLE) / CAST(7.0 AS DOUBLE)')],
    }
    prepared = {}
    manifest = {'source': str(args.source.resolve()),
                'reason': 'Explicit DOUBLE division matches the DuckDB ratio result type; no tolerance changes',
                'queries': {}}
    for number, edits in replacements.items():
        name = f'q{number:02}.sql'
        source = sql = (args.source/name).read_text()
        for old, new in edits:
            sql = replace_once(sql, old, new)
        prepared[name] = sql
        manifest['queries'][str(number)] = {
            'source_sha256': hashlib.sha256(source.encode()).hexdigest(),
            'executed_sha256': hashlib.sha256(sql.encode()).hexdigest()}
    # Validate every source anchor before publishing any adapted query.
    args.output.mkdir(parents=True)
    for name, sql in prepared.items():
        (args.output/name).write_text(sql)
    (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')


if __name__ == '__main__':
    main()
