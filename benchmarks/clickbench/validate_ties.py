#!/usr/bin/env python3
"""Preserve independent ClickHouse evidence for tied LIMIT answers.

The unchanged audit remains untouched. Refine its ORDER BY with all selected
columns, preserving existing order, LIMIT, and OFFSET. A matching result is
a permitted result of the original query, including unordered LIMIT queries.
This is supplemental correctness evidence, never an unchanged-query timing.
"""
import argparse
import json
import os
import pathlib
import subprocess
from answer_validation import refine_limit

p = argparse.ArgumentParser()
p.add_argument('--audit', required=True)
p.add_argument('--oracle', required=True)
p.add_argument('--output', required=True)
p.add_argument('--database', default='clickbench_audit')
a = p.parse_args()
queries = (pathlib.Path(__file__).parent / 'queries.sql').read_text().splitlines()
oracles = {r['id']: r for r in map(json.loads, pathlib.Path(a.oracle).read_text().splitlines())}
with open(a.output, 'w') as out:
    for record in map(json.loads, pathlib.Path(a.audit).read_text().splitlines()):
        if record['status'] != 'answer_mismatch' or record['id'] == 4:
            continue
        assert record['sql'] == queries[record['id']-1]
        oracle = oracles[record['id']]
        sql = refine_limit(record['sql'], len(oracle['meta']))
        result = subprocess.run(['clickhouse-client', '--database', a.database,
            '--max_threads', '8', '--enable_positional_arguments', '1',
            '--max_memory_usage', '34359738368', '--format', 'JSONCompact',
            '--query', sql], env=dict(os.environ,TZ='UTC'), capture_output=True,
            text=True, timeout=900, check=True)
        data = json.loads(result.stdout)
        out.write(json.dumps(dict(id=record['id'], original_sql=record['sql'],
            sql=sql, status='success', rows=data['data'], meta=data['meta']))+'\n')
        out.flush()
