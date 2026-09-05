#!/usr/bin/env python3
"""Gate every recorded resident trial, retaining original measurement records."""
import argparse
import copy
import json
import pathlib
from gate import ALL_QUERIES, validate

p=argparse.ArgumentParser()
p.add_argument('--results',required=True)
p.add_argument('--wide-oracle',required=True)
p.add_argument('--refinements',required=True)
p.add_argument('--output',required=True)
p.add_argument('--ids',default='all',help='Explicit experiment subset; default requires all 43 queries')
a=p.parse_args()
required=ALL_QUERIES if a.ids=='all' else set(map(int,a.ids.split(',')))
records=list(map(json.loads,pathlib.Path(a.results).read_text().splitlines()))
wide=json.loads(pathlib.Path(a.wide_oracle).read_text())
refinements={r['id']:r for r in map(json.loads,pathlib.Path(a.refinements).read_text().splitlines())}
failures=[]
for record in records:
    if len(record.get('runs',[]))!=3:
        failures.append(f"Q{record['id']}: expected exactly three trials")
gates=[]
for trial in range(3):
    rows=[]
    for record in records:
        row={k:copy.deepcopy(v) for k,v in record.items() if k!='runs'}
        if len(record.get('runs',[]))<=trial:
            row['status']='missing_trial'
        else:
            row.update(copy.deepcopy(record['runs'][trial]))
        rows.append(row)
    errors=validate(rows,required,wide,refinements,expected_ids=required)
    failures.extend(f'trial {trial+1}: {error}' for error in errors)
    gates.append(dict(trial=trial+1,failures=errors,queries=rows))
pathlib.Path(a.output).write_text(json.dumps(dict(expected_queries=sorted(required),failures=failures,trials=gates),indent=2)+'\n')
for failure in failures:print(failure)
raise SystemExit(bool(failures))
