#!/usr/bin/env python3
"""Fail a fixture audit on missing queries, regressions, or wrong answers."""
import argparse
import decimal
import json
import pathlib
from answer_validation import matches, refine_limit

STAGE3 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 16, 20, 26, 30, 34, 35, 37, 38}

def validate(records, required, wide_oracle=None, refinements=None):
    by_id = {r['id']: r for r in records}
    failures = []
    if set(by_id) != set(range(1, 44)) or len(records) != 43:
        failures.append('Expected exactly one result for each of 43 queries')
    for qid in required:
        r = by_id.get(qid, {})
        if qid == 4 and wide_oracle and r.get('status') == 'answer_mismatch':
            try:
                expected = decimal.Decimal(wide_oracle['sum_userid']) / wide_oracle['count']
                actual = decimal.Decimal(r['rows'][0][0])
                valid = len(r['rows']) == 1 and len(r['rows'][0]) == 1
                valid &= abs(actual - expected) <= abs(expected) * decimal.Decimal('1e-15')
                if valid:
                    r['status'] = 'correct_wide_average'
                    continue
            except (KeyError, IndexError, decimal.InvalidOperation, ZeroDivisionError):
                pass
        evidence = (refinements or {}).get(qid)
        if r.get('status') == 'answer_mismatch' and evidence:
            try:
                valid = evidence['original_sql'] == r['sql']
                valid &= evidence['sql'] == refine_limit(r['sql'], len(evidence['meta']))
                valid &= evidence['status'] == 'success'
                valid &= matches(r['rows'], evidence['rows'], evidence['meta'], True)
                if valid:
                    r['status'] = 'correct_order_refinement'
                    continue
            except (KeyError, ValueError, TypeError):
                pass
        if r.get('status') not in {'success', 'correct_wide_average'}:
            failures.append(f"Q{qid}: {r.get('status', 'missing')}")
    for qid, r in by_id.items():
        if qid not in required and r['status'] != 'parse_failure':
            failures.append(f"Q{qid}: new outcome {r['status']} requires an explicit coverage update")
    return failures

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--results', required=True)
    p.add_argument('--required', default=','.join(map(str, sorted(STAGE3))))
    p.add_argument('--wide-oracle')
    p.add_argument('--refinements')
    p.add_argument('--output', required=True)
    args = p.parse_args()
    records = [json.loads(s) for s in pathlib.Path(args.results).read_text().splitlines()]
    oracle = json.loads(pathlib.Path(args.wide_oracle).read_text()) if args.wide_oracle else None
    refinements = {r['id']: r for r in map(json.loads, pathlib.Path(args.refinements).read_text().splitlines())} if args.refinements else {}
    failures = validate(records, {int(q) for q in args.required.split(',')}, oracle, refinements)
    pathlib.Path(args.output).write_text(json.dumps({'failures': failures, 'queries': records}, indent=2))
    for error in failures:
        print(error)
    raise SystemExit(bool(failures))
