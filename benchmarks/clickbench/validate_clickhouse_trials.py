#!/usr/bin/env python3
"""Validate fresh CH trials, using DuckDB source checks for arbitrary tied rows.

These supplemental queries are correctness evidence, never benchmark timings.
Count-ordered group candidates must have the reference rank-count sequence,
unique group keys, and independently recomputed complete aggregate values.
"""
import argparse
import collections
import datetime
import json
import pathlib
import re
import duckdb
from answer_validation import matches, matches_count_ordered_tie

p = argparse.ArgumentParser()
p.add_argument('--results', required=True)
p.add_argument('--oracle', required=True)
p.add_argument('--parquet', required=True)
p.add_argument('--output', required=True)
a = p.parse_args()
records = list(map(json.loads, pathlib.Path(a.results).read_text().splitlines()))
oracles = {q['id']: q for q in map(json.loads, pathlib.Path(a.oracle).read_text().splitlines())}
failures, evidence = [], []
if sorted(q['id'] for q in records) != list(range(1, 44)):
    failures.append('Expected exactly 43 queries')
connection = duckdb.connect()
connection.execute("SET threads=8")
connection.execute("SET memory_limit='32GB'")
connection.read_parquet(a.parquet).create_view("source_hits")
connection.execute("CREATE VIEW hits AS SELECT * REPLACE "
    "(DATE '1970-01-01' + CAST(EventDate AS INTEGER) AS EventDate, "
    "TIMESTAMP '1970-01-01' + EventTime * INTERVAL '1 second' AS EventTime) "
    "FROM source_hits")
# These unchanged queries have non-unique COUNT ordering; other differences
# remain failures requiring investigation.
specs = {
    22: (['SearchPhrase'], 2),
    31: (['SearchEngineID', 'ClientIP'], 2),
    32: (['WatchID', 'ClientIP'], 2),
    33: (['WatchID', 'ClientIP'], 2),
    39: (['URL'], 1),
    40: (['TraficSourceID', 'SearchEngineID', 'AdvEngineID',
          "CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END", 'URL'], 5),
    41: (['URLHash', 'EventDate'], 2),
    42: (['WindowClientWidth', 'WindowClientHeight'], 2),
}
def key(row, count):
    return tuple(str(v) for v in row[:count])
for record in records:
    qid, sql = record['id'], record['sql']
    oracle = oracles[qid]
    runs = record.get('runs', [])
    if len(runs) != 3:
        failures.append(f'Q{qid}: expected three trials')
        continue
    pending = []
    for trial, run in enumerate(runs, 1):
        if run['status'] not in {'success', 'answer_mismatch'}:
            failures.append(f'Q{qid} trial {trial}: execution failure')
        elif matches(run['rows'], oracle['rows'], oracle['meta'], 'order by' in sql.lower()):
            evidence.append(dict(id=qid, trial=trial, method='saved oracle', valid=True))
        else:
            pending.append((trial, run['rows']))
    if not pending:
        continue
    if qid == 25:
        source_sql = """WITH boundary AS (
            SELECT EventTime FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 1 OFFSET 9
        ) SELECT SearchPhrase,EventTime FROM hits WHERE SearchPhrase <> ''
        AND EventTime <= (SELECT EventTime FROM boundary) ORDER BY EventTime"""
        allowed = connection.execute(source_sql).fetchall()
        groups = []
        for phrase, time in allowed:
            if not groups or groups[-1][0] != time:
                groups.append((time, []))
            groups[-1][1].append(phrase)
        for trial, rows in pending:
            valid = len(rows) == len(oracle['rows']) == 10 and all(len(row) == 1 for row in rows)
            offset = 0
            for _, phrases in groups[:-1]:
                valid &= collections.Counter(row[0] for row in rows[offset:offset + len(phrases)]) == collections.Counter(phrases)
                offset += len(phrases)
            requested = collections.Counter(row[0] for row in rows[offset:])
            available = collections.Counter(groups[-1][1])
            valid &= not (requested - available)
            evidence.append(dict(id=qid, trial=trial, method='DuckDB boundary rows', valid=valid,
                                 supplemental_sql=source_sql, boundary_rows=allowed))
            if not valid:
                failures.append(f'Q{qid} trial {trial}: invalid projected tie')
        continue
    if qid not in specs:
        failures.append(f'Q{qid}: unsupported oracle difference requires investigation')
        continue
    expressions, count_column = specs[qid]
    width = len(expressions)
    candidates = {}
    for _, rows in pending:
        for row in rows:
            candidates[key(row, width)] = row[:width]
    parameters = []
    for values in candidates.values():
        values = list(values)
        if qid == 41:
            values[1] = datetime.date.fromisoformat(str(values[1]))
        parameters.extend(values)
    placeholders = ','.join('(' + ','.join('?' for _ in expressions) + ')' for _ in candidates)
    prefix, suffix = re.split(r'\s+GROUP BY\s+', sql, maxsplit=1, flags=re.I)
    group = re.split(r'\s+ORDER BY\s+|\s+LIMIT\s+', suffix, maxsplit=1, flags=re.I)[0]
    comparison = '(' + ','.join(expressions) + ') IN (VALUES ' + placeholders + ')'
    source_sql = prefix + (' AND ' if re.search(r'\bWHERE\b', prefix, re.I) else ' WHERE ') + comparison + ' GROUP BY ' + group
    actual = connection.execute(source_sql, parameters).fetchall()
    expected_ranks = [str(row[count_column]) for row in oracle['rows']]
    for trial, rows in pending:
        valid = matches_count_ordered_tie(rows, oracle['rows'], actual, width, count_column, oracle['meta'])
        evidence.append(dict(id=qid, trial=trial, method='DuckDB candidate aggregates and reference ranks',
                             valid=valid, supplemental_sql=source_sql, parameters=parameters,
                             candidate_aggregates=actual, expected_rank_counts=expected_ranks))
        if not valid:
            failures.append(f'Q{qid} trial {trial}: invalid grouped tie')
pathlib.Path(a.output).write_text(json.dumps(dict(duckdb_version=duckdb.__version__,
    source_parquet=a.parquet, failures=failures, trials=evidence), default=str, indent=2) + '\n')
for failure in failures:
    print(failure)
raise SystemExit(bool(failures))
