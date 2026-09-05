"""Typed answer checks and deterministic refinements of pinned LIMIT SQL."""
import math
import re

def matches(actual, expected, meta, ordered=False):
    if len(actual) != len(expected):
        return False
    if not ordered:
        actual = sorted(actual, key=lambda row: tuple(map(str, row)))
        expected = sorted(expected, key=lambda row: tuple(map(str, row)))
    for left, right in zip(actual, expected):
        if len(left) != len(meta) or len(right) != len(meta):
            return False
        for x, y, t in zip(left, right, meta):
            if 'Float' in t['type']:
                if not math.isclose(float(x), float(y), rel_tol=1e-12, abs_tol=1e-12):
                    return False
            elif str(x) != str(y):
                return False
    return True

def refine_limit(sql, columns):
    # Deliberately restricted by the caller to the pinned, single-line corpus.
    limit = re.search(r'\bLIMIT\s+\d+(?:\s+OFFSET\s+\d+)?\s*;?\s*$', sql, re.I)
    if not limit:
        raise ValueError('Order refinement requires a pinned LIMIT query')
    prefix = sql[:limit.start()].rstrip()
    separator = ', ' if re.search(r'\bORDER\s+BY\b', prefix, re.I) else ' ORDER BY '
    return prefix + separator + ', '.join(f'{i} ASC' for i in range(1, columns + 1)) + ' ' + sql[limit.start():]

def matches_count_ordered_tie(rows, reference, candidates, key_columns, count_column, meta):
    """Check arbitrary tied groups using independently recomputed candidates.

The reference must establish the rank-count sequence for the unchanged
COUNT-ordered query. Candidate values must come from the original source
with the complete original predicates and aggregation, before LIMIT/OFFSET.
"""
    if len(rows) != len(reference) or any(len(row) != len(meta) for row in rows):
        return False
    def key(row):
        return tuple(str(value) for value in row[:key_columns])
    if len({key(row) for row in rows}) != len(rows):
        return False
    if [str(row[count_column]) for row in rows] != [str(row[count_column]) for row in reference]:
        return False
    by_key = {key(row): row for row in candidates}
    return all(key(row) in by_key and matches([row], [by_key[key(row)]], meta, True) for row in rows)
