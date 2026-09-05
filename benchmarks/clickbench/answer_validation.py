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
