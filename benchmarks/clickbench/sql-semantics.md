# SQL semantics for the ClickBench subset

Input columns are non-nullable. Imports containing NULL values are rejected;
NULL literals are not accepted as a substitute for three-valued SQL logic.
An empty aggregate input yields zero for COUNT and COUNT DISTINCT and NULL
for SUM, AVG, MIN, and MAX. No approximate distinct fallback is used.

Integers and fixed-point expressions use checked 128-bit intermediate values.
The wide AVG policy is intentional, including the documented Q4 difference
from the unchanged ClickHouse query. Division by zero and arithmetic overflow
return errors. Arithmetic requires numeric inputs; SUM/AVG reject strings,
dates, and timestamps. Grouping keys compare ordered typed logical values,
including across independent dictionary code spaces. Selected grouped
expressions must appear in GROUP BY; functional dependency inference is not
part of the binder.

Strings use case-sensitive bytewise comparison without collation or Unicode
normalization. `length` counts bytes. LIKE uses UTF-8 matching through RE2:
percent matches any sequence and underscore one code point, including a
multibyte character. Backslash escapes percent, underscore, or backslash;
otherwise it remains literal. Invalid UTF-8 is outside this matching contract.
LIKE syntax follows [ClickHouse's documented matching rules](https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/like.cpp).
The SQL lexer uses doubled single quotes inside string literals and preserves
backslashes, so write a pattern such as `'a\_b'` for a literal underscore.
There is no custom ESCAPE clause. IN/NOT IN use exact, same-type non-NULL
literal lists; an empty list is rejected.

LIMIT and OFFSET are nonnegative unsigned integers; their sum must not
overflow. Grouped and ordered results use deterministic selected-value tie
breakers. SQL does not otherwise promise which tied rows cross a LIMIT/OFFSET
boundary. The audit preserves raw discrepancies and independently checks
order refinements before accepting equivalent tied results.

Timestamps are UTC seconds since the Unix epoch; dates are epoch days.
EXTRACT supports second, minute, and hour. DATE_TRUNC supports second, minute,
hour, and day and returns a timestamp. Truncation floors toward the earlier
boundary, including negative epochs. Unsupported units and non-timestamp
arguments fail at binding. No session timezone or daylight-saving conversion
is applied.

REGEXP_REPLACE accepts a string and literal RE2 pattern/replacement. It replaces
the first match (the PostgreSQL default), with dot matching newlines and
backreferences zero through nine
and escaped backslashes. The ClickBench expression is anchored to the entire
string, so first-match versus global replacement has no effect there. Invalid
patterns or replacement references are rejected. Computed string ownership and
allocations follow the query lifetime and memory budget; whole-string capture
rewrites can reuse the original resident string view.

SELECT star expands the declared schema. ORDER BY may bind hidden expressions,
but these do not appear in output. HAVING currently supports COUNT comparisons
against integer bounds, combined with AND; it runs before ORDER BY/LIMIT. Other
HAVING expressions fail explicitly.

CASE supports one WHEN/THEN pair and a required ELSE. Predicates support
comparisons and AND with conventional arithmetic precedence; evaluation is
short-circuited. Only the chosen CASE branch executes. Branches must have
compatible types, with numeric branches promoted to a common fixed scale.
NULL-producing implicit ELSE and multi-arm CASE are not supported.
