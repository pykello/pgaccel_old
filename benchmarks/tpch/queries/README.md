# TPC-H query corpus provenance

The audit reads the 22 statements exported verbatim from DuckDB 1.4.3's
`tpch_queries()` table function. Export into an external fixture directory:

```python
import duckdb
from pathlib import Path
output = Path('/home/hadi/projects/tpch-work/queries-duckdb-1.4.3')
output.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
con.execute('LOAD tpch')
for number, sql in con.execute('FROM tpch_queries()').fetchall():
    (output / f'q{number:02}.sql').write_text(sql + '\n')
```

The existing DuckDB extension supplies benchmark SQL and generated data
only; PGAccel does not link to or execute through it. TPC's dbgen materials
have a separate [TPC license](https://github.com/duckdb/duckdb/blob/v1.4.3/extension/tpch/dbgen/LICENSE),
so they are not vendored into PGAccel. New engine algorithms are original
implementations with no added dependency.

These engineering measurements are not comparable to audited TPC
Benchmark Results. This corpus is separate from the older custom
seven-query lineitem benchmark in the external docs directory.
