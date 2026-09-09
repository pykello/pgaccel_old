# Optional native Python interface

The read-only interface loads a directory of `.pgaccel` tables. It uses the
existing parser and executor, with no optional index construction or result
cache. Table names are lowercase file stems. Result cells are strings, matching
the CLI's exact decimal and date formatting; SQL NULL is represented as `NULL`.
This interface does not yet expose typed or streaming results or cancellation.

Install the normal project build dependencies, Python development headers, and
[pybind11 3.0.1](https://github.com/pybind/pybind11/tree/v3.0.1). Its
[BSD license](https://github.com/pybind/pybind11/blob/v3.0.1/LICENSE) permits
commercial use and redistribution subject to retaining its notices.

```sh
sudo apt-get install python3-dev
python -m pip install pybind11==3.0.1
cmake -S . -B build-python -DPGACCEL_BUILD_PYTHON=ON \
  -DPython_EXECUTABLE="$(command -v python)" \
  -Dpybind11_DIR="$(python -m pybind11 --cmakedir)"
cmake --build build-python --target pgaccel_native -j8
PYTHONPATH=build-python python
```

```python
import pgaccel_native

with pgaccel_native.Database('/path/to/native-tables') as db:
    result = db.execute('SELECT COUNT(*) FROM lineitem')
    print(result['columns'], result['rows'])
```

`memory_bytes` (default 64 GiB) and `spill_bytes` (default 512 GiB) apply to each
query and its children. They exclude resident tables and Python result objects.
`parallel` and `avx` default to true. Parallel execution uses the existing
executor's eight-worker setting. Calls on one database are serialized; loading,
execution, and explicit `close()` release the GIL. Use the context manager to
close large datasets promptly. Parsing errors raise `ValueError`; execution and
closed-database errors raise `RuntimeError`.

`execution_ms` measures only `ExecuteQuery`. Time the entire `execute()` call to
include parsing, conversion to Python lists/strings, and native result cleanup.
`peak_memory_bytes` and `spill_bytes` are the executor's diagnostic counters.
String reuse during conversion is bounded to 4,096 distinct values per call;
there is no reuse across queries.

For integration checks, install PyArrow and run:

```sh
python python/test_native.py build-python/pgaccel_native*.so build/query_runner
```

The CMake setup follows the official
[pybind11 build documentation](https://pybind11.readthedocs.io/en/stable/compiling.html).
The default build keeps this optional dependency disabled.

## Experimental direct loading

Pass `direct=True` to load only the physical columns referenced by each SQL
statement. Binding includes joins and nested queries. For a simple query scope,
preparation also discards rows failing simple filters on the largest SUM-input
table (or on the only table), estimated using snapshot stripe counts. Columns are compacted together; numeric bounds
and local dictionaries are rebuilt. Dimensions remain intact to preserve
consecutive-key join proofs. Join-membership pruning declines child scopes,
repeated uses of the candidate physical table, outer joins and existence joins. Other dimension
aliases can coexist with fact compaction. Inner-join preparation loads dimensions
first and can intersect the fact input with filtered dimension-key membership,
including transitive integer equalities. The original join still expands duplicate
matches. Bitmap storage is bounded; sparse extreme domains skip this optional
pruning. A literal condition shared by every OR branch can also be applied
during preparation; conditions present in only some branches cannot. The full
original predicate remains in execution. Membership construction is included
in preparation time. Native payloads are read
with Linux `O_DIRECT`; schema metadata uses ordinary small reads. Unsupported
filesystems or alignment failures raise errors without a buffered fallback.
Each direct reader owns an 8 MiB aligned buffer; a projected stripe scan uses
one per selected column plus its header reader. These buffers count toward the
total process limit in addition to retained columns and executor memory.
Snapshots must remain immutable while the database is open.

For two-table conditional counts whose SUM inputs are constant CASE values,
preparation can filter the single string/date grouping input. This preserves
the other table's key sequence and retains the same scope restrictions.

Single-table scopes can also discard rows inside subqueries or CTEs when
every bound use of that physical table requires the same literal filters.
An unfiltered use, a different filter, or a use in a multi-table scope prevents
this shared row removal. This preserves rows needed by another alias or scope.

Preparation may also replace a string dictionary by matching/nonmatching
representatives when every bound use, across all scopes, observes the same
literal predicate. It retains all rows and keys. Original strings remain when
an output, grouping, key or different predicate needs them. These buffers belong
only to the prepared statement; they are not a general replacement snapshot.
Date pairs used exclusively in one same-row comparison can likewise retain
representatives of true/false outcomes. Actual date values, other constraints,
and cross-alias comparisons prevent that transformation.

The database retains one projected working set. An identical SQL string reuses
its columns, while a different string releases them before preparing the next
statement. Every call reparses and executes the query; there is no result cache.
On glibc, changing SQL also returns unused allocator pages after releasing
the old tables. That process-wide allocator operation is included in preparation
time and may increase later allocation cost. Identical SQL does not trigger it.
`prepare_ms` and `direct_load_bytes` report new preparation, and are zero on
reuse. `load_ms` measures constructor schema checks in this mode. Client timing
includes preparation on the first call and after SQL changes; `execution_ms`
still measures only execution.

This is not an automatic 8 GiB memory policy. Projected columns, dictionaries,
Python results and query state together may exceed the query allocation budget.
Enforce a total process/cgroup limit separately. Some SF100 queries still exceed
8 GiB even with column projection; see the memory experiment report in
`benchmarks/tpch/memory-8g-20260908/README.md`.
