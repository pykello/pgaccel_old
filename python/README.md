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
preparation also discards rows failing simple filters on the sole SUM-input
table (or on the only table). Columns are compacted together; numeric bounds
and local dictionaries are rebuilt. Dimensions remain intact to preserve
consecutive-key join proofs. Queries with child scopes, repeated uses of the candidate physical table,
outer joins or existence joins currently use projection alone. Other dimension
aliases can coexist with fact compaction. Native payloads are read
with Linux `O_DIRECT`; schema metadata uses ordinary small reads. Unsupported
filesystems or alignment failures raise errors without a buffered fallback.
Snapshots must remain immutable while the database is open.

Preparation may also replace a string dictionary by matching/nonmatching
representatives when every bound use, across all scopes, observes the same
literal predicate. It retains all rows and keys. Original strings remain when
an output, grouping, key or different predicate needs them. These buffers belong
only to the prepared statement; they are not a general replacement snapshot.

The database retains one projected working set. An identical SQL string reuses
its columns, while a different string releases them before preparing the next
statement. Every call reparses and executes the query; there is no result cache.
`prepare_ms` and `direct_load_bytes` report new preparation, and are zero on
reuse. `load_ms` measures constructor schema checks in this mode. Client timing
includes preparation on the first call and after SQL changes; `execution_ms`
still measures only execution.

This is not an automatic 8 GiB memory policy. Projected columns, dictionaries,
Python results and query state together may exceed the query allocation budget.
Enforce a total process/cgroup limit separately. Some SF100 queries still exceed
8 GiB even with column projection; see the memory experiment report in
`benchmarks/tpch/memory-8g-20260908/README.md`.
