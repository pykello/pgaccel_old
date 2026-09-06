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
