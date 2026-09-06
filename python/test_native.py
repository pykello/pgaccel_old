"""Integration checks: python test_native.py MODULE RUNNER (requires PyArrow)."""
import concurrent.futures
import importlib.util
import pathlib
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

spec = importlib.util.spec_from_file_location('pgaccel_native', sys.argv[1])
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)

with tempfile.TemporaryDirectory() as directory:
    root = pathlib.Path(directory)
    parquet = root / 'fixture.parquet'
    labels = ['éclair', 'a"b\\c', 'éclair', '']
    pq.write_table(pa.table({'id': pa.array([1, 2, 3, 4], type=pa.int64()),
                            'label': labels}), parquet)
    subprocess.run([sys.argv[2], 'parquet', str(parquet), 'fixture', '-',
                    '1', '1', '1', str(root / 'fixture.pgaccel')],
                   input='', text=True, capture_output=True, check=True)
    db = native.Database(directory)
    result = db.execute('SELECT id AS key, label AS text FROM fixture ORDER BY id')
    assert result['rows'] == [[str(i + 1), label] for i, label in enumerate(labels)]
    assert result['columns'] == ['key', 'text'], result['columns']
    assert result['execution_ms'] >= 0 and db.load_ms >= 0
    assert result['peak_memory_bytes'] >= 0 and result['spill_bytes'] == 0
    try:
        db.execute('SELECT missing FROM fixture')
    except ValueError:
        pass
    else:
        raise AssertionError('Invalid query accepted')
    # Both calls execute native code without the GIL; the database serializes
    # access, and returned rows own their data independently of table lifetime.
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(db.execute, 'SELECT COUNT(*) FROM fixture')
                   for _ in range(8)]
        assert all(f.result()['rows'] == [['4']] for f in futures)
    db.close()
    db.close()
    assert result['rows'][0] == ['1', 'éclair']
    try:
        db.execute('SELECT COUNT(*) FROM fixture')
    except RuntimeError as error:
        assert 'closed' in str(error)
    else:
        raise AssertionError('Closed database accepted query')
    with native.Database(directory, parallel=False, avx=False) as db:
        assert db.execute('SELECT SUM(id) FROM fixture')['rows'] == [['10']]
print('Native Python integration checks passed')
