#!/usr/bin/env python3
"""Optional integration tests: run with Python pyarrow and the built REPL."""
import datetime,pathlib,subprocess,sys,tempfile
import pyarrow as pa
import pyarrow.parquet as pq
runner=str(pathlib.Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory() as directory:
 root=pathlib.Path(directory)
 def run(table,schema,sql='',success=True):
  path=root/'data.parquet';types=root/'types.schema';pq.write_table(table,path);types.write_text(schema)
  command=f'load_parquet t {path} * {types};\n'+sql
  p=subprocess.run([runner],input=command,text=True,capture_output=True)
  assert (p.returncode==0)==success,(p.returncode,p.stdout,p.stderr)
  return p.stdout
 text=run(pa.table({'d':pa.array([0,1],pa.uint16()),'ts':pa.array([0,86400],pa.int64())}),
          'd Date\nts TIMESTAMP\n','SELECT min(d),max(ts) FROM t;\n')
 assert '1970-01-01' in text and '1970-01-02 00:00:00' in text,text
 text=run(pa.table({'ts':pa.array([0,1000],pa.timestamp('ms',tz='UTC'))}),
          'ts TIMESTAMP\n','SELECT max(ts) FROM t;\n')
 assert '1970-01-01 00:00:01' in text,text
 run(pa.table({'ts':pa.array([1],pa.timestamp('ms',tz='UTC'))}),'ts TIMESTAMP\n',success=False)
 run(pa.table({'v':pa.array([1,None],pa.int64())}),'v BIGINT\n',success=False)
 run(pa.table({'v':pa.array([40000],pa.int32())}),'v SMALLINT\n',success=False)
 run(pa.table({'v':pa.array([1],pa.int64())}),'v Date\n',success=False)
 text=run(pa.table({'v':pa.array([],pa.int64())}),'v BIGINT\n','SELECT count(*),min(v) FROM t;\nschema t;\n')
 assert 'NULL' in text,text
 text=run(pa.table({'v':pa.array([1],pa.int32())}),'v INTEGER\n','SELECT count(*) FROM t WHERE v < 5000000000;\n')
 assert 'ERROR' not in text,text
 p=subprocess.run([runner],input='SELECT broken FROM absent;\n',text=True,capture_output=True);assert p.returncode!=0
print('PASS: declared date/timestamp units, invalid imports, empty tables, wide literals, and batch status')
