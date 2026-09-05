#!/usr/bin/env python3
"""Validate the date workload with DuckDB and run ClickHouse hot samples."""
import argparse,decimal,json,pathlib,subprocess,os
import duckdb
p=argparse.ArgumentParser();p.add_argument('--queries',required=True);p.add_argument('--data',required=True);p.add_argument('--output',required=True);p.add_argument('--database',required=True);a=p.parse_args()
os.environ['TZ']='UTC';con=duckdb.connect();con.execute('SET threads=8');con.execute(f"CREATE VIEW lineitem AS SELECT * FROM read_parquet('{a.data}/lineitem.parquet')")
result=[]
for qid,sql in enumerate(pathlib.Path(a.queries).read_text().splitlines(),1):
 oracle=[[str(c) for c in row] for row in con.execute(sql).fetchall()]
 for threads in [1,8]:
  samples=[]
  for iteration in range(10):
   run=subprocess.run(['clickhouse-client','--database',a.database,'--max_threads',str(threads),'--format','JSONCompact','--query',sql],capture_output=True,text=True,check=True)
   r=json.loads(run.stdout);samples.append(r['statistics']['elapsed']*1000)
  actual=[[str(c) for c in row] for row in r['data']]
  def canon(rows):
   def cell(v):
    try:return str(decimal.Decimal(v).normalize())
    except decimal.InvalidOperation:return v
   return sorted(tuple(cell(c) for c in row) for row in rows)
  assert canon(actual)==canon(oracle),(qid,threads)
  result.append({'id':qid,'threads':threads,'rows':oracle,'ms':samples[3:],'database':a.database})
pathlib.Path(a.output).write_text(json.dumps(result,indent=2))
