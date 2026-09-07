import pathlib,subprocess
repo=pathlib.Path('/home/hadi/projects/pgaccel_old')
work=pathlib.Path('/home/hadi/projects/tpch-work')
out=pathlib.Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906')
common=[str(work/'venv/bin/python'),'benchmarks/tpch/compare_scale.py','--queries',str(work/'queries-duckdb-1.4.3'),'--repeats','15']
for engine in ['duckdb','clickhouse','pgaccel-native']:
 args=['--engine',engine,'--output',str(out/(engine+'-final-all1-sf10.jsonl'))]
 if engine=='duckdb':args+=['--database',str(work/'sf10/tpch.duckdb')]
 else:args+=['--oracle',str(out/'duckdb-final-all1-sf10.jsonl')]
 if engine=='clickhouse':args+=['--database','tpch_sf10','--url','http://127.0.0.1:8123','--clickhouse-query-overrides',str(work/'queries-clickhouse-double-1.4.3')]
 if engine=='pgaccel-native':args+=['--module',str(work/'checkpoints/final-all-v1/pgaccel_native.cpython-312-x86_64-linux-gnu.so'),'--native',str(work/'native-sf10')]
 with (out/(engine+'-final-all1-sf10.log')).open('w') as log:subprocess.run(common+args,cwd=repo,stdout=log,stderr=subprocess.STDOUT,check=True)
