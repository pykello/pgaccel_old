from pathlib import Path
import subprocess,json,statistics
out=Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906')
twofold={7,8,9,10,21,22}
module_hash=None
for scale,repeats in [(10,15),(100,9)]:
 subprocess.run(['python3',f'/tmp/run-final-all1-sf{scale}.py'],check=True,timeout=3600)
 times={}
 for engine in ['duckdb','clickhouse','pgaccel-native']:
  data=[json.loads(l) for l in (out/f'{engine}-final-all1-sf{scale}.jsonl').read_text().splitlines()]
  records=data[1:];assert len(records)==22 and {q['id'] for q in records}==set(range(1,23))
  if engine=='pgaccel-native':
   current=data[0]['module_sha256']
   if module_hash is None:module_hash=current
   assert current==module_hash, 'Frozen module changed between scales'
  times[engine]={}
  for q in records:
   assert q['correct'] and len(q['trials'])==repeats and all(t['correct'] for t in q['trials']), (scale,engine,q['id'])
   times[engine][q['id']]=statistics.median(t['client_ms'] for t in q['trials'][1:])
 failures=[]
 for query in range(1,23):
  target=2 if query in twofold else 4
  pg=times['pgaccel-native'][query]
  ratio=min(times['duckdb'][query],times['clickhouse'][query])/pg
  print(scale,query,pg,ratio,target,flush=True)
  if ratio<target:failures.append({'query':query,'ratio':ratio,'target':target})
 (out/f'final-all1-sf{scale}-gate.json').write_text(json.dumps({'times_ms':times,'failures':failures},indent=2)+'\n')
 assert not failures,(scale,failures)
