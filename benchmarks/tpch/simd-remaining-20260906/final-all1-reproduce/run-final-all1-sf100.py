import subprocess,pathlib,json,time,datetime
repo=pathlib.Path('/home/hadi/projects/pgaccel_old')
work=pathlib.Path('/home/hadi/projects/tpch-work')
out=pathlib.Path('/home/hadi/projects/docs/tpch/simd-remaining-20260906')
py=str(work/'venv/bin/python'); queries=str(work/'queries-duckdb-1.4.3')
phases=[]
def run(name,command,unit=None):
    phase={'name':name,'command':list(map(str,command)),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    phases.append(phase)
    with (out/(name+'.log')).open('w') as log:
        p=subprocess.Popen(phase['command'],cwd=repo,stdout=log,stderr=subprocess.STDOUT)
        while p.poll() is None:
            if unit:
                state=subprocess.run(['systemctl','--user','show',unit,'-p','MemoryCurrent','-p','MemoryPeak','-p','MemoryMax','-p','ControlGroup'],capture_output=True,text=True)
                sample={'phase':name,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'properties':state.stdout}
                for line in state.stdout.splitlines():
                    if line.startswith('ControlGroup='):
                        group=pathlib.Path('/sys/fs/cgroup')/line.split('=',1)[1].lstrip('/')
                        for field in ['memory.events','memory.swap.current']:
                            try: sample[field]=(group/field).read_text()
                            except FileNotFoundError:pass
                with (out/'final-all1-sf100-resources.jsonl').open('a') as f:f.write(json.dumps(sample)+'\n')
            time.sleep(2)
    phase['returncode']=p.returncode;phase['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out/'final-all1-sf100-phases.json').write_text(json.dumps(phases,indent=2)+'\n')
    if p.returncode:raise RuntimeError(name)
def capped(name,command):
    unit='pgaccel-simd-'+name
    run(name,['systemd-run','--user','--unit='+unit,'--property=MemoryMax=192G','--property=MemorySwapMax=0','--working-directory='+str(repo),'--setenv=TZ=UTC','--wait','--pipe']+command,unit+'.service')
def compare(engine):
    return [py,'benchmarks/tpch/compare_scale.py','--engine',engine,'--queries',queries,'--output',str(out/(engine+'-final-all1-sf100.jsonl')),'--repeats','9']
capped('duckdb-final-all1-sf100',compare('duckdb')+['--database',str(work/'sf100/duckdb/tpch.duckdb')])
unit='pgaccel-simd-clickhouse-final-all1-sf100.service'
run('final-all1-ch-sf100-start',['systemd-run','--user','--unit='+unit,'--property=MemoryMax=192G','--property=MemorySwapMax=0','--setenv=TZ=UTC','/usr/bin/clickhouse-server','--config-file='+str(work/'sf100/clickhouse/config.xml'),'--pid-file='+str(work/'sf100/clickhouse/server.pid')])
try:
    for _ in range(60):
        if subprocess.run(['curl','-fsS','http://127.0.0.1:18123/ping'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
        time.sleep(1)
    else:raise RuntimeError('ClickHouse startup timeout')
    run('final-all1-ch-sf100-layout',[py,'benchmarks/tpch/prepare_clickhouse.py','--database','tpch_sf100_mergetree','--queries',queries,'--report',str(out/'final-all1-ch-sf100-layout.json')],unit)
    run('clickhouse-final-all1-sf100',compare('clickhouse')+['--database','tpch_sf100_mergetree','--clickhouse-query-overrides',str(work/'queries-clickhouse-double-1.4.3'),'--oracle',str(out/'duckdb-final-all1-sf100.jsonl'),'--server-pid-file',str(work/'sf100/clickhouse/server.pid')],unit)
finally:
    run('final-all1-ch-sf100-stop',['systemctl','--user','stop',unit])
capped('pgaccel-native-final-all1-sf100',compare('pgaccel-native')+['--module',str(work/'checkpoints/final-all-v1/pgaccel_native.cpython-312-x86_64-linux-gnu.so'),'--native',str(work/'sf100/native'),'--oracle',str(out/'duckdb-final-all1-sf100.jsonl')])
