#!/usr/bin/env python3
"""Import one benchmark table at a time and publish complete native snapshots."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data', type=Path, required=True)
    p.add_argument('--native', type=Path, required=True)
    p.add_argument('--runner', type=Path, required=True)
    args = p.parse_args()
    manifest = json.loads((args.data/'manifest.json').read_text())
    args.native.mkdir(parents=True, exist_ok=True)
    if any(args.native.iterdir()):
        p.error('native output directory must be empty')
    report = {'scale_factor': manifest['scale_factor'], 'source_manifest_sha256':
              hashlib.sha256((args.data/'manifest.json').read_bytes()).hexdigest(),
              'runner_sha256': hashlib.sha256(args.runner.read_bytes()).hexdigest(), 'status': 'in_progress', 'tables': {}}
    for name, info in manifest['tables'].items():
        if not re.fullmatch('[a-z][a-z0-9_]*', name):
            raise ValueError('Invalid table name')
        destination = args.native/f'{name}.pgaccel'
        temporary = args.native/f'{name}.pgaccel.partial'
        start = time.perf_counter()
        result = subprocess.run([str(args.runner.resolve()), 'parquet', str(args.data/f'{name}.parquet'),
                                 name, '-', '1', '1', '1', str(temporary)],
                                input=f'SELECT count(*) FROM {name};\n', text=True, capture_output=True, check=True,
                                env=dict(os.environ, PGACCEL_BUILD_DICTIONARIES='0'))
        records = [json.loads(line) for line in result.stdout.splitlines()]
        if len(records) != 3 or records[0]['rows'] != info['rows'] or records[1]['status'] != 'saved' or records[2]['rows'] != [[str(info['rows'])]]:
            raise RuntimeError(f'{name}: imported row count or save status mismatch')
        # Publish only after successful import, save, and count verification.
        temporary.rename(destination)
        Path(str(temporary)+'.metadata').rename(Path(str(destination)+'.metadata'))
        report['tables'][name] = {'rows': info['rows'], 'seconds': time.perf_counter()-start,
                                 'bytes': destination.stat().st_size, 'import': records[0]}
        (args.native/'manifest.json').write_text(json.dumps(report, indent=2)+'\n')
        print(name, info['rows'], round(report['tables'][name]['seconds'], 3), flush=True)
    report['status'] = 'complete'
    (args.native/'manifest.json').write_text(json.dumps(report, indent=2)+'\n')


if __name__ == '__main__':
    main()
