"""Keep complete answers externally and compact timing evidence in the repo."""
import copy
import hashlib
import json
from pathlib import Path

def save_report(root, name, report):
    raw_dir = Path('/home/hadi/projects/docs/tpch/memory-8g-20260908/raw')
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(report, indent=2) + '\n'
    raw_path = raw_dir/name
    raw_path.write_text(raw)
    compact = copy.deepcopy(report)
    compact.pop('stdout', None)
    compact['raw_file'] = str(raw_path)
    compact['raw_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    for trial in compact.get('measurements', {}).get('trials', []):
        rows = trial.pop('rows', None)
        if rows is not None:
            trial['row_count'] = len(rows)
            trial['rows_sha256'] = hashlib.sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()
    (root/name).write_text(json.dumps(compact, indent=2)+'\n')
