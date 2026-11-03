"""Hash resident Instacart inputs against frozen identities; output a new wrapper."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time

NAMES = ('orders', 'order_products__prior', 'order_products__train', 'products', 'aisles', 'departments')
def sha256_file(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8*1024*1024), b''):
            value.update(block)
    return value.hexdigest()

def verify(raw_dir, frozen_manifest, out):
    raw, frozen, output = Path(raw_dir).resolve(), Path(frozen_manifest).resolve(), Path(out).resolve()
    if output.exists():
        raise FileExistsError('source identity output is create-only')
    records = json.loads(frozen.read_text(encoding='utf-8'))
    if not isinstance(records, list):
        raise ValueError('expected the frozen native_input_manifest list')
    expected = {}
    for record in records:
        if not isinstance(record, dict) or record.get('file') not in {n+'.csv' for n in NAMES}:
            raise ValueError('unexpected frozen source entry')
        name = record['file'][:-4]
        if name in expected or not isinstance(record.get('bytes'), int) or record['bytes'] < 1 or not re.fullmatch('[a-f0-9]{64}', record.get('sha256', '')):
            raise ValueError('duplicate or invalid frozen identity')
        expected[name] = record
    if set(expected) != set(NAMES):
        raise ValueError('frozen identity must contain exactly the six input CSVs')
    started = time.perf_counter()
    sources = {}
    for name in NAMES:
        path = raw / (name+'.csv')
        before = path.stat()
        if before.st_size != expected[name]['bytes']:
            raise ValueError('source size mismatch: '+name)
        digest = sha256_file(path)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or digest != expected[name]['sha256']:
            raise ValueError('source changed or hash mismatch: '+name)
        sources[name] = {'path':str(path), 'bytes':after.st_size, 'mtime_ns':after.st_mtime_ns, 'sha256':digest,
                         'source_url':expected[name].get('url')}
    result = {'schema':'m4-verified-source-identity-v1','sources':sources,'frozen_manifest':str(frozen),
              'frozen_manifest_sha256':sha256_file(frozen),'verified_at_utc':datetime.now(timezone.utc).isoformat(),
              'hash_wall_seconds':time.perf_counter()-started,'command':sys.argv,
              'scope':'fresh byte hashes against the unchanged frozen six-file manifest; no data transformation'}
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x',encoding='utf-8') as stream:
        json.dump(result,stream,indent=2);stream.write('\n')
    return result

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--raw-dir',required=True)
    parser.add_argument('--frozen-manifest',required=True)
    parser.add_argument('--out',required=True)
    args=parser.parse_args()
    print(json.dumps(verify(args.raw_dir,args.frozen_manifest,args.out),indent=2))
