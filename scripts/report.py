"""Validate and render saved full-data results without rerunning fitting or timing."""
import hashlib
import json
import math
import os
import shutil
from pathlib import Path
import numpy as np

ROOT=Path(__file__).resolve().parents[1]


def sync_experiments():
    canonical=ROOT/'results/experiments'
    files={'results.json':ROOT/'experiments/reports/results.json',
           'validation.json':ROOT/'experiments/reports/validation.json'}
    for path in (ROOT/'experiments/reports/figures').glob('*.png'):
        files[f'figures/{path.name}']=path
    for name in ('ab_test_readout.ipynb','ab_test_readout.html'):
        files[f'notebooks/{name}']=ROOT/'experiments/notebooks'/name
    records=[]
    for name,source in files.items():
        target=canonical/name; target.parent.mkdir(parents=True,exist_ok=True)
        digest=hashlib.sha256(source.read_bytes()).hexdigest()
        if not target.exists() or hashlib.sha256(target.read_bytes()).hexdigest()!=digest:
            shutil.copy2(source,target)
        assert hashlib.sha256(target.read_bytes()).hexdigest()==digest
        records.append({'source':source.relative_to(ROOT).as_posix(),
                        'canonical':target.relative_to(ROOT).as_posix(),
                        'bytes':source.stat().st_size,'source_sha256':digest,'canonical_sha256':digest})
    checks=json.loads((canonical/'validation.json').read_text())
    assert checks['notebook_errors']==0 and checks['executed_notebook_cells']==6
    (canonical/'provenance.json').write_text(json.dumps({'method':'byte-identical synchronization of executed outputs; no analysis rerun','files':records},indent=2)+'\n')
    print('EXPERIMENT_RESULTS_SYNCED')


def main():
    sync_experiments()
    result=json.loads((ROOT/'results/metrics.json').read_text())
    api=json.loads((ROOT/'results/api_bench.json').read_text()) if (ROOT/'results/api_bench.json').exists() else None
    ds=result['dataset']; users=ds['users']; k=result['ranker']['identity']['candidates']
    assert result['ranker']['fit_users']+result['ranker']['early_stop_users']==users
    assert result['ranker']['identity']['feature_fingerprint']==ds['feature_fingerprint']
    assert all(epoch['examples']==ds['line_counts']['train'] for epoch in result['two_tower']['epochs'])
    for metrics in result['test'].values():
        assert metrics['users']==users and metrics['heldout_positive_products']==ds['line_counts']['test']
        assert all(math.isfinite(v) and 0<=v<=1 for key,v in metrics.items() if '@' in key)
    assert result['test']['two_tower_hnsw'][f'recall@{k}']==result['test']['two_tower_lambdamart'][f'recall@{k}']
    section=f"Full-data run: **{users:,} users, {ds['products']:,} products and {sum(ds['line_counts'].values()):,} labeled order lines**. The test baskets contain {ds['line_counts']['test']:,} positives across all {users:,} users.\n\n"
    section+='| Method | Next-basket NDCG@10 | Recall@10 | Candidate Recall@'+str(k)+' |\n|---|---:|---:|---:|\n'
    for key,name in (('training_popularity','Training popularity'),('two_tower_hnsw','Two-tower + HNSW'),('two_tower_lambdamart','Two-tower + LambdaMART')):
        row=result['test'][key]
        section+=f"| {name} | {row['ndcg@10']:.5f} | {row['recall@10']:.5f} | {row[f'recall@{k}']:.5f} |\n"
    coverage=result['ranker']['candidate_coverage']
    section+=f"\nValidation candidate Recall@{k}: {coverage[f'recall@{k}']:.5f} macro / {coverage[f'micro_recall@{k}']:.5f} micro. LambdaMART fits {result['ranker']['fit_users']:,} user groups and monitors {result['ranker']['early_stop_users']:,} disjoint groups for early stopping. The selected model has {result['ranker']['best_iteration']} trees from a {result['ranker']['identity']['rounds']}-round cap. HNSW's exact top-{k} agreement is {result['faiss']['exact_topk_agreement']:.5f} on a seeded {result['faiss']['agreement_users']:,}-user diagnostic.\n\n"
    section+=f"{len(result['two_tower']['epochs'])} CPU epochs used every one of the {ds['line_counts']['train']:,} training purchase rows per epoch. These are elapsed execution records: two-tower fitting {result['two_tower']['seconds']:.1f}s, ranker fitting {result['ranker']['fit_seconds']:.1f}s. The full quality evaluation above uses every eligible user; the separate ANN diagnostic is explicitly smaller.\n"
    if api:
        assert api['model_run_id']==result['model_run_id'],'HTTP measurements belong to a different model run'
        serving=json.loads((Path(os.environ.get('RECSYS_ARTIFACTS',ROOT/'artifacts'))/'serving.json').read_text())
        assert api['model_run_id']==serving['model_run_id'] and serving['feature_fingerprint']==ds['feature_fingerprint']
        assert api['errors']==0 and api['successful_responses']==api['requests']==len(api['raw_requests'])
        client=np.array([r['client_ms'] for r in api['raw_requests']]); handler=np.array([r['handler_ms'] for r in api['raw_requests']])
        assert np.isfinite(client).all() and np.isfinite(handler).all() and (client>0).all()
        for p in (50,95,99):
            assert abs(api['client_latency_ms'][f'p{p}']-np.percentile(client,p))<1e-9
            assert abs(api['handler_latency_ms'][f'p{p}']-np.percentile(handler,p))<1e-9
        section+=f"\n**HTTP:** {api['requests']:,}/{api['requests']:,} validated responses, zero errors, concurrency {api['concurrency']}, after {api['warmup_requests']} excluded warmups. Client p50/p95/p99: {api['client_latency_ms']['p50']:.2f}/{api['client_latency_ms']['p95']:.2f}/{api['client_latency_ms']['p99']:.2f}ms; handler p50/p95/p99: {api['handler_latency_ms']['p50']:.2f}/{api['handler_latency_ms']['p95']:.2f}/{api['handler_latency_ms']['p99']:.2f}ms. Throughput {api['throughput_rps']:.1f} requests/s.\n"
        section+=f"\nThe HTTP run used one Uvicorn worker, one FAISS/LightGBM thread per call and {api['concurrency']} closed-loop clients on an i7-12700KF under WSL2. The client timer ends at complete response-body arrival; the handler timer covers retrieval, feature lookup, reranking and item construction. The recorded WSL process snapshot contained only the task keepalive before launch. [Environment](results/environment.json) · [WSL process snapshot](results/api-background-processes.txt).\n"
    section+='\n[Full metrics and fit provenance](results/metrics.json) · [every HTTP request](results/api_bench.json) · [split and feature hashes](results/dataset.json).\n'
    readme=ROOT/'README.md'; text=readme.read_text()
    before,marker,after=text.partition('<!-- RESULTS -->'); assert marker
    readme.write_text(before+marker+'\n\n'+section+'\n'+after[after.index('## What the split means'):])
    table=section.replace('(results/','(')
    (ROOT/'results/table.md').write_text('# Instacart next-basket results\n\n'+table)
    (ROOT/'results/dataset.json').write_text(json.dumps(ds,indent=2)+'\n')
    if os.environ.get('RECSYS_ARTIFACTS'):
        root=Path(os.environ['RECSYS_ARTIFACTS']); hashes=[]
        for path in sorted(root.iterdir()):
            if path.name in ('two_tower.pt','ranker.txt','items_hnsw.faiss','user_emb.npy','item_emb.npy','rank_fit_users.npy','rank_early_stop_users.npy','serving.json'):
                digest=hashlib.sha256()
                with path.open('rb') as stream:
                    while chunk:=stream.read(1024*1024): digest.update(chunk)
                hashes.append({'file':path.name,'bytes':path.stat().st_size,'sha256':digest.hexdigest()})
        (ROOT/'results/artifact_hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
    (ROOT/'results/validation.json').write_text(json.dumps({'status':'passed','evaluated_all_users':users,
        'training_rows_each_epoch':ds['line_counts']['train'],'multi_positive_test_labels':ds['line_counts']['test'],
        'feature_fingerprint':ds['feature_fingerprint'],'http_percentiles_recomputed':bool(api),
        'model_run_id':result['model_run_id'],'serving_model_run_id':serving['model_run_id'] if api else None,
        'http_validated_responses':api['successful_responses'] if api else None},indent=2)+'\n')
    print('COMMERCE_RESULTS_VALIDATED')


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--sync-experiments-only',action='store_true')
    if parser.parse_args().sync_experiments_only: sync_experiments()
    else: main()
