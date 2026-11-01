"""Full-user next-basket retrieval and validation-only LambdaMART fitting."""
from __future__ import annotations
import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import time
import faiss
import lightgbm as lgb
import numpy as np
import torch
from .data import export, Dataset
from .features import Context, FEATURES, make_features
from .metrics import BasketMetrics
from .twotower import TwoTower, train


def candidate_labels(users,candidates,labels):
    result=np.zeros(candidates.shape,np.uint8)
    for row,user in enumerate(users):
        truth=labels.for_user(int(user))
        pos=np.searchsorted(truth,candidates[row]); valid=pos<len(truth)
        result[row]=valid & (truth[np.minimum(pos,len(truth)-1)]==candidates[row])
    return result


def write_json(path,value):
    Path(path).write_text(json.dumps(value,indent=2)+'\n')


def index_for(items):
    assert np.isfinite(items).all()
    index=faiss.IndexHNSWFlat(items.shape[1],32,faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction=100; index.hnsw.efSearch=128
    index.add(np.ascontiguousarray(items,np.float32)); return index


def run(database,cache,artifacts,out,epochs=3,seed=42,batch_users=1024,candidates=100,rounds=120):
    start=time.perf_counter(); art=Path(artifacts); art.mkdir(parents=True,exist_ok=True)
    output=Path(out); output.parent.mkdir(parents=True,exist_ok=True)
    ds=export(database,cache); faiss.omp_set_num_threads(4); torch.set_num_threads(4)
    k=min(candidates,ds.n_items)
    checkpoint=art/'two_tower.pt'
    model_definition=hashlib.sha256(Path(__file__).with_name('twotower.py').read_bytes()).hexdigest()
    identity={'split_sha256':ds.manifest['split_sha256'],'feature_fingerprint':ds.manifest['feature_fingerprint'],
              'model_definition_sha256':model_definition,'epochs':epochs,'seed':seed,'dimension':32}
    if checkpoint.exists():
        saved=torch.load(checkpoint,map_location='cpu',weights_only=False)
        assert saved['identity']==identity,'Existing model belongs to a different run; choose a new artifact directory'
        model=TwoTower(ds); model.load_state_dict(saved['state']); training=saved['training']
        print('REUSE_TRAINED_TWO_TOWER',flush=True)
    else:
        model,training=train(ds,epochs=epochs,seed=seed)
        torch.save({'identity':identity,'state':model.state_dict(),'training':training},checkpoint)
    ue,item=model.embeddings(ds,'validation')
    assert np.isfinite(ue).all()
    np.save(art/'item_emb.npy',item); np.save(art/'validation_user_emb.npy',ue)
    index=index_for(item); faiss.write_index(index,str(art/'items_hnsw.faiss'))
    labels=ds.labels('validation'); ctx=Context(ds,'validation')
    permutation=np.random.default_rng(seed).permutation(ds.n_users)
    split=max(1,min(ds.n_users-1,int(.9*ds.n_users)))
    fit_users,stop_users=permutation[:split],permutation[split:]
    np.save(art/'rank_fit_users.npy',ds.user_ids[fit_users]); np.save(art/'rank_early_stop_users.npy',ds.user_ids[stop_users])
    rank_identity={**identity,'candidates':k,'rounds':rounds,'fit_user_sha256':hashlib.sha256(fit_users.tobytes()).hexdigest(),
                   'early_stop_user_sha256':hashlib.sha256(stop_users.tobytes()).hexdigest(),
                   'ranking_definition_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    rank_path=art/'ranker.txt'; rank_meta_path=art/'ranker.json'
    if rank_path.exists():
        rank_info=json.loads(rank_meta_path.read_text()); assert rank_info['identity']==rank_identity
        ranker=lgb.Booster(model_file=str(rank_path)); print('REUSE_TRAINED_RANKER',flush=True)
    else:
        coverage=BasketMetrics((k,)); matrices=[]; targets=[]; fit_start=time.perf_counter()
        for name,users in (('fit',fit_users),('early_stop',stop_users)):
            x=np.lib.format.open_memmap(art/f'{name}_features.npy',mode='w+',dtype=np.float32,shape=(len(users)*k,len(FEATURES)))
            y=np.lib.format.open_memmap(art/f'{name}_labels.npy',mode='w+',dtype=np.uint8,shape=(len(users)*k,))
            for offset in range(0,len(users),batch_users):
                u=users[offset:offset+batch_users]
                scores,c=index.search(np.ascontiguousarray(ue[u]),k)
                assert (c>=0).all() and np.isfinite(scores).all()
                values=candidate_labels(u,c,labels)
                x[offset*k:(offset+len(u))*k]=make_features(u,c,scores,ctx)
                y[offset*k:(offset+len(u))*k]=values.ravel()
                coverage.add(c,u,labels)
                if (offset//batch_users+1)%25==0:
                    print('RANKING_FEATURE_USERS',name,offset+len(u),'/',len(users),flush=True)
            x.flush(); y.flush(); matrices.append(x); targets.append(y)
            print('RANKING_FEATURES',name,'users',len(users),'rows',len(y),'positives',int(y.sum()),flush=True)
        feature_seconds=time.perf_counter()-fit_start; fit_start=time.perf_counter()
        params={'objective':'lambdarank','metric':'ndcg','ndcg_eval_at':[10],'learning_rate':.06,
                'num_leaves':15,'min_data_in_leaf':100,'max_bin':63,'num_threads':4,'seed':seed,
                'deterministic':True,'force_col_wise':True,'verbosity':-1}
        fitting=lgb.Dataset(matrices[0],label=targets[0],group=np.full(len(fit_users),k),feature_name=FEATURES,params={'max_bin':63})
        stopping=lgb.Dataset(matrices[1],label=targets[1],group=np.full(len(stop_users),k),reference=fitting,feature_name=FEATURES)
        ranker=lgb.train(params,fitting,num_boost_round=rounds,valid_sets=[stopping],valid_names=['validation_unseen_users'],
                         callbacks=[lgb.early_stopping(15,verbose=True),lgb.log_evaluation(20)])
        ranker.save_model(str(rank_path))
        rank_info={'identity':rank_identity,'fit_users':len(fit_users),'early_stop_users':len(stop_users),
                   'fit_candidate_rows':len(fit_users)*k,'early_stop_candidate_rows':len(stop_users)*k,
                   'fit_positive_rows':int(targets[0].sum()),'early_stop_positive_rows':int(targets[1].sum()),
                   'candidate_coverage':coverage.result(),'feature_seconds':feature_seconds,'fit_seconds':time.perf_counter()-fit_start,
                   'best_iteration':ranker.best_iteration,'features':FEATURES,
                   'feature_gain':dict(zip(FEATURES,map(float,ranker.feature_importance('gain')))),
                   'labels':'validation baskets only; deterministic disjoint user groups for fitting and early stopping',
                   'early_stopping_metric':'LightGBM built-in NDCG@10 over the candidate labels; published next-basket metrics use every held-out positive'}
        write_json(rank_meta_path,rank_info)
        del fitting,stopping,matrices,targets; gc.collect()
    del ctx,ue; gc.collect()
    # Test labels are first used below, after both trained models and selections are fixed.
    ue,item=model.embeddings(ds,'test'); del model; gc.collect()
    assert np.isfinite(ue).all() and np.isfinite(item).all()
    np.save(art/'user_emb.npy',ue)
    labels=ds.labels('test'); ctx=Context(ds,'test')
    retrieval=BasketMetrics((10,k)); reranked=BasketMetrics((10,k)); popularity=BasketMetrics((10,k))
    popular=np.argsort(-ctx.prior[:,0],kind='stable')[:k]
    eval_start=time.perf_counter()
    for offset in range(0,ds.n_users,batch_users):
        users=np.arange(offset,min(offset+batch_users,ds.n_users))
        scores,c=index.search(np.ascontiguousarray(ue[users]),k)
        assert (c>=0).all() and (c<ds.n_items).all() and np.isfinite(scores).all()
        prediction=ranker.predict(make_features(users,c,scores,ctx),num_threads=4).reshape(len(users),k)
        assert np.isfinite(prediction).all()
        ranked=np.take_along_axis(c,np.argsort(-prediction,axis=1,kind='stable'),axis=1)
        retrieval.add(c,users,labels); reranked.add(ranked,users,labels)
        popularity.add(np.broadcast_to(popular,(len(users),k)),users,labels)
        if offset%(batch_users*25)==0: print('EVALUATED_TEST_USERS',offset+len(users),'/',ds.n_users,flush=True)
    evaluation_seconds=time.perf_counter()-eval_start
    diagnostic_users=np.sort(np.random.default_rng(seed).choice(ds.n_users,min(2000,ds.n_users),replace=False))
    exact=faiss.IndexFlatIP(item.shape[1]); exact.add(item)
    _,a=exact.search(np.ascontiguousarray(ue[diagnostic_users]),k)
    _,b=index.search(np.ascontiguousarray(ue[diagnostic_users]),k)
    ann_recall=float(np.mean([len(set(x)&set(y))/k for x,y in zip(a,b)]))
    run_id=hashlib.sha256(json.dumps({'ranker':rank_identity,'weights':training['final_weights_sha256']},sort_keys=True).encode()).hexdigest()
    write_json(art/'serving.json',{'feature_cache':str(Path(cache).resolve()),'candidates':k,'model_run_id':run_id,
               'dataset':'Instacart 2017','history_stage':'test','split_sha256':ds.manifest['split_sha256'],
               'feature_fingerprint':ds.manifest['feature_fingerprint']})
    result={'dataset':ds.manifest,'two_tower':training,'ranker':rank_info,'model_run_id':run_id,
            'test':{'two_tower_hnsw':retrieval.result(),'two_tower_lambdamart':reranked.result(),'training_popularity':popularity.result()},
            'faiss':{'index':'HNSW32 inner product on normalized vectors','efConstruction':100,'efSearch':128,
                     'catalog_products':ds.n_items,'reorders_eligible':True,'exact_topk_agreement':ann_recall,
                     'agreement_users':len(diagnostic_users),'agreement_user_sha256':hashlib.sha256(diagnostic_users.tobytes()).hexdigest()},
            'evaluation_seconds':evaluation_seconds,'total_pipeline_seconds':time.perf_counter()-start,
            'runtime':{'python':platform.python_version(),'torch':torch.__version__,'numpy':np.__version__,
                       'faiss':faiss.__version__,'lightgbm':lgb.__version__,'threads':4,'device':'cpu',
                       'peak_process_rss_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024},
            'timing_scope':'elapsed execution records; model fitting/evaluation can overlap separate GPU training, not a latency benchmark'}
    write_json(output,result)
    print('COMMERCE_TRAIN_EVAL_COMPLETE',output,flush=True)
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--database',default=os.environ.get('DUCKDB_PATH','commerce.duckdb'))
    p.add_argument('--cache',default=os.environ.get('COMMERCE_CACHE','data/features'))
    p.add_argument('--artifacts',default=os.environ.get('RECSYS_ARTIFACTS','artifacts'))
    p.add_argument('--out',default='results/metrics.json'); p.add_argument('--epochs',type=int,default=3)
    p.add_argument('--rounds',type=int,default=120); p.add_argument('--seed',type=int,default=42)
    p.add_argument('--candidates',type=int,default=100)
    args=p.parse_args(); run(**vars(args))


if __name__=='__main__': main()
