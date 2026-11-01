"""Instacart IDs -> FAISS products -> dbt cutoff features -> LambdaMART."""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import time
import faiss
import lightgbm as lgb
import numpy as np
from fastapi import FastAPI,HTTPException,Query
from .data import Dataset
from .features import Context,make_features


class Recommender:
    def __init__(self,artifacts):
        art=Path(artifacts); settings=json.loads((art/'serving.json').read_text())
        self.ds=Dataset(settings['feature_cache']); self.ctx=Context(self.ds,'test')
        assert settings['feature_fingerprint']==self.ds.manifest['feature_fingerprint'],'Serving features differ from the trained artifact'
        self.users=np.load(art/'user_emb.npy',mmap_mode='r')
        self.index=faiss.read_index(str(art/'items_hnsw.faiss')); self.index.hnsw.efSearch=128
        assert len(self.users)==self.ds.n_users and self.index.ntotal==self.ds.n_items
        self.ranker=lgb.Booster(model_file=str(art/'ranker.txt'))
        self.names=json.loads((self.ds.root/'products.json').read_text()); self.k=settings['candidates']
        self.run_id=settings['model_run_id']

    def recommend(self,user_id,k=10,rerank=True):
        faiss.omp_set_num_threads(1)
        u=int(np.searchsorted(self.ds.user_ids,user_id))
        if u==self.ds.n_users or int(self.ds.user_ids[u])!=user_id: raise KeyError(user_id)
        scores,candidates=self.index.search(np.ascontiguousarray(self.users[u:u+1]),self.k)
        if rerank:
            scores=self.ranker.predict(make_features(np.array([u]),candidates,scores,self.ctx),num_threads=1)[None,:]
            order=np.argsort(-scores,axis=1,kind='stable'); candidates=np.take_along_axis(candidates,order,1)
            scores=np.take_along_axis(scores,order,1)
        return [{'product_id':int(self.ds.product_ids[i]),'product_name':self.names[i],'score':float(s)}
                for i,s in zip(candidates[0,:k],scores[0,:k])]


def create_app(artifacts=None):
    @asynccontextmanager
    async def lifespan(app):
        faiss.omp_set_num_threads(1)
        app.state.recommender=Recommender(artifacts or os.environ.get('RECSYS_ARTIFACTS','artifacts'))
        yield
    app=FastAPI(title='Instacart recommendation service',lifespan=lifespan)
    @app.get('/health')
    def health():
        r=app.state.recommender
        return {'status':'ok','dataset':'Instacart 2017','users':r.ds.n_users,'products':r.ds.n_items,'model_run_id':r.run_id}
    @app.get('/recommend/{user_id}')
    def recommend(user_id:int,k:int=Query(10,ge=1,le=100),rerank:bool=True):
        r=app.state.recommender
        if k>r.k: raise HTTPException(400,'k exceeds candidate count')
        start=time.perf_counter()
        try: items=r.recommend(user_id,k,rerank)
        except KeyError: raise HTTPException(404,'unknown user')
        return {'user_id':user_id,'items':items,'handler_ms':(time.perf_counter()-start)*1000}
    return app


app=create_app()
