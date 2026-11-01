"""Closed-loop real HTTP requests with a validated Instacart catalog response."""
import argparse
import asyncio
from collections import Counter
from datetime import datetime,timezone
import json
import math
import os
from pathlib import Path
import time
import httpx
import numpy as np


def validate(response,user,k,catalog):
    response.raise_for_status(); body=response.json(); items=body['items']
    ids=[item['product_id'] for item in items]
    assert body['user_id']==int(user) and len(ids)==len(set(ids))==k
    assert all(product in catalog for product in ids)
    assert all(item['product_name']==catalog[item['product_id']] and item['product_name'] and math.isfinite(item['score']) for item in items)
    assert math.isfinite(body['handler_ms']) and body['handler_ms']>=0
    return body['handler_ms']


async def run(url,cache,n,concurrency,k,out):
    root=Path(cache); users=np.load(root/'user_ids.npy'); products=np.load(root/'product_ids.npy')
    catalog=dict(zip(map(int,products),json.loads((root/'products.json').read_text())))
    statuses=Counter(); failures=Counter(); records=[]
    queries=np.random.default_rng(0).choice(users,n+50,replace=True)
    started=datetime.now(timezone.utc).isoformat()
    async with httpx.AsyncClient(base_url=url,timeout=30) as client:
        response=await client.get('/health'); response.raise_for_status(); health=response.json()
        assert health['users']==len(users) and health['products']==len(products)
        serving=json.loads((Path(os.environ.get('RECSYS_ARTIFACTS','artifacts'))/'serving.json').read_text())
        assert health['model_run_id']==serving['model_run_id'],'Service identity differs from the selected artifacts'
        for user in queries[:50]:
            validate(await client.get(f'/recommend/{user}',params={'k':k}),user,k,catalog)
        queue=asyncio.Queue()
        for i,user in enumerate(queries[50:]): queue.put_nowait((i,int(user)))
        async def worker():
            while not queue.empty():
                try: i,user=queue.get_nowait()
                except asyncio.QueueEmpty: return
                begin=time.perf_counter(); record={'request':i,'user_id':user}
                try:
                    response=await client.get(f'/recommend/{user}',params={'k':k})
                    record['client_ms']=(time.perf_counter()-begin)*1000
                    statuses[str(response.status_code)]+=1
                    record['handler_ms']=validate(response,user,k,catalog)
                    record['status']=response.status_code
                except Exception as error:
                    failures[type(error).__name__]+=1; record['error']=type(error).__name__
                record.setdefault('client_ms',(time.perf_counter()-begin)*1000); records.append(record)
        begin=time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        seconds=time.perf_counter()-begin
    records.sort(key=lambda record:record['request'])
    def percentiles(values): return {f'p{p}':float(np.percentile(values,p)) for p in (50,95,99)} if values else {}
    result={'started_utc':started,'finished_utc':datetime.now(timezone.utc).isoformat(),
            'requests':n,'concurrency':concurrency,'warmup_requests':50,'seed':0,'k':k,
            'users':len(users),'products':len(products),'model_run_id':health['model_run_id'],
            'successful_responses':sum('handler_ms' in r for r in records),
            'errors':sum(failures.values()),'failures':dict(failures),'status_counts':dict(statuses),
            'elapsed_seconds':seconds,'throughput_rps':n/seconds,
            'client_latency_ms':percentiles([r['client_ms'] for r in records]),
            'handler_latency_ms':percentiles([r['handler_ms'] for r in records if 'handler_ms' in r]),
            'protocol':'closed-loop persistent localhost HTTP; client timer includes queueing within the server and network/serialization; handler timer covers retrieval/features/reranking/response construction',
            'raw_requests':records}
    Path(out).parent.mkdir(parents=True,exist_ok=True); Path(out).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({key:value for key,value in result.items() if key!='raw_requests'},indent=2))
    assert len(records)==n and result['successful_responses']==n and not failures


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--url',default='http://127.0.0.1:8088')
    parser.add_argument('--cache',default=os.environ.get('COMMERCE_CACHE','data/features'))
    parser.add_argument('-n',type=int,default=5000); parser.add_argument('-c','--concurrency',type=int,default=8)
    parser.add_argument('-k',type=int,default=10); parser.add_argument('--out',default='results/api_bench.json')
    args=parser.parse_args(); assert min(args.n,args.concurrency,args.k)>0
    asyncio.run(run(**vars(args)))
