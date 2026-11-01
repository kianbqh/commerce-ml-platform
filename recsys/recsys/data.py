"""Export compact, temporal dbt features; raw order lines stay inside DuckDB."""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import numpy as np
import duckdb


def save(root, name, array):
    array=np.ascontiguousarray(array)
    np.save(Path(root)/f'{name}.npy', array, allow_pickle=False)
    digest=hashlib.sha256(str((array.dtype.str,array.shape)).encode())
    digest.update(memoryview(array).cast('B'))
    return digest.hexdigest()


def definition_hash():
    root=Path(__file__).resolve().parents[2]
    files=sorted((root/'warehouse/models/recommendations').glob('*.sql'))
    files += [Path(__file__),Path(__file__).with_name('features.py')]
    digest=hashlib.sha256()
    for path in files:
        digest.update(path.name.encode()); digest.update(path.read_bytes())
    return digest.hexdigest()


def export(database, directory, force=False):
    root = Path(directory); root.mkdir(parents=True, exist_ok=True)
    database = Path(database).resolve()
    stamp = {'path':str(database),'bytes':database.stat().st_size,'mtime_ns':database.stat().st_mtime_ns,
             'feature_definition_sha256':definition_hash()}
    manifest = root/'manifest.json'
    if not force and manifest.exists() and json.loads(manifest.read_text())['database'] == stamp:
        return Dataset(root)
    con = duckdb.connect(str(database), read_only=True)
    con.execute("set memory_limit='4GB'"); con.execute('set threads=4')
    def arrays(sql): return con.execute(sql).fetchnumpy()
    hashes={}
    def emit(name,array): hashes[name]=save(root,name,array)
    users = arrays('select distinct cast(user_id as integer) user_id from recommendations.rec_order_split order by user_id')['user_id']
    products = arrays('select * from recommendations.rec_product_prior order by product_id')
    product_ids = products['product_id'].astype(np.int32)
    n_users, n_items = len(users), len(product_ids)
    assert n_users > 1 and n_items > 1
    emit('user_ids',users); emit('product_ids',product_ids)
    aisles = np.unique(products['aisle_id']).astype(np.int32)
    departments = np.unique(products['department_id']).astype(np.int32)
    emit('aisles',aisles); emit('departments',departments)
    emit('item_aisle',np.searchsorted(aisles,products['aisle_id']).astype(np.int32))
    emit('item_department',np.searchsorted(departments,products['department_id']).astype(np.int32))
    (root/'products.json').write_text(json.dumps(products['product_name'].tolist(),ensure_ascii=False))
    count = products['purchase_count'].astype(np.float32)
    product_raw = np.column_stack([np.log1p(count),np.log1p(products['buyers']),
                                  products['reorder_count']/np.maximum(count,1)]).astype(np.float32)
    item_mean, item_scale = product_raw.mean(0),np.maximum(product_raw.std(0),1e-6)
    emit('item_features',(product_raw-item_mean)/item_scale); emit('item_prior',product_raw)
    user_mean = user_scale = None
    names = ['log_history_orders','log_item_count','log_unique_products','mean_basket_size']
    names += [f'aisle_affinity_{x}' for x in aisles] + [f'department_affinity_{x}' for x in departments]
    for stage in ('validation','test'):
        h = arrays(f"select * from recommendations.rec_history_user where stage='{stage}' order by user_id")
        assert np.array_equal(h['user_id'], users)
        base = np.column_stack([np.log1p(h['history_orders']),np.log1p(h['item_count']),
                                np.log1p(h['unique_products']),h['item_count']/np.maximum(h['history_orders'],1)]).astype(np.float32)
        affinity = np.zeros((n_users,len(aisles)+len(departments)),np.float32)
        for kind,categories,offset in (('aisle',aisles,0),('department',departments,len(aisles))):
            cats = arrays(f"select cast(user_id as integer) user_id, cast(category_id as integer) category_id, cast(item_count as float) n from recommendations.rec_history_category where stage='{stage}' and kind='{kind}'")
            uid = np.searchsorted(users,cats['user_id'])
            affinity[uid,offset+np.searchsorted(categories,cats['category_id'])] = cats['n']/h['item_count'][uid]
        raw = np.concatenate([base,affinity],axis=1)
        if stage == 'validation': user_mean,user_scale=raw.mean(0),np.maximum(raw.std(0),1e-6)
        emit(f'{stage}_user_features',(raw-user_mean)/user_scale)
        emit(f'{stage}_affinity',affinity)
        emit(f'{stage}_user_history',np.column_stack([h['history_orders'],h['item_count'],h['unique_products'],h['cutoff_order_number']]).astype(np.float32))
        pairs = arrays(f"select cast(user_id as integer) user_id,cast(product_id as integer) product_id,cast(purchase_count as float) purchase_count,cast(last_order_number as float) last_order_number,cast(reorder_count as float) reorder_count from recommendations.rec_history_product where stage='{stage}' order by user_id,product_id")
        key=np.searchsorted(users,pairs['user_id']).astype(np.int64)*n_items+np.searchsorted(product_ids,pairs['product_id'])
        emit(f'{stage}_pair_keys',key)
        emit(f'{stage}_pair_values',np.column_stack([pairs['purchase_count'],pairs['last_order_number'],pairs['reorder_count']]).astype(np.float32))
        labels=arrays(f"select cast(user_id as integer) user_id,cast(product_id as integer) product_id from recommendations.rec_lines where split='{stage}' order by user_id,product_id")
        u=np.searchsorted(users,labels['user_id'])
        emit(f'{stage}_label_items',np.searchsorted(product_ids,labels['product_id']).astype(np.int32))
        emit(f'{stage}_label_ptr',np.concatenate([[0],np.cumsum(np.bincount(u,minlength=n_users))]).astype(np.int64))
        print('EXPORTED_STAGE',stage,'users',n_users,'history_pairs',len(key),'labels',len(u),flush=True)
        del pairs,labels,raw,affinity
    train=arrays("select cast(user_id as integer) user_id,cast(product_id as integer) product_id from recommendations.rec_lines where split='train' order by user_id,order_number,product_id")
    emit('train_users',np.searchsorted(users,train['user_id']).astype(np.int32))
    emit('train_items',np.searchsorted(product_ids,train['product_id']).astype(np.int32))
    split=arrays('select cast(user_id as integer) user_id,cast(order_id as integer) order_id,cast(order_number as integer) order_number,split from recommendations.rec_order_split order by user_id,order_number')
    digest=hashlib.sha256()
    for name in ('user_id','order_id','order_number'): digest.update(split[name].tobytes())
    digest.update('|'.join(split['split']).encode())
    con.execute(f"copy (select * from recommendations.rec_order_split order by user_id,order_number) to '{(root/'order_splits.parquet').as_posix()}' (format parquet)")
    counts=dict(con.execute('select split,count(*) from recommendations.rec_lines group by split').fetchall())
    orders=dict(con.execute('select split,count(*) from recommendations.rec_order_split group by split').fetchall())
    raw_orders,raw_users=con.execute('select count(*),count(distinct user_id) from staging.stg_orders').fetchone()
    if raw_users==n_users:
        # Every raw user is eligible, so rec_order_split contains every labeled
        # order. Its complement is exact without another 33.8M-line CSV scan.
        unlabeled=raw_orders-sum(orders.values())
    else:
        unlabeled=con.execute('select count(*) from staging.stg_orders o where not exists (select 1 from staging.stg_order_products l where l.order_id=o.order_id)').fetchone()[0]
    meta={'database':stamp,'users':n_users,'products':n_items,'line_counts':counts,'order_counts':orders,
          'raw_orders':raw_orders,'excluded_users_fewer_than_three_labeled_baskets':raw_users-n_users,
          'unlabeled_orders':unlabeled,'excluded_labeled_orders':raw_orders-unlabeled-sum(orders.values()),'split_sha256':digest.hexdigest(),
          'split_rule':'last labeled basket test; penultimate validation; all earlier labeled baskets train',
          'test_history':'training history plus the same user completed validation basket',
          'global_priors':'training lines only; fixed for validation and test',
          'user_feature_names':names,'item_feature_names':['log_train_purchases','log_train_buyers','train_reorder_share'],
          'normalizers':{'user_mean':user_mean.tolist(),'user_scale':user_scale.tolist(),'item_mean':item_mean.tolist(),'item_scale':item_scale.tolist()},
          'feature_models':['rec_order_split','rec_lines','rec_history_product','rec_history_user','rec_history_category','rec_product_prior']}
    meta['array_sha256']=hashes
    feature_hashes={key:value for key,value in hashes.items() if '_label_' not in key}
    meta['feature_fingerprint']=hashlib.sha256(json.dumps({'definition':stamp['feature_definition_sha256'],
                                                         'arrays':feature_hashes},sort_keys=True).encode()).hexdigest()
    manifest.write_text(json.dumps(meta,indent=2)+'\n'); con.close()
    return Dataset(root)


@dataclass
class Labels:
    ptr: np.ndarray
    items: np.ndarray
    def for_user(self,user): return self.items[self.ptr[user]:self.ptr[user+1]]


class Dataset:
    def __init__(self,root):
        self.root=Path(root); self.manifest=json.loads((self.root/'manifest.json').read_text())
        self.n_users,self.n_items=self.manifest['users'],self.manifest['products']
        self.user_ids,self.product_ids=self.array('user_ids'),self.array('product_ids')
        self.item_features=self.array('item_features')
        self.item_aisle,self.item_department=self.array('item_aisle'),self.array('item_department')
        self.train_users,self.train_items=self.array('train_users'),self.array('train_items')
    def array(self,name): return np.load(self.root/f'{name}.npy',mmap_mode='r',allow_pickle=False)
    def labels(self,stage): return Labels(self.array(f'{stage}_label_ptr'),self.array(f'{stage}_label_items'))
    def user_features(self,stage): return self.array(f'{stage}_user_features')
