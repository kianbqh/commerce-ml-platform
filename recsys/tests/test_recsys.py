"""Real dbt -> learned retrieval/ranking -> API contracts on synthetic CI baskets."""
import json
import os
from pathlib import Path
import subprocess
import duckdb
import numpy as np
import pytest
import torch
from recsys.data import Labels,export
from recsys.features import Context,FEATURES,make_features
from recsys.fixture import write_fixture
from recsys.metrics import BasketMetrics
from recsys.pipeline import run
from recsys.twotower import TwoTower

WAREHOUSE=Path(__file__).resolve().parents[2]/'warehouse'


def build(root,change_test=False):
    raw=root/'raw'; write_fixture(raw,change_test)
    database=root/'fixture.duckdb'
    env={**os.environ,'RAW_DIR':str(raw),'DUCKDB_PATH':str(database),'DBT_PROFILES_DIR':str(WAREHOUSE),
         'DBT_SEND_ANONYMOUS_USAGE_STATS':'false'}
    command=['dbt','build','--select','path:models/staging','path:models/recommendations',
             '--indirect-selection','cautious','--target-path',str(root/'target'),'--log-path',str(root/'logs')]
    result=subprocess.run(command,cwd=WAREHOUSE,env=env,capture_output=True,text=True)
    (root/'dbt.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stdout[-8000:]+result.stderr[-2000:]
    return database,export(database,root/'features')


@pytest.fixture(scope='module')
def prepared(tmp_path_factory):
    root=tmp_path_factory.mktemp('commerce'); database,ds=build(root)
    return root,database,ds


@pytest.fixture(scope='module')
def trained(prepared):
    root,database,ds=prepared
    result=run(database,root/'features',root/'artifacts',root/'metrics.json',epochs=1,rounds=3,candidates=12)
    return root,ds,result


def test_multi_positive_metrics_hand_computed():
    labels=Labels(np.array([0,3,4]),np.array([1,2,5,4]))
    metrics=BasketMetrics((3,)); metrics.add(np.array([[1,9,2],[8,4,0]]),np.array([0,1]),labels)
    result=metrics.result()
    assert result['recall@3']==pytest.approx((2/3+1)/2)
    expected=((1+1/np.log2(4))/(1+1/np.log2(3)+1/np.log2(4))+1/np.log2(3))/2
    assert result['ndcg@3']==pytest.approx(expected)
    assert result['micro_recall@3']==pytest.approx(3/4)


def test_metrics_reject_duplicate_recommendations():
    with pytest.raises(ValueError,match='Duplicate'):
        BasketMetrics((2,)).add(np.array([[1,1]]),np.array([0]),Labels(np.array([0,1]),np.array([1])))


def test_temporal_basket_split_and_unlabeled_exclusions(prepared):
    _,database,ds=prepared
    assert (ds.n_users,ds.n_items)==(40,24)
    assert ds.manifest['order_counts']=={'train':160,'validation':40,'test':40}
    assert ds.manifest['unlabeled_orders']==40
    assert ds.manifest['excluded_users_fewer_than_three_labeled_baskets']==1
    assert ds.manifest['excluded_labeled_orders']==2
    with duckdb.connect(str(database),read_only=True) as con:
        rows=con.execute("select split,min(order_number),max(order_number) from recommendations.rec_order_split group by split").fetchall()
        assert dict((s,(a,b)) for s,a,b in rows)=={'train':(1,4),'validation':(5,5),'test':(6,6)}


def test_features_equal_actual_dbt_cutoff_values(prepared):
    _,database,ds=prepared; ctx=Context(ds,'validation')
    with duckdb.connect(str(database),read_only=True) as con:
        product,count,last,reorders=con.execute("select product_id,purchase_count,last_order_number,reorder_count from recommendations.rec_history_product where stage='validation' and user_id=1001 order by product_id limit 1").fetchone()
    item=int(np.searchsorted(ds.product_ids,product))
    values=make_features(np.array([0]),np.array([[item]]),np.zeros((1,1),np.float32),ctx)
    assert values.shape==(1,len(FEATURES)) and np.isfinite(values).all()
    assert values[0,FEATURES.index('user_product_count')]==count
    assert values[0,FEATURES.index('orders_since_product')]==4-last
    assert values[0,FEATURES.index('user_product_frequency')]==pytest.approx(count/4)
    assert values[0,FEATURES.index('user_product_reorder_share')]==pytest.approx(reorders/count)


def test_test_basket_perturbation_cannot_change_training_features(prepared,tmp_path):
    _,_,original=prepared; _,changed=build(tmp_path,change_test=True)
    for name in ('train_users','train_items','item_prior','item_features','validation_user_features',
                 'validation_pair_keys','validation_pair_values','test_user_features','test_pair_values'):
        np.testing.assert_array_equal(original.array(name),changed.array(name))
    assert not np.array_equal(original.labels('test').items,changed.labels('test').items)
    assert original.manifest['normalizers']==changed.manifest['normalizers']


def test_learned_two_tower_consumes_history_features(trained):
    root,ds,result=trained
    saved=torch.load(root/'artifacts/two_tower.pt',map_location='cpu',weights_only=False)
    model=TwoTower(ds); model.load_state_dict(saved['state']); model.eval()
    features=torch.from_numpy(np.array(ds.user_features('validation')[:2])); users=torch.tensor([0,1])
    with torch.inference_mode():
        a=model.users(users,features).numpy(); b=model.users(users,features+.5).numpy()
    assert not np.allclose(a,b)
    fit=result['two_tower']; assert fit['initial_weights_sha256']!=fit['final_weights_sha256']
    assert fit['epochs'][0]['examples']==len(ds.train_users)


def test_all_fixture_users_evaluated_with_disjoint_ranking_groups(trained):
    root,ds,result=trained
    fit=np.load(root/'artifacts/rank_fit_users.npy'); stop=np.load(root/'artifacts/rank_early_stop_users.npy')
    assert not np.intersect1d(fit,stop).size and len(fit)+len(stop)==ds.n_users
    for metrics in result['test'].values():
        assert metrics['users']==40
        assert metrics['heldout_positive_products']==len(ds.labels('test').items)
        assert 0<=metrics['recall@10']<=1 and 0<=metrics['ndcg@10']<=1
    assert result['ranker']['labels'].startswith('validation baskets only')


def test_instacart_api_returns_external_ids_and_products(trained):
    from fastapi.testclient import TestClient
    from recsys.serve import create_app
    root,ds,_=trained
    with TestClient(create_app(root/'artifacts')) as client:
        assert client.get('/health').json()['dataset']=='Instacart 2017'
        response=client.get('/recommend/1001',params={'k':10}); assert response.status_code==200
        body=response.json(); items=body['items']
        assert body['user_id']==1001 and len(items)==len({x['product_id'] for x in items})==10
        assert all(x['product_id'] in ds.product_ids and x['product_name'].startswith('fixture grocery') and np.isfinite(x['score']) for x in items)
        assert client.get('/recommend/0').status_code==404
        assert client.get('/recommend/1001',params={'k':13}).status_code==400


def test_changed_feature_definition_rejects_stale_model(trained,prepared,tmp_path,monkeypatch):
    from recsys import data
    root,_,_=trained; _,database,_=prepared
    monkeypatch.setattr(data,'definition_hash',lambda:'changed_feature_contract')
    changed=export(database,tmp_path/'features')
    old=json.loads((root/'features/manifest.json').read_text())
    assert changed.manifest['feature_fingerprint']!=old['feature_fingerprint']
    with pytest.raises(AssertionError,match='different run'):
        run(database,tmp_path/'features',root/'artifacts',tmp_path/'metrics.json',epochs=1,rounds=3,candidates=12)
