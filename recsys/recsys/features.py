"""Candidate features consume dbt's cutoff tables through compact exported arrays."""
import numpy as np
FEATURES=['tt_score','tt_rank','log_train_purchases','log_train_buyers','train_reorder_share',
          'log_history_orders','log_history_items','log_unique_products','user_product_count',
          'user_product_frequency','orders_since_product','user_product_reorder_share',
          'aisle_affinity','department_affinity','aisle_id','department_id']


class Context:
    def __init__(self,dataset,stage):
        self.ds=dataset; self.stage=stage; self.history=dataset.array(f'{stage}_user_history')
        self.affinity=dataset.array(f'{stage}_affinity'); self.keys=dataset.array(f'{stage}_pair_keys')
        self.values=dataset.array(f'{stage}_pair_values'); self.prior=dataset.array('item_prior')
        self.n_aisles=len(dataset.array('aisles'))
    def pair_history(self,users,items):
        keys=users.astype(np.int64)*self.ds.n_items+items
        positions=np.searchsorted(self.keys,keys); found=positions<len(self.keys)
        positions=np.minimum(positions,len(self.keys)-1); found &= self.keys[positions]==keys
        return np.where(found[:,None],self.values[positions],0.)


def make_features(uids,candidates,scores,ctx):
    n,k=candidates.shape; u=np.repeat(uids,k); c=candidates.ravel()
    hist=ctx.history[u]; pairs=ctx.pair_history(u,c)
    aisle=ctx.ds.item_aisle[c]; department=ctx.ds.item_department[c]
    age=np.where(pairs[:,0]>0,hist[:,3]-pairs[:,1],hist[:,0]+1)
    result=np.column_stack([scores.ravel(),np.tile(np.arange(k),n),ctx.prior[c],np.log1p(hist[:,:3]),
                            pairs[:,0],pairs[:,0]/np.maximum(hist[:,0],1),age,pairs[:,2]/np.maximum(pairs[:,0],1),
                            ctx.affinity[u,aisle],ctx.affinity[u,ctx.n_aisles+department],aisle,department]).astype(np.float32)
    assert result.shape==(n*k,len(FEATURES)) and np.isfinite(result).all()
    return result
