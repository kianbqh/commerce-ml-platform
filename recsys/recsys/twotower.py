"""A CPU two-tower model with learned IDs and actual dbt history features."""
import hashlib
import time
import numpy as np
import torch
from torch import nn
from torch.nn import functional as F


class TwoTower(nn.Module):
    def __init__(self,dataset,dim=32):
        super().__init__()
        self.user_id=nn.Embedding(dataset.n_users,dim,sparse=True)
        self.product_id=nn.Embedding(dataset.n_items,dim,sparse=True)
        self.aisle=nn.Embedding(len(dataset.array('aisles')),dim,sparse=True)
        self.department=nn.Embedding(len(dataset.array('departments')),dim,sparse=True)
        self.user_dense=nn.Linear(dataset.user_features('validation').shape[1],dim)
        self.item_dense=nn.Linear(dataset.item_features.shape[1],dim)
        for table in (self.user_id,self.product_id,self.aisle,self.department):
            nn.init.normal_(table.weight,std=.05)
        self.register_buffer('item_features',torch.from_numpy(dataset.item_features.copy()))
        self.register_buffer('item_aisle',torch.from_numpy(dataset.item_aisle.astype(np.int64)))
        self.register_buffer('item_department',torch.from_numpy(dataset.item_department.astype(np.int64)))

    def users(self,ids,features):
        return F.normalize(self.user_id(ids)+self.user_dense(features),dim=-1)

    def items(self,ids):
        return F.normalize(self.product_id(ids)+self.aisle(self.item_aisle[ids])+
                           self.department(self.item_department[ids])+self.item_dense(self.item_features[ids]),dim=-1)

    @torch.inference_mode()
    def embeddings(self,dataset,stage,batch=4096):
        self.eval(); user=[]; item=[]; features=dataset.user_features(stage)
        for start in range(0,dataset.n_users,batch):
            ids=torch.arange(start,min(start+batch,dataset.n_users))
            user.append(self.users(ids,torch.from_numpy(np.array(features[start:start+batch]))).numpy())
        for start in range(0,dataset.n_items,batch):
            item.append(self.items(torch.arange(start,min(start+batch,dataset.n_items))).numpy())
        return np.concatenate(user),np.concatenate(item)


def weight_hash(model):
    digest=hashlib.sha256()
    for name,parameter in model.named_parameters():
        digest.update(name.encode()); digest.update(parameter.detach().numpy().tobytes())
    return digest.hexdigest()


def train(dataset,epochs=3,batch=4096,negatives=4,seed=42,dim=32):
    torch.manual_seed(seed); torch.set_num_threads(4)
    rng=np.random.default_rng(seed); model=TwoTower(dataset,dim)
    before=weight_hash(model)
    sparse=[table.weight for table in (model.user_id,model.product_id,model.aisle,model.department)]
    sparse_optimizer=torch.optim.SparseAdam(sparse,lr=.01)
    dense_optimizer=torch.optim.Adam([*model.user_dense.parameters(),*model.item_dense.parameters()],lr=.002)
    features=dataset.user_features('validation')
    order=np.arange(len(dataset.train_users),dtype=np.int32)
    records=[]; all_start=time.perf_counter()
    for epoch in range(epochs):
        model.train(); rng.shuffle(order); total=0.; examples=0; start=time.perf_counter()
        for offset in range(0,len(order),batch):
            ix=order[offset:offset+batch]
            u=torch.from_numpy(dataset.train_users[ix].astype(np.int64))
            p=torch.from_numpy(dataset.train_items[ix].astype(np.int64))
            uf=torch.from_numpy(np.array(features[u.numpy()]))
            n=torch.randint(dataset.n_items,(len(ix),negatives))
            # Exact positive collisions are replaced; other observed products can be sampled
            # as implicit negatives. Validation/test labels are never consulted here.
            n=torch.where(n==p[:,None],(n+1)%dataset.n_items,n)
            ue=model.users(u,uf); pe=model.items(p); ne=model.items(n)
            pos=(ue*pe).sum(-1)/.1; neg=(ue[:,None,:]*ne).sum(-1)/.1
            loss=F.softplus(-pos).mean()+F.softplus(neg).mean()
            if not torch.isfinite(loss): raise FloatingPointError('Non-finite two-tower loss')
            sparse_optimizer.zero_grad(set_to_none=True); dense_optimizer.zero_grad(set_to_none=True)
            loss.backward(); sparse_optimizer.step(); dense_optimizer.step()
            total+=float(loss.detach())*len(ix); examples+=len(ix)
            if (offset//batch+1)%500==0:
                print('TWO_TOWER_PROGRESS','epoch',epoch+1,'examples',examples,'/',len(order),'mean_loss',total/examples,flush=True)
        record={'epoch':epoch+1,'examples':examples,'loss':total/examples,'seconds':time.perf_counter()-start}
        records.append(record); print('TWO_TOWER_EPOCH',record,flush=True)
    after=weight_hash(model); assert before!=after
    return model,{'seconds':time.perf_counter()-all_start,'epochs':records,'seed':seed,'batch_size':batch,
                  'dimension':dim,'negatives_per_positive':negatives,'positive_rows_per_epoch':len(order),
                  'negative_rule':'uniform product IDs, exact target collisions replaced; other observed products remain eligible implicit negatives',
                  'objective':'balanced positive and sampled-negative logistic loss on normalized dot products / 0.1',
                  'device':'cpu','threads':4,'initial_weights_sha256':before,'final_weights_sha256':after,
                  'user_features':dataset.manifest['user_feature_names'],'item_features':dataset.manifest['item_feature_names']}
