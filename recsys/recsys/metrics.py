"""Macro next-basket metrics: every distinct held-out product is a positive."""
import numpy as np


class BasketMetrics:
    def __init__(self,cutoffs=(10,100)):
        self.cutoffs=tuple(dict.fromkeys(cutoffs)); self.users=0; self.positives=0
        self.recall={k:0. for k in cutoffs}; self.ndcg={k:0. for k in cutoffs}; self.hits={k:0 for k in cutoffs}
    def add(self,ranked,user_ids,labels):
        for row,user in zip(ranked,user_ids):
            truth=np.unique(labels.for_user(int(user)))
            if not len(truth): raise ValueError('An evaluated basket must have supplied labels')
            if len(np.unique(row))!=len(row): raise ValueError('Duplicate recommendation')
            relevance=np.isin(row,truth); self.users+=1; self.positives+=len(truth)
            for k in self.cutoffs:
                hits=int(relevance[:k].sum()); self.hits[k]+=hits; self.recall[k]+=hits/len(truth)
                dcg=float((relevance[:k]/np.log2(np.arange(min(k,len(row)))+2)).sum())
                ideal=float((1/np.log2(np.arange(min(k,len(truth)))+2)).sum())
                self.ndcg[k]+=dcg/ideal
    def result(self):
        assert self.users>0
        result={'users':self.users,'heldout_positive_products':self.positives,
                'definition':'macro mean over users; Recall divides by full distinct held-out basket; NDCG ideal length min(k,basket size)'}
        for k in self.cutoffs:
            result[f'recall@{k}']=self.recall[k]/self.users; result[f'ndcg@{k}']=self.ndcg[k]/self.users
            result[f'micro_recall@{k}']=self.hits[k]/self.positives
        return result
