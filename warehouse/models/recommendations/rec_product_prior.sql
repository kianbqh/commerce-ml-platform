{{ config(materialized='table', schema='recommendations') }}
-- Global priors stay fixed at the training boundary for validation AND test.
with counts as (
    select product_id, count(*) as purchase_count, count(distinct user_id) as buyers,
           sum(reordered) as reorder_count
    from {{ ref('rec_lines') }} where split = 'train' group by product_id
)
select p.*, coalesce(c.purchase_count, 0) as purchase_count, coalesce(c.buyers, 0) as buyers,
       coalesce(c.reorder_count, 0) as reorder_count
from {{ ref('stg_products') }} p left join counts c using (product_id)
