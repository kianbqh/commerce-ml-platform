{{ config(tags=['m4_output'], meta={'dagster': {'ref': {'name': 'fct_order_lines'}}}) }}
with source_counts as (
    select {{ m4_batch_key('o.order_number') }} batch, count(*) n
    from {{ ref('stg_order_products') }} l join {{ ref('stg_orders') }} o using(order_id) group by 1
), fact_counts as (
    select {{ m4_batch_key('order_number') }} batch, count(*) n
    from {{ ref('fct_order_lines') }} group by 1
)
select coalesce(s.batch,f.batch) batch, s.n source_n, f.n fact_n
from source_counts s full join fact_counts f on s.batch=f.batch
where coalesce(s.n,0) <> coalesce(f.n,0)
