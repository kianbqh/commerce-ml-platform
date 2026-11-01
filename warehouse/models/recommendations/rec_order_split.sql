{{ config(materialized='table', schema='recommendations') }}
-- Only baskets with supplied item labels define the temporal boundary.
with labeled as (
    select distinct order_id from {{ ref('stg_order_products') }}
), numbered as (
    select o.*,
           row_number() over (partition by user_id order by order_number desc, order_id desc) as reverse_rank,
           count(*) over (partition by user_id) as labeled_orders
    from {{ ref('stg_orders') }} o join labeled using (order_id)
)
select *, case when reverse_rank = 1 then 'test'
               when reverse_rank = 2 then 'validation' else 'train' end as split
from numbered where labeled_orders >= 3
