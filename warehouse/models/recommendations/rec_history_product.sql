{{ config(materialized='table', schema='recommendations') }}
-- Test history adds the user's completed validation basket, never any test line.
with aggregated as (
    select user_id, product_id,
           count(*) filter (where split = 'train') as train_count,
           max(order_number) filter (where split = 'train') as train_last,
           sum(reordered) filter (where split = 'train') as train_reorders,
           count(*) as test_count, max(order_number) as test_last, sum(reordered) as test_reorders
    from {{ ref('rec_lines') }} where split != 'test' group by user_id, product_id
)
select 'validation' as stage, user_id, product_id, train_count as purchase_count,
       train_last as last_order_number, train_reorders as reorder_count
from aggregated where train_count > 0
union all
select 'test', user_id, product_id, test_count, test_last, test_reorders from aggregated
