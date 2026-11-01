{{ config(materialized='table', schema='recommendations') }}
-- Candidate-specific affinities are proportions of observed items, not held-out labels.
with counts as (
    select h.stage, h.user_id, p.aisle_id, p.department_id, sum(h.purchase_count) as n
    from {{ ref('rec_history_product') }} h join {{ ref('stg_products') }} p using (product_id)
    group by h.stage, h.user_id, p.aisle_id, p.department_id
)
select stage, user_id, 'aisle' as kind, aisle_id as category_id, sum(n) as item_count
from counts group by stage, user_id, aisle_id
union all
select stage, user_id, 'department', department_id, sum(n)
from counts group by stage, user_id, department_id
