{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key='department_id', on_schema_change='fail') }}
-- Cumulative scan with keyed replacement, not a partition-local speed claim.
with totals as (select count(*) as all_lines from {{ ref('fct_order_lines') }})
select
    d.department_id, d.department_name,
    count(*) as n_lines,
    count(distinct l.order_id) as n_orders,
    count(distinct l.user_id) as n_users,
    avg(cast(l.reordered as double)) as reorder_rate,
    avg(cast(l.add_to_cart_order as double)) as avg_cart_position,
    count(*) * 1.0 / max(t.all_lines) as share_of_lines
from {{ ref('fct_order_lines') }} l
join {{ ref('stg_departments') }} d on d.department_id = l.department_id
cross join totals t
group by d.department_id, d.department_name
