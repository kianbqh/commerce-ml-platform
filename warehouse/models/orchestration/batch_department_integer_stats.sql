{{ config(schema='orchestration', materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['batch', 'department_id'], on_schema_change='fail', tags=['m4']) }}
select {{ m4_batch_key('order_number') }} as batch,
       cast(department_id as bigint) as department_id,
       cast(count(*) as bigint) as n_lines,
       cast(count(distinct order_id) as bigint) as n_orders,
       cast(count(distinct user_id) as bigint) as n_users,
       cast(sum(reordered) as bigint) as n_reordered,
       cast(sum(add_to_cart_order) as bigint) as sum_cart_position
from {{ ref('fct_order_lines') }}
{% if is_incremental() %}where {{ m4_affected('order_number') }}{% endif %}
group by 1, 2
