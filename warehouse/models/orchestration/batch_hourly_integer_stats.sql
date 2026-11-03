{{ config(schema='orchestration', materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['batch', 'order_dow', 'order_hour_of_day'], on_schema_change='fail', tags=['m4']) }}
select {{ m4_batch_key('order_number') }} as batch,
       cast(order_dow as bigint) as order_dow,
       cast(order_hour_of_day as bigint) as order_hour_of_day,
       cast(count(distinct order_id) as bigint) as n_orders,
       cast(count(*) as bigint) as n_lines,
       cast(sum(reordered) as bigint) as n_reordered
from {{ ref('fct_order_lines') }}
{% if is_incremental() %}where {{ m4_affected('order_number') }}{% endif %}
group by 1, 2, 3
