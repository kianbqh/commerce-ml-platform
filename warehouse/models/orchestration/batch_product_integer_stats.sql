{{ config(schema='orchestration', materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['batch', 'product_id'], on_schema_change='fail', tags=['m4']) }}
select {{ m4_batch_key('order_number') }} as batch,
       cast(product_id as bigint) as product_id,
       cast(count(*) as bigint) as n_lines,
       cast(count(distinct user_id) as bigint) as n_buyers,
       cast(sum(reordered) as bigint) as n_reordered
from {{ ref('fct_order_lines') }}
{% if is_incremental() %}where {{ m4_affected('order_number') }}{% endif %}
group by 1, 2
