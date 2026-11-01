with stats as (
    select product_id,
           count(*)                      as n_lines,
           count(distinct user_id)       as n_buyers,
           avg(cast(reordered as double)) as reorder_rate,
           avg(cast(add_to_cart_order as double)) as avg_cart_position
    from {{ ref('fct_order_lines') }}
    group by product_id
)
select
    p.product_id, p.product_name, p.aisle_id, a.aisle_name, p.department_id, d.department_name,
    coalesce(s.n_lines, 0) as n_lines, coalesce(s.n_buyers, 0) as n_buyers, s.reorder_rate, s.avg_cart_position,
    dense_rank() over (order by coalesce(s.n_lines, 0) desc) as popularity_rank
from {{ ref('stg_products') }} p
left join {{ ref('stg_aisles') }} a on a.aisle_id = p.aisle_id
left join {{ ref('stg_departments') }} d on d.department_id = p.department_id
left join stats s on s.product_id = p.product_id
