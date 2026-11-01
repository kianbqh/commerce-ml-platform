-- Market-basket association rules among the top-N products: support, confidence and lift.
{% set top_n = 1000 %}
with top_products as (
    select product_id from {{ ref('dim_products') }} where popularity_rank <= {{ top_n }}
),
lines as (
    select l.order_id, l.product_id
    from {{ ref('fct_order_lines') }} l join top_products t on t.product_id = l.product_id
),
n_orders as (select count(*) as n from {{ ref('fct_orders') }}),
item_cnt as (select product_id, count(*) as cnt from lines group by product_id),
pairs as (
    select a.product_id as product_a, b.product_id as product_b, count(*) as n_together
    from lines a join lines b on a.order_id = b.order_id and a.product_id < b.product_id
    group by a.product_id, b.product_id
    having count(*) >= 200
)
select
    p.product_a, pa.product_name as product_a_name, p.product_b, pb.product_name as product_b_name,
    p.n_together,
    p.n_together * 1.0 / n.n                                   as support,
    p.n_together * 1.0 / ca.cnt                                as confidence_a_to_b,
    p.n_together * 1.0 * n.n / (ca.cnt * cb.cnt)               as lift
from pairs p
cross join n_orders n
join item_cnt ca on ca.product_id = p.product_a
join item_cnt cb on cb.product_id = p.product_b
join {{ ref('dim_products') }} pa on pa.product_id = p.product_a
join {{ ref('dim_products') }} pb on pb.product_id = p.product_b
