-- Every staged line must survive the joins into the fact table (no silent drops from bad keys).
select a.n, b.n
from (select count(*) as n from {{ ref('stg_order_products') }}) a
cross join (select count(*) as n from {{ ref('fct_order_lines') }}) b
where a.n <> b.n
