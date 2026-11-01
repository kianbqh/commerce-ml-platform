-- Sum of basket sizes in fct_orders must equal the number of lines in fct_order_lines.
select o.total, l.total
from (select sum(basket_size) as total from {{ ref('fct_orders') }}) o
cross join (select count(*) as total from {{ ref('fct_order_lines') }}) l
where o.total <> l.total
