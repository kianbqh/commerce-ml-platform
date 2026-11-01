-- A product cannot be a "reorder" on a user's very first order.
select * from {{ ref('fct_order_lines') }} where order_number = 1 and reordered = 1
