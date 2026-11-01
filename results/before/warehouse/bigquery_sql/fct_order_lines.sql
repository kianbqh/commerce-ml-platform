/* Line grain fact. Incremental: append only lines of orders not yet loaded (anti-join works on DuckDB and BigQuery). */
SELECT
  l.order_id,
  l.product_id,
  o.user_id,
  o.order_number,
  o.order_dow,
  o.order_hour_of_day,
  p.aisle_id,
  p.department_id,
  l.add_to_cart_order,
  l.reordered,
  l.eval_set
FROM `warehouse`.`staging`.`stg_order_products` AS l
JOIN `warehouse`.`staging`.`stg_orders` AS o
  ON o.order_id = l.order_id
JOIN `warehouse`.`staging`.`stg_products` AS p
  ON p.product_id = l.product_id
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM `warehouse`.`marts`.`fct_order_lines` AS t
    WHERE
      t.order_id = l.order_id
  )
