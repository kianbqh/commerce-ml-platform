WITH totals AS (
  SELECT
    COUNT(*) AS all_lines
  FROM `warehouse`.`marts`.`fct_order_lines`
)
SELECT
  d.department_id,
  d.department_name,
  COUNT(*) AS n_lines,
  COUNT(DISTINCT l.order_id) AS n_orders,
  COUNT(DISTINCT l.user_id) AS n_users,
  AVG(CAST(l.reordered AS FLOAT64)) AS reorder_rate,
  AVG(CAST(l.add_to_cart_order AS FLOAT64)) AS avg_cart_position,
  COUNT(*) * 1.0 / MAX(t.all_lines) AS share_of_lines
FROM `warehouse`.`marts`.`fct_order_lines` AS l
JOIN `warehouse`.`staging`.`stg_departments` AS d
  ON d.department_id = l.department_id
CROSS JOIN totals AS t
GROUP BY
  d.department_id,
  d.department_name
