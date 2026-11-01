WITH stats AS (
  SELECT
    product_id,
    COUNT(*) AS n_lines,
    COUNT(DISTINCT user_id) AS n_buyers,
    AVG(CAST(reordered AS FLOAT64)) AS reorder_rate,
    AVG(CAST(add_to_cart_order AS FLOAT64)) AS avg_cart_position
  FROM `warehouse`.`marts`.`fct_order_lines`
  GROUP BY
    product_id
)
SELECT
  p.product_id,
  p.product_name,
  p.aisle_id,
  a.aisle_name,
  p.department_id,
  d.department_name,
  COALESCE(s.n_lines, 0) AS n_lines,
  COALESCE(s.n_buyers, 0) AS n_buyers,
  s.reorder_rate,
  s.avg_cart_position,
  DENSE_RANK() OVER (ORDER BY COALESCE(s.n_lines, 0) DESC) AS popularity_rank
FROM `warehouse`.`staging`.`stg_products` AS p
LEFT JOIN `warehouse`.`staging`.`stg_aisles` AS a
  ON a.aisle_id = p.aisle_id
LEFT JOIN `warehouse`.`staging`.`stg_departments` AS d
  ON d.department_id = p.department_id
LEFT JOIN stats AS s
  ON s.product_id = p.product_id
