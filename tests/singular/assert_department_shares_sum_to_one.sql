select s from (select sum(share_of_lines) as s from {{ ref('mart_department_kpis') }}) x where abs(s - 1) > 1e-9
