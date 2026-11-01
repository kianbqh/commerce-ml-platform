select
    cast(product_id as bigint)    as product_id,
    trim(cast(product_name as varchar)) as product_name,
    cast(aisle_id as bigint)      as aisle_id,
    cast(department_id as bigint) as department_id
from {{ raw_source('products') }}
