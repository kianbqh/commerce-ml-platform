select cast(aisle_id as bigint) as aisle_id, cast(aisle as varchar) as aisle_name
from {{ raw_source('aisles') }}
