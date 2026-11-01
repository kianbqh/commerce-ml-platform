select cast(department_id as bigint) as department_id, cast(department as varchar) as department_name
from {{ raw_source('departments') }}
