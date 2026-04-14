with source as (
    select * from {{ source('raw_hr', 'departments') }}
)

select
    department_id,
    department_name,
    faculty_name,
    current_timestamp() as _loaded_at
from source
