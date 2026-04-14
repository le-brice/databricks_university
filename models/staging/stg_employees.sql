with source as (
    select * from {{ source('raw_hr', 'employees') }}
)

select
    employee_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    department_id,
    role,
    cast(hire_date as date)            as hire_date,
    cast(termination_date as date)     as termination_date,
    employment_type,
    current_timestamp()                as _loaded_at
from source
