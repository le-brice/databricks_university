with source as (
    select * from {{ source('raw_finance', 'research_projects') }}
)

select
    project_id,
    title,
    principal_investigator_id,
    department_id,
    funding_body,
    cast(grant_amount as decimal(18,2)) as grant_amount,
    cast(start_date as date)            as start_date,
    cast(end_date as date)              as end_date,
    status,
    research_theme,
    current_timestamp()                 as _loaded_at
from source
