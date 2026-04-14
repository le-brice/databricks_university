with source as (
    select * from {{ source('raw_finance', 'grant_payments') }}
)

select
    payment_id,
    project_id,
    cast(payment_date as date)         as payment_date,
    cast(amount as decimal(18,2))      as amount,
    payment_type,
    current_timestamp()                as _loaded_at
from source
