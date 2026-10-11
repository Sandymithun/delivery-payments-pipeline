-- Latest snapshot of each customer, typed and cleaned.
with source as (

    select * from {{ source('raw', 'customers') }}

),

latest as (

    select *
    from source
    qualify row_number() over (partition by customer_id order by _batch_date desc, _loaded_at desc) = 1

)

select
    customer_id,
    trim(first_name)                         as first_name,
    trim(last_name)                          as last_name,
    lower(trim(email))                       as email,
    city,
    cast(signup_date as date)                as signup_date,
    _batch_date                              as snapshot_date
from latest
