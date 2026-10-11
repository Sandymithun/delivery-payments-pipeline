{{ config(materialized='table') }}

-- Type 2 (slowly changing) merchant dimension: a new row each time a merchant's
-- attributes change, with a validity window. Facts join on the version that was
-- valid at order time, so historical commission is reported correctly.
with versions as (

    select * from {{ ref('stg_merchants') }}

),

windowed as (

    select
        *,
        updated_at                                                         as valid_from,
        lead(updated_at) over (partition by merchant_id order by updated_at) as valid_to
    from versions

)

select
    {{ surrogate_key(['merchant_id', 'valid_from']) }}   as merchant_key,
    merchant_id,
    merchant_name,
    category,
    city,
    commission_rate,
    onboarded_date,
    valid_from,
    valid_to,
    valid_to is null                                     as is_current
from windowed
