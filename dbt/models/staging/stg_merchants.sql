-- One row per merchant *version*: a new version starts whenever the source's
-- updated_at changes (e.g. commission renegotiated). Built from the daily full
-- snapshots kept in raw, so history is fully rebuildable from the lake.
with source as (

    select * from {{ source('raw', 'merchants') }}

),

versions as (

    select *
    from source
    qualify row_number() over (
        partition by merchant_id, updated_at
        order by _batch_date desc, _loaded_at desc
    ) = 1

)

select
    merchant_id,
    merchant_name,
    category,
    city,
    cast(commission_rate as decimal(5, 4))   as commission_rate,
    cast(onboarded_date as date)             as onboarded_date,
    cast(updated_at as timestamp)            as updated_at
from versions
