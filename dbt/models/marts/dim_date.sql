{{ config(materialized='table') }}

with days as (

    {{ days_between(var('date_spine_start', '2026-01-01'), var('date_spine_end', '2027-12-31')) }}

)

select
    {{ date_key('date_day') }}                         as date_key,
    date_day,
    year(date_day)                                     as year,
    quarter(date_day)                                  as quarter,
    month(date_day)                                    as month,
    day(date_day)                                      as day_of_month,
    {{ iso_day_of_week('date_day') }}                             as iso_day_of_week,
    {{ iso_day_of_week('date_day') }} in (6, 7)                   as is_weekend,
    {{ dbt.date_trunc('week', 'date_day') }}           as week_start,
    {{ dbt.date_trunc('month', 'date_day') }}          as month_start
from days
