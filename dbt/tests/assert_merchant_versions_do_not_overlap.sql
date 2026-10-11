-- SCD2 integrity: per merchant exactly one current row, and each version's
-- window ends exactly where the next one starts (no gaps, no overlaps).
with ordered as (

    select
        merchant_id,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by merchant_id order by valid_from) as next_valid_from
    from {{ ref('dim_merchant') }}

),

bad_windows as (

    select merchant_id, 'window mismatch' as problem
    from ordered
    where next_valid_from is not null
      and (valid_to is null or valid_to <> next_valid_from)

),

bad_current as (

    select merchant_id, 'current rows <> 1' as problem
    from {{ ref('dim_merchant') }}
    group by merchant_id
    having sum(case when is_current then 1 else 0 end) <> 1

)

select * from bad_windows
union all
select * from bad_current
