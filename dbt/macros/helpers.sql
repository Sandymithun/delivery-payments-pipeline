{# Deterministic surrogate key: md5 of the '|'-joined, null-safe natural key parts.
   md5() returns the same hex string on DuckDB and Snowflake. #}
{% macro surrogate_key(parts) -%}
    md5(
        {%- for p in parts %}
        coalesce(cast({{ p }} as varchar), '_null_'){% if not loop.last %} || '|' ||{% endif %}
        {%- endfor %}
    )
{%- endmacro %}


{# Integer YYYYMMDD key for joining facts to dim_date. #}
{% macro date_key(date_expr) -%}
    (year({{ date_expr }}) * 10000 + month({{ date_expr }}) * 100 + day({{ date_expr }}))
{%- endmacro %}


{# One row per day between two dates (inclusive), column `date_day`.
   Warehouse-specific, so it uses adapter.dispatch. #}
{% macro days_between(start_date, end_date) -%}
    {{ return(adapter.dispatch('days_between')(start_date, end_date)) }}
{%- endmacro %}

{% macro default__days_between(start_date, end_date) -%}
    select cast(gs as date) as date_day
    from generate_series(cast('{{ start_date }}' as date), cast('{{ end_date }}' as date), interval 1 day) as t(gs)
{%- endmacro %}

{% macro snowflake__days_between(start_date, end_date) -%}
    {#- GENERATOR needs a literal rowcount, so compute it here; seq4() can have gaps, row_number() can't -#}
    {%- set n_days = (modules.datetime.date.fromisoformat(end_date) - modules.datetime.date.fromisoformat(start_date)).days + 1 -%}
    select dateadd(day, row_number() over (order by seq4()) - 1, to_date('{{ start_date }}')) as date_day
    from table(generator(rowcount => {{ n_days }}))
{%- endmacro %}


{# ISO day of week, Monday = 1 ... Sunday = 7 #}
{% macro iso_day_of_week(date_expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(date_expr)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(date_expr) -%}isodow({{ date_expr }}){%- endmacro %}

{% macro snowflake__iso_day_of_week(date_expr) -%}dayofweekiso({{ date_expr }}){%- endmacro %}
