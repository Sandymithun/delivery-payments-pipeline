{#
  Small, dependency-free generic tests (no dbt_utils needed, so the project
  builds offline and in CI without `dbt deps`).
  Each test returns the failing rows; zero rows = pass.
#}

{% test non_negative(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}


{% test in_range(model, column_name, min_value, max_value) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ min_value }}
       or {{ column_name }} > {{ max_value }}
{% endtest %}


{% test is_clean_email(model, column_name) %}
    -- lower-case, no surrounding whitespace, exactly one @ followed by a dot
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} <> lower(trim({{ column_name }}))
       or {{ column_name }} not like '%_@_%._%'
       or {{ column_name }} like '%@%@%'
{% endtest %}


{% test unique_combination(model, columns) %}
    select {{ columns | join(', ') }}, count(*) as n
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}
