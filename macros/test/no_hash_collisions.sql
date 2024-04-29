{% test no_hash_collisions(model,column_name,hashed_fields) %}

    with all_tuples as (
        SELECT {{column_name}} as HASH, {{ hashed_fields }}
        from {{ model }}
    ),
    validation_error as (
        SELECT HASH, count(*)
        FROM all_tuples
        GROUP BY HASH
        HAVING count(*) > 1
    )
    SELECT * FROM validation_error
{% endtest %}

{%  macro as_sql_list(hashed_fields_list)  %}
    {{ hashed_fields_list|join(', ') }}
{% endmacro %}