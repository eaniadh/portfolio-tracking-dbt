{% test not_empty(model, column_name) %}
    with validation_error as (
        select {{ column_name }}
        from {{ model }}
        where len({{ column_name }}) = 0
    )
    SELECT * FROM validation_errors
{% endtest %}