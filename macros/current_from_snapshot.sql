{% macro current_from_snapshot(snsh_ref, output_load_ts=true) %}
    select *
    EXCLUDE (DBT_SCD_ID, DBT_VALID_FROM, DBT_VALID_TO, DBT_UPDATED_AT)
        {% if output_load_ts %}
           , DBT_UPDATED_AT AS SNSH_LOAD_TS_UTC
        {% endif %}
    FROM {{ snsh_ref }}
    where DBT_VALID_TO is null
{% endmacro %}