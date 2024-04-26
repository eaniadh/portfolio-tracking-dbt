with current_from_snapshot as (
    select *
        EXCLUDE (
            DBT_SCD_ID, DBT_UPDATED_AT,
            DBT_VALID_FROM, DBT_VALID_TO
        )
    from {{ ref("SNSH_ABC_BANK_SECURITY_INFO") }}
    where DBT_VALID_TO is null
)

select * from current_from_snapshot