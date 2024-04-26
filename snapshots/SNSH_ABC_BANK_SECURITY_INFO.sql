{% snapshot SNSH_ABC_BANK_SECURITY_INFO %}

    {{
        config(
            unique_key = 'SECURITY_HKEY',
            strategy = 'check',
            check_cols = ['SECURITY_HDIFF'],
            target_schema = 'snapshot'
        )
    }}

    select * from {{ ref("STG_ABC_BANK_SECURITY_INFO") }}

{% endsnapshot %}