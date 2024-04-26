{{ config(materialized='ephemeral') }}

with src_data
as (
    select
        ACCOUNTID AS ACCOUNT_CODE --TEXT
        , SYMBOL AS SECURITY_CODE --TEXT
        , DESCRIPTION AS SECURITY_NAME --TEXT
        , EXCHANGE AS EXCHANGE_CODE --TEXT
        , REPORT_DATE AS REPORT_DATE --DATE
        , QUANTITY AS QUANTITY --NUMBER
        , COST_BASE AS COST_BASE --NUMBER
        , POSITION_VALUE AS POSITION_VALUE --NUMBER
        , CURRENCY AS CURRENCY_CODE --NUMBER
        , 'SOURCE_DATA.ABC_BANK_POSITION' AS RECORD_SOURCE
        from {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),
hashed as (
    select 
         concat_ws('|', ACCOUNT_CODE, SECURITY_CODE) as POSITION_HKEY
        ,concat_ws('|', ACCOUNT_CODE, SECURITY_CODE,
                    SECURITY_NAME, EXCHANGE_CODE, REPORT_DATE,
                    QUANTITY, COST_BASE, POSITION_VALUE, CURRENCY_CODE) as POSITION_HDIFF
        , *
        , '{{ run_started_at }}' as LOAD_TS_UTC
        FROM src_data
)
SELECT * FROM hashed