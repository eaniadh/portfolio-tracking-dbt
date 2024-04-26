{{
    config(
        materialized = 'ephemeral'
    )
}}

WITH src_data AS (
    SELECT
      SECURITY_CODE AS SECURITY_CODE -- TEXT
    , SECURITY_NAME AS SECURITY_NAME -- TEXT
    , SECTOR AS SECTOR_NAME --TEXT
    , INDUSTRY AS INDUSTRY_NAME -- TEXT
    , COUNTRY AS COUNTRY_CODE -- TEXT
    , EXCHANGE AS EXCHANGE_CODE -- TEXT
    , CURRENT_TIMESTAMP() AS LOAD_TS
    , 'SEED.ABC_BANK_SECURITY_INFO' AS RECORD_SOURCE
    FROM {{ source("seeds", "ABC_BANK_SECURITY_INFO") }}
),
default_record as (
    SELECT
        '-1'      as SECURITY_CODE
        , 'Missing' as SECURITY_NAME
        , 'Missing' as SECTOR_NAME
        , 'Missing' as INDUSTRY_NAME
        , '-1'      as COUNTRY_CODE
        , '-1'      as EXCHANGE_CODE
        , '2024-01-01' as LOAD_TS_UTC
        , 'Missing' as RECORD_SOURCE
),
with_default_record as (
    select * from src_data
    union all
    select * from default_record
),
hashed as (
    SELECT concat_ws('|', SECURITY_CODE) as SECURITY_HKEY,
    concat_ws('|', SECURITY_CODE, SECURITY_NAME, SECTOR_NAME,
                   INDUSTRY_NAME, COUNTRY_CODE, EXCHANGE_CODE 
             ) as SECURITY_HDIFF
    , * EXCLUDE LOAD_TS
    , LOAD_TS AS LOAD_TS_UTC
    FROM with_default_record
)

select * from hashed