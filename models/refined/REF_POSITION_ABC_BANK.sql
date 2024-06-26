with current_from_snapshot as (
    {{
        current_from_snapshot(
            snsh_ref = ref("SNSH_ABC_BANK_POSITION"),
            output_load_ts = false
        )
    }}
    
)

select *
    , POSITION_VALUE - COST_BASE as UNREALIZED_PROFIT
    , ROUND(UNREALIZED_PROFIT / COST_BASE, 2) as UNREALIZED_PROFIT_PCT
from current_from_snapshot