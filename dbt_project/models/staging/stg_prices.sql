{{ config(
    materialized='view'
) }}

with source_data as (

    select
        date,
        ticker,
        open,
        high,
        low,
        close,
        volume
    from raw_finance.market_prices

),

deduplicated as (

    select *,
        row_number() over (
            partition by ticker, date
            order by date desc
        ) as rn
    from source_data

)

select
    date,
    ticker,
    open,
    high,
    low,
    close,
    volume
from deduplicated
where rn = 1