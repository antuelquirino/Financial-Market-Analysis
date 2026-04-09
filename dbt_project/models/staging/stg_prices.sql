{{ config(
    materialized='view'
) }}



select
    `date`,
    ticker,
    high,
    low,
    close,
    volume
from
    raw_finance.market_prices