{{ config(
    materialized='view'
) }}

-- Staging table for market prices
-- Cleans column names and prepares data for intermediate/marts layer

select
    `date`,
    ticker,
    high,
    low,
    close,
    volume
from
    raw_finance.market_prices