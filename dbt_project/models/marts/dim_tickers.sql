{{ config(materialized='table') }}

-- One row per ticker: what the dashboard's selector lists.

with coverage as (
    select ticker, min(date) as first_date, max(date) as last_date
    from {{ ref('stg_prices') }}
    group by ticker
)

select
    t.ticker,
    t.name,
    t.asset_type,
    t.sector,
    t.industry,
    t.sector_etf,
    t.ticker = '{{ var("benchmark_ticker") }}' as is_benchmark,
    c.first_date,
    c.last_date
from {{ ref('stg_tickers') }} t
left join coverage c using (ticker)
