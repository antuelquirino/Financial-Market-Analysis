{{ config(materialized='view') }}

-- Legacy interface for the Streamlit app and Tableau, same columns as before.
-- Remove once the Next.js frontend replaces Streamlit (Phase 3).

select
    d.date,
    d.ticker,
    t.name as company_name,
    t.sector,
    t.industry,
    d.adj_close as price,
    d.daily_return,
    d.cum_return,
    d.rolling_volatility_1y as volatility,
    d.drawdown,
    d.rolling_sharpe_1y as sharpe_ratio,
    m.cagr
from {{ ref('mart_daily_metrics') }} d
join {{ ref('stg_tickers') }} t using (ticker)
left join {{ ref('mart_period_metrics') }} m
    on m.ticker = d.ticker and m.period = 'MAX'
