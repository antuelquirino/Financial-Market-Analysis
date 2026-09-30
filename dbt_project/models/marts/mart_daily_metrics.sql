{{ config(
    partition_by={'field': 'date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['ticker']
) }}

-- One row per ticker and session, with the benchmark on the same row.

with metrics as (
    select * from {{ ref('int_price_metrics') }}
),

benchmark as (
    select date, adj_close, daily_return
    from metrics
    where ticker = '{{ var("benchmark_ticker") }}'
)

select
    m.ticker,
    m.date,
    t.name,
    t.asset_type,
    t.sector,
    m.adj_close,
    m.daily_return,
    m.cum_return,
    m.drawdown,
    m.rolling_volatility_1m,
    m.rolling_volatility_1y,
    m.rolling_sharpe_1y,
    b.adj_close as benchmark_adj_close,
    b.daily_return as benchmark_daily_return
from metrics m
join {{ ref('stg_tickers') }} t using (ticker)
left join benchmark b using (date)
