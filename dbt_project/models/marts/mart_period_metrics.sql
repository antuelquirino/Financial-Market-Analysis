{{ config(cluster_by=['period', 'ticker']) }}

-- Headline metrics per ticker and period: the numbers on the dashboard cards
-- and the sector comparison. Daily returns exclude the first session of the
-- period, whose return belongs to the day before the period starts.

with series as (
    select * from {{ ref('mart_period_series') }}
),

aggregated as (
    select
        period,
        ticker,
        min(date) as start_date,
        max(date) as end_date,
        count(*) as sessions,
        array_agg(cum_return order by date desc limit 1)[offset(0)] as total_return,
        array_agg(benchmark_cum_return order by date desc limit 1)[offset(0)] as benchmark_total_return,
        min(drawdown) as max_drawdown,
        avg(daily_return) as mean_daily_return,
        stddev_samp(daily_return) as daily_stddev,
        safe_divide(
            covar_samp(daily_return, benchmark_daily_return),
            var_samp(benchmark_daily_return)
        ) as beta
    from series
    group by period, ticker
)

select
    a.period,
    p.period_sort_order,
    a.ticker,
    t.name,
    t.asset_type,
    t.sector,
    t.sector_etf,
    a.start_date,
    a.end_date,
    a.sessions,
    p.has_full_history,
    a.total_return,
    -- Compound annual growth rate over the calendar length of the period.
    pow(1 + a.total_return, 365.25 / nullif(date_diff(a.end_date, a.start_date, day), 0)) - 1 as cagr,
    {{ annualize_volatility('a.daily_stddev') }} as volatility,
    a.max_drawdown,
    {{ annualized_sharpe('a.mean_daily_return', 'a.daily_stddev') }} as sharpe_ratio,
    a.beta,
    a.benchmark_total_return,
    a.total_return - a.benchmark_total_return as excess_return
from aggregated a
join {{ ref('int_periods') }} p using (period, ticker)
join {{ ref('stg_tickers') }} t using (ticker)
