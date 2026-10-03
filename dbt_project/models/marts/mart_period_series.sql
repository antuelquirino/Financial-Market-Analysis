{{ config(
    partition_by={'field': 'date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['period', 'ticker']
) }}

-- Each ticker's series re-based to the start of every period, next to the
-- benchmark re-based to the same date: what a "growth of $1" chart plots.

with series as (
    select
        p.period,
        d.ticker,
        d.date,
        d.adj_close,
        d.benchmark_adj_close
    from {{ ref('int_periods') }} p
    join {{ ref('mart_daily_metrics') }} d
        on d.ticker = p.ticker
        and d.date between p.start_date and p.end_date
)

select
    period,
    ticker,
    date,
    adj_close / lag(adj_close) over by_date - 1 as daily_return,
    benchmark_adj_close / lag(benchmark_adj_close) over by_date - 1 as benchmark_daily_return,
    adj_close / first_value(adj_close) over by_date - 1 as cum_return,
    benchmark_adj_close / first_value(benchmark_adj_close ignore nulls) over by_date - 1 as benchmark_cum_return,
    -- Drawdown within the period: the peak is the highest price since the period started.
    adj_close / max(adj_close) over (
        partition by period, ticker order by date rows between unbounded preceding and current row
    ) - 1 as drawdown
from series
window by_date as (partition by period, ticker order by date)
