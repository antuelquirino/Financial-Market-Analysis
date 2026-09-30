-- Per ticker and session: return, cumulative return, drawdown and rolling risk.
-- All from adj_close, so dividends and splits do not show up as price moves.

with returns as (
    select
        ticker,
        date,
        adj_close,
        adj_close / lag(adj_close) over (partition by ticker order by date) - 1 as daily_return
    from {{ ref('stg_prices') }}
)

select
    ticker,
    date,
    adj_close,
    daily_return,

    -- Growth since the first loaded session.
    adj_close / first_value(adj_close) over (partition by ticker order by date) - 1 as cum_return,

    -- Distance from the running peak of the price (equivalently, of wealth).
    adj_close / max(adj_close) over (
        partition by ticker order by date rows between unbounded preceding and current row
    ) - 1 as drawdown,

    -- Rolling metrics stay null until the window is full, so early values are
    -- not computed from a handful of sessions.
    if(
        count(daily_return) over short_window = {{ var('short_window_days') }},
        {{ annualize_volatility('stddev_samp(daily_return) over short_window') }},
        null
    ) as rolling_volatility_1m,

    if(
        count(daily_return) over long_window = {{ var('long_window_days') }},
        {{ annualize_volatility('stddev_samp(daily_return) over long_window') }},
        null
    ) as rolling_volatility_1y,

    if(
        count(daily_return) over long_window = {{ var('long_window_days') }},
        {{ annualized_sharpe('avg(daily_return) over long_window', 'stddev_samp(daily_return) over long_window') }},
        null
    ) as rolling_sharpe_1y

from returns
window
    short_window as (
        partition by ticker order by date
        rows between {{ var('short_window_days') - 1 }} preceding and current row
    ),
    long_window as (
        partition by ticker order by date
        rows between {{ var('long_window_days') - 1 }} preceding and current row
    )
