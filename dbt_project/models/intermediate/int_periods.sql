-- Analysis windows per ticker, anchored on the latest benchmark session so every
-- ticker is measured over the same dates. MAX starts at each ticker's first session.

with as_of as (
    select max(date) as as_of_date
    from {{ ref('stg_prices') }}
    where ticker = '{{ var("benchmark_ticker") }}'
),

periods as (
    select * from unnest([
        struct('1Y' as period, 1 as years, 1 as sort_order),
        struct('3Y', 3, 2),
        struct('5Y', 5, 3),
        struct('MAX', cast(null as int64), 4)
    ])
),

ticker_bounds as (
    select ticker, min(date) as first_date
    from {{ ref('stg_prices') }}
    group by ticker
)

select
    p.period,
    p.sort_order as period_sort_order,
    t.ticker,
    greatest(
        t.first_date,
        coalesce(date_sub(a.as_of_date, interval p.years year), t.first_date)
    ) as start_date,
    a.as_of_date as end_date,
    -- False when the ticker's history is shorter than the period.
    p.years is null or t.first_date <= date_sub(a.as_of_date, interval p.years year) as has_full_history
from periods p
cross join as_of a
cross join ticker_bounds t
