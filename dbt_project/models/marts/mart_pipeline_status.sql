{{ config(materialized='table') }}

-- A single row saying how fresh the data is, for the dashboard's
-- "Data updated" indicator and for monitoring.

with runs as (
    select * from {{ ref('stg_pipeline_runs') }}
),

last_run as (
    select started_at, finished_at, status
    from runs
    qualify row_number() over (order by started_at desc) = 1
),

latest_by_ticker as (
    select ticker, max(date) as last_date
    from {{ ref('stg_prices') }}
    group by ticker
),

market as (
    -- The latest session of the benchmark is "the latest market day".
    select max(date) as last_market_date
    from {{ ref('stg_prices') }}
    where ticker = '{{ var("benchmark_ticker") }}'
)

select
    m.last_market_date,
    (select max(finished_at) from runs where status = 'success') as last_successful_run_at,
    lr.finished_at as last_run_at,
    lr.status as last_run_status,
    (select count(*) from {{ ref('stg_tickers') }}) as tickers_expected,
    (select countif(last_date = m.last_market_date) from latest_by_ticker) as tickers_current,
    current_timestamp() as built_at
from market m
cross join last_run lr
