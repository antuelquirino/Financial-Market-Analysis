-- Every ticker in the seed has a session within 5 days of the newest one.
-- Warns (does not fail the build) so one ticker cannot block the others.
{{ config(severity='warn') }}

with latest as (
    select ticker, max(date) as last_date
    from {{ ref('stg_prices') }}
    group by ticker
)

select t.ticker, l.last_date
from {{ ref('stg_tickers') }} t
left join latest l using (ticker)
where l.last_date is null
   or l.last_date < date_sub((select max(date) from {{ ref('stg_prices') }}), interval 5 day)
