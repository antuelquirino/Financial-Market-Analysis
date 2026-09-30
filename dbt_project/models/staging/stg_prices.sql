select
    ticker,
    date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    loaded_at
from {{ source('raw_finance', 'daily_prices') }}
