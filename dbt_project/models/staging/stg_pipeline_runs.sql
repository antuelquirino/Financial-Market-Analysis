select
    run_id,
    started_at,
    finished_at,
    status,
    trigger,
    array_length(tickers_ok) as tickers_ok_count,
    array_length(tickers_failed) as tickers_failed_count,
    rows_upserted,
    max_market_date
from {{ source('raw_finance', 'pipeline_runs') }}
