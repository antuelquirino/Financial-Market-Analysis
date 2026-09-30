select
    ticker,
    name,
    asset_type,
    sector,
    industry,
    sector_etf
from {{ ref('tickers') }}
