"""Dependencies shared by the routers."""
from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Path, Request

from api.bigquery import MartsClient, Row

# Letters plus the characters real symbols use: ^GSPC, BRK.B, BF-B.
TICKER_PATTERN = r"^[A-Za-z0-9^.\-]{1,12}$"


def get_marts(request: Request) -> MartsClient:
    return request.app.state.marts


Marts = Annotated[MartsClient, Depends(get_marts)]


def load_tickers(marts: MartsClient) -> list[Row]:
    sql = f"""
        select ticker, name, asset_type, sector, sector_etf, is_benchmark, first_date, last_date
        from {marts.settings.marts}.dim_tickers
        order by
            case asset_type when 'stock' then 1 when 'sector_etf' then 2 else 3 end,
            ticker
    """
    return marts.query(sql)


def known_ticker(
    marts: Marts,
    ticker: Annotated[str, Path(pattern=TICKER_PATTERN, description="Ticker symbol.", examples=["NVDA"])],
) -> Row:
    """The ticker's row in dim_tickers, or 404. Matching ignores case."""
    for row in load_tickers(marts):
        if row["ticker"].upper() == ticker.upper():
            return row
    raise HTTPException(status_code=404, detail=f"Unknown ticker {ticker!r}. See /tickers.")


KnownTicker = Annotated[Row, Depends(known_ticker)]


def benchmark_of(rows: list[Row]) -> Row:
    return next(row for row in rows if row["is_benchmark"])
