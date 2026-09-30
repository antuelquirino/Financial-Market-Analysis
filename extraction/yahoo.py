"""Download and validate daily prices from Yahoo Finance.

yfinance is an unofficial client: calls can fail, return an empty frame (network
hiccup, rate limit, delisted ticker) or return placeholder rows without prices.
Everything that talks to yfinance goes through `download`, so tests can replace it.
"""

import logging
import time
from datetime import date, timedelta
from typing import Callable

import pandas as pd
import yfinance as yf

from extraction import config

log = logging.getLogger(__name__)

PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close", "volume"]
OUTPUT_COLUMNS = ["ticker", "date", *PRICE_COLUMNS]


class FetchError(Exception):
    """Yahoo Finance did not return usable data for a ticker."""


class ValidationError(FetchError):
    """Yahoo Finance returned data that fails the sanity checks."""


def download(ticker: str, start: date, end: date) -> pd.DataFrame:
    """One raw yfinance call. `end` is exclusive, as in yfinance."""
    return yf.Ticker(ticker).history(
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1d",
        auto_adjust=False,  # keep both close and adj_close
        actions=False,
    )


def fetch_with_retry(
    ticker: str,
    start: date,
    end: date,
    *,
    download: Callable[[str, date, date], pd.DataFrame] = download,
    sleep: Callable[[float], None] | None = None,
    attempts: int = config.MAX_ATTEMPTS,
    backoff: float = config.BACKOFF_SECONDS,
) -> pd.DataFrame:
    """Call `download`, retrying errors and empty responses with exponential backoff.

    The window always includes recent sessions, so an empty frame is never a
    valid answer: it is either transient or the ticker no longer exists.
    """
    sleep = sleep or time.sleep
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            raw = download(ticker, start, end)
            if raw is None or raw.empty:
                raise FetchError("empty response (ticker delisted or data unavailable)")
            return raw
        except Exception as exc:  # yfinance raises many unrelated exception types
            last_error = exc
            log.warning("%s: attempt %d/%d failed: %s", ticker, attempt, attempts, exc)
            if attempt < attempts:
                sleep(backoff * 2 ** (attempt - 1))
    raise FetchError(f"{attempts} attempts failed, last error: {last_error}")


def normalize(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """yfinance frame (DatetimeIndex, 'Adj Close'...) -> one row per session."""
    df = raw.rename(columns=lambda c: str(c).strip().lower().replace(" ", "_"))
    missing = [c for c in PRICE_COLUMNS if c not in df.columns]
    if missing:
        raise ValidationError(f"missing columns: {missing}")

    index = pd.DatetimeIndex(df.index)
    if index.tz is not None:
        # Bars are stamped at midnight exchange time; take the date in New York.
        index = index.tz_convert(config.MARKET_TZ)
    df = df[PRICE_COLUMNS].copy()
    df.insert(0, "date", index.date)
    df.insert(0, "ticker", ticker)
    df["volume"] = pd.to_numeric(df["volume"]).round().astype("Int64")
    return df.reset_index(drop=True)


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Drop placeholder rows and reject data that cannot be a real price series."""
    df = df.dropna(subset=["close", "adj_close"], how="all")
    if df.empty:
        raise ValidationError("no rows with prices")
    problems = []
    if df[["open", "high", "low", "close", "adj_close"]].isna().any().any():
        problems.append("null prices")
    if (df[["open", "high", "low", "close", "adj_close"]] <= 0).any().any():
        problems.append("non-positive prices")
    if (df["high"] < df["low"]).any():
        problems.append("high below low")
    if (df["volume"].dropna() < 0).any():
        problems.append("negative volume")
    if df["date"].duplicated().any():
        problems.append("duplicate dates")
    if problems:
        raise ValidationError(", ".join(problems))
    return df.sort_values("date").reset_index(drop=True)


def get_prices(
    ticker: str,
    start: date,
    last_session: date,
    *,
    fetch: Callable[..., pd.DataFrame] = fetch_with_retry,
) -> pd.DataFrame:
    """Validated prices for [start, last_session]. Sessions after it are dropped
    because their bar may still be intraday."""
    raw = fetch(ticker, start, last_session + timedelta(days=1))
    df = normalize(raw, ticker)
    df = df[df["date"] <= last_session]
    return validate(df)
