"""Test doubles: a fake yfinance and an in-memory warehouse. No network, no BigQuery."""

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from extraction import yahoo
from extraction.warehouse import TickerState

# 23:00 UTC on Wednesday 2026-09-30 is 19:00 in New York: that day's bar is final.
NOW = datetime(2026, 9, 30, 23, 0, tzinfo=timezone.utc)
LAST_SESSION = date(2026, 9, 30)


def yahoo_frame(start: date, end: date, factor: float = 1.0) -> pd.DataFrame:
    """What yfinance's Ticker.history returns: weekdays in [start, end), a
    tz-aware DatetimeIndex and title-case columns. Prices depend only on the
    date, so overlapping calls agree unless `factor` (a re-basing) changes."""
    days = pd.bdate_range(start, end - timedelta(days=1), tz="America/New_York", name="Date")
    base = pd.Series(100.0 + (days - pd.Timestamp("2021-01-01", tz="America/New_York")).days * 0.1, index=days)
    return pd.DataFrame(
        {
            "Open": base,
            "High": base * 1.01,
            "Low": base * 0.99,
            "Close": base,
            "Adj Close": base * factor,
            "Volume": 1_000_000,
        },
        index=days,
    )


class FakeYFinance:
    """Stands in for `yf.Ticker`. `behaviors[ticker]` is a list consumed one call
    at a time: "ok", "empty", an Exception, or a callable(start, end) -> frame.
    The last behavior repeats once the list runs out."""

    def __init__(self):
        self.behaviors: dict[str, list] = {}
        self.calls: list[tuple[str, dict]] = []
        self.factor: dict[str, float] = {}

    def __call__(self, ticker: str):
        fake = self

        class _Ticker:
            def history(self, **kwargs):
                fake.calls.append((ticker, kwargs))
                queue = fake.behaviors.get(ticker, ["ok"])
                behavior = queue.pop(0) if len(queue) > 1 else queue[0]
                start = date.fromisoformat(kwargs["start"])
                end = date.fromisoformat(kwargs["end"])
                if isinstance(behavior, Exception):
                    raise behavior
                if behavior == "empty":
                    return pd.DataFrame()
                if callable(behavior):
                    return behavior(start, end)
                return yahoo_frame(start, end, fake.factor.get(ticker, 1.0))

        return _Ticker()

    def calls_for(self, ticker: str) -> int:
        return sum(1 for t, _ in self.calls if t == ticker)


class FakeWarehouse:
    """In-memory warehouse with the same MERGE semantics as BigQuery: one row per (ticker, date)."""

    def __init__(self):
        self.rows: dict[tuple[str, date], dict] = {}
        self.runs: list[dict] = []

    def ensure_tables(self) -> None:
        pass

    def loaded_state(self, today: date, lookback_days: int = 10) -> dict[str, TickerState]:
        state: dict[str, TickerState] = {}
        for ticker in {t for t, _ in self.rows}:
            dates = sorted(d for t, d in self.rows if t == ticker)
            last = dates[-1]
            recent = {
                d: self.rows[(ticker, d)]["adj_close"]
                for d in dates
                if d >= last - timedelta(days=lookback_days)
            }
            state[ticker] = TickerState(last_date=last, recent_adj_close=recent)
        return state

    def upsert_prices(self, df: pd.DataFrame) -> int:
        for row in df.to_dict("records"):
            self.rows[(row["ticker"], row["date"])] = row
        return len(df)

    def log_run(self, record: dict) -> None:
        self.runs.append(record)

    def count(self, ticker: str | None = None) -> int:
        return sum(1 for t, _ in self.rows if ticker is None or t == ticker)


@pytest.fixture
def fake_yf(monkeypatch):
    fake = FakeYFinance()
    monkeypatch.setattr(yahoo.yf, "Ticker", fake)
    monkeypatch.setattr(yahoo.time, "sleep", lambda seconds: None)
    return fake


@pytest.fixture
def warehouse():
    return FakeWarehouse()
