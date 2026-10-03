"""API tests with a fake marts client: no BigQuery, no network."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient
from google.api_core.exceptions import InternalServerError

from api.bigquery import MartsClient
from api.distribution import return_distribution
from api.main import create_app
from api.settings import Settings

FRONTEND = "http://localhost:3000"

TICKERS = [
    {"ticker": "AAA", "name": "Alpha Inc.", "asset_type": "stock", "sector": "Information Technology",
     "sector_etf": "XLK", "is_benchmark": False, "first_date": date(2021, 1, 4), "last_date": date(2026, 10, 2)},
    {"ticker": "XLE", "name": "Energy SPDR", "asset_type": "sector_etf", "sector": "Energy",
     "sector_etf": "XLE", "is_benchmark": False, "first_date": date(2021, 1, 4), "last_date": date(2026, 10, 2)},
    {"ticker": "XLK", "name": "Technology SPDR", "asset_type": "sector_etf", "sector": "Information Technology",
     "sector_etf": "XLK", "is_benchmark": False, "first_date": date(2021, 1, 4), "last_date": date(2026, 10, 2)},
    {"ticker": "^GSPC", "name": "S&P 500 Index", "asset_type": "benchmark", "sector": None,
     "sector_etf": None, "is_benchmark": False, "first_date": date(2021, 1, 4), "last_date": date(2026, 10, 2)},
    {"ticker": "SPY", "name": "SPDR S&P 500 ETF Trust", "asset_type": "benchmark", "sector": None,
     "sector_etf": None, "is_benchmark": True, "first_date": date(2021, 1, 4), "last_date": date(2026, 10, 2)},
]


def metrics(ticker: str, total_return: float, **overrides) -> dict:
    row = {
        "ticker": ticker, "period": "1Y", "name": next(t["name"] for t in TICKERS if t["ticker"] == ticker),
        "sector": next(t["sector"] for t in TICKERS if t["ticker"] == ticker),
        "asset_type": next(t["asset_type"] for t in TICKERS if t["ticker"] == ticker),
        "start_date": date(2025, 10, 2), "end_date": date(2026, 10, 2), "sessions": 3,
        "has_full_history": True, "total_return": total_return, "cagr": total_return,
        "volatility": 0.2, "max_drawdown": -0.1, "sharpe_ratio": 1.1, "beta": 1.0,
        "benchmark_total_return": 0.1, "excess_return": total_return - 0.1,
    }
    return {**row, **overrides}


PERIOD_METRICS = [metrics("AAA", 0.25), metrics("XLE", 0.40), metrics("XLK", 0.05), metrics("SPY", 0.10)]

PERIOD_SERIES = [
    {"ticker": "AAA", "period": "1Y", "date": date(2025, 10, 2), "cum_return": 0.0,
     "benchmark_cum_return": 0.0, "drawdown": 0.0, "daily_return": None},
    {"ticker": "AAA", "period": "1Y", "date": date(2025, 10, 3), "cum_return": 0.3,
     "benchmark_cum_return": 0.05, "drawdown": 0.0, "daily_return": 0.3},
    {"ticker": "AAA", "period": "1Y", "date": date(2026, 10, 2), "cum_return": 0.25,
     "benchmark_cum_return": 0.1, "drawdown": -0.0385, "daily_return": -0.0385},
]

DAILY_METRICS = [
    {"ticker": "AAA", "date": date(2025, 10, 1), "volatility_1m": 0.30, "volatility_1y": 0.25},
    {"ticker": "AAA", "date": date(2025, 10, 2), "volatility_1m": 0.31, "volatility_1y": 0.25},
    {"ticker": "AAA", "date": date(2026, 10, 2), "volatility_1m": 0.28, "volatility_1y": 0.26},
]

STATUS = {
    "last_market_date": date(2026, 10, 2),
    "last_successful_run_at": datetime(2026, 10, 2, 22, 41, tzinfo=timezone.utc),
    "last_run_at": datetime(2026, 10, 2, 22, 41, tzinfo=timezone.utc),
    "last_run_status": "success", "tickers_expected": 20, "tickers_current": 20,
}


class FakeMarts(MartsClient):
    """Answers each query from in-memory tables, applying the bound parameters."""

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.tables = {
            "dim_tickers": TICKERS,
            "mart_period_metrics": PERIOD_METRICS,
            "mart_period_series": PERIOD_SERIES,
            "mart_daily_metrics": DAILY_METRICS,
            "mart_pipeline_status": [STATUS],
        }
        self.calls: list[tuple[str, dict]] = []
        self.error: Exception | None = None

    def query(self, sql, params=None):
        params = params or {}
        self.calls.append((sql, params))
        if self.error:
            raise self.error
        table = next(name for name in self.tables if f".{name}" in sql.split("from", 1)[1].split()[0])
        rows = self.tables[table]
        if "ticker" in params:
            rows = [r for r in rows if r["ticker"] == params["ticker"]]
        if "period" in params:
            rows = [r for r in rows if r.get("period", params["period"]) == params["period"]]
        if "start_date" in params:
            rows = [r for r in rows if params["start_date"] <= r["date"] <= params["end_date"]]
        return [dict(r) for r in rows]


@pytest.fixture
def marts():
    return FakeMarts(Settings(allowed_origins=FRONTEND))


@pytest.fixture
def client(marts):
    return TestClient(create_app(Settings(allowed_origins=FRONTEND), marts))


def test_health_does_not_touch_bigquery(client, marts):
    assert client.get("/health").json() == {"status": "ok"}
    assert marts.calls == []


def test_status(client):
    body = client.get("/status").json()

    assert body["last_market_date"] == "2026-10-02"
    assert body["last_run_status"] == "success"


def test_status_without_rows_is_503(client, marts):
    marts.tables["mart_pipeline_status"] = []

    assert client.get("/status").status_code == 503


def test_tickers_lists_everything_and_names_the_benchmark(client):
    body = client.get("/tickers").json()

    assert body["benchmark"] == "SPY"
    assert [t["ticker"] for t in body["tickers"]] == [t["ticker"] for t in TICKERS]


def test_performance(client):
    body = client.get("/performance/AAA", params={"period": "1Y"}).json()

    assert body["ticker"] == {"ticker": "AAA", "name": "Alpha Inc."}
    assert body["benchmark"]["ticker"] == "SPY"
    assert body["metrics"]["total_return"] == 0.25
    assert [p["cum_return"] for p in body["series"]] == [0.0, 0.3, 0.25]


def test_ticker_is_case_insensitive(client):
    assert client.get("/performance/aaa").json()["ticker"]["ticker"] == "AAA"


def test_benchmark_index_with_caret_is_accepted(client, marts):
    marts.tables["mart_period_metrics"] = [*PERIOD_METRICS, metrics("^GSPC", 0.09)]

    response = client.get("/performance/%5EGSPC")

    assert response.status_code == 200
    assert response.json()["ticker"]["ticker"] == "^GSPC"


def test_unknown_ticker_is_404(client):
    response = client.get("/performance/ZZZ")

    assert response.status_code == 404
    assert "Unknown ticker" in response.json()["detail"]


def test_ticker_without_data_in_the_period_is_404(client):
    assert client.get("/performance/AAA", params={"period": "5Y"}).status_code == 404


@pytest.mark.parametrize(
    "path",
    ["/performance/AAA?period=2Y", "/risk/AAA?period=1y", "/sectors?period=ALL", "/performance/AA%20A", "/risk/'or'1"],
)
def test_invalid_input_is_rejected_before_querying_metrics(client, marts, path):
    assert client.get(path).status_code == 422
    # At most the fixed, cached ticker lookup ran; nothing that takes user input.
    assert all("dim_tickers" in sql and not params for sql, params in marts.calls)


def test_queries_bind_parameters_instead_of_formatting_them(client, marts):
    client.get("/risk/AAA", params={"period": "1Y"})

    for sql, _ in marts.calls:
        assert "AAA" not in sql and "'1Y'" not in sql


def test_risk(client):
    body = client.get("/risk/AAA", params={"period": "1Y"}).json()

    assert [p["drawdown"] for p in body["drawdown"]] == [0.0, 0.0, -0.0385]
    # Rolling volatility is limited to the period's dates.
    assert [p["date"] for p in body["rolling_volatility"]] == ["2025-10-02", "2026-10-02"]
    distribution = body["return_distribution"]
    assert distribution["sessions"] == 2  # the first session has no return within the period
    assert sum(b["sessions"] for b in distribution["bins"]) == 2
    assert distribution["worst_day"] == {"date": "2026-10-02", "daily_return": -0.0385}


def test_sectors_separates_the_benchmark(client):
    body = client.get("/sectors", params={"period": "1Y"}).json()

    assert body["benchmark"]["ticker"] == "SPY"
    assert [s["ticker"] for s in body["sectors"]] == ["XLE", "XLK"]
    assert body["start_date"] == "2025-10-02"


def test_bigquery_failure_is_a_502(client, marts):
    marts.error = InternalServerError("boom")

    response = client.get("/status")

    assert response.status_code == 502
    assert "boom" not in response.text


def test_cors_allows_only_the_frontend(client):
    allowed = client.get("/health", headers={"Origin": FRONTEND})
    other = client.get("/health", headers={"Origin": "https://evil.example"})

    assert allowed.headers.get("access-control-allow-origin") == FRONTEND
    assert "access-control-allow-origin" not in other.headers


# --- Cache -------------------------------------------------------------------------------


class CountingBigQuery:
    def __init__(self):
        self.calls = 0

    def query(self, sql, job_config=None):
        self.calls += 1
        calls = self.calls

        class _Job:
            def result(self):
                return [{"n": calls}]

        return _Job()


def test_cache_expires_after_the_ttl():
    now = [0.0]
    bq = CountingBigQuery()
    marts = MartsClient(Settings(cache_ttl_seconds=3600), client=bq, clock=lambda: now[0])

    first = marts.query("select 1")
    now[0] = 3599
    assert marts.query("select 1") == first
    now[0] = 3600
    assert marts.query("select 1") == [{"n": 2}]
    assert bq.calls == 2


def test_cache_sets_the_byte_cap():
    seen = {}

    class Recorder(CountingBigQuery):
        def query(self, sql, job_config=None):
            seen["cap"] = job_config.maximum_bytes_billed
            return super().query(sql, job_config)

    MartsClient(Settings(max_bytes_billed=123), client=Recorder()).query("select 1")

    assert seen["cap"] == 123


# --- Return distribution -----------------------------------------------------------------


def test_distribution_bins_align_on_zero():
    days = [(date(2026, 1, d), r) for d, r in enumerate([-0.012, -0.001, 0.0, 0.004, 0.006], start=1)]

    result = return_distribution(days, bin_width=0.005)

    assert [(b.lower, b.upper, b.sessions) for b in result.bins] == [
        (-0.015, -0.01, 1),
        (-0.01, -0.005, 0),
        (-0.005, 0.0, 1),
        (0.0, 0.005, 2),
        (0.005, 0.01, 1),
    ]
    assert result.share_negative == pytest.approx(0.4)
    assert result.worst_day.daily_return == -0.012
    assert result.best_day.daily_return == 0.006


def test_distribution_of_nothing_is_empty():
    result = return_distribution([])

    assert result.bins == [] and result.sessions == 0 and result.worst_day is None
