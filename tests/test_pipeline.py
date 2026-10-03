from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from extraction import config
from extraction.pipeline import last_complete_session, run
from tests.conftest import LAST_SESSION, NOW, yahoo_frame

TICKERS = ["AAA", "BBB", "CCC"]


def run_pipeline(warehouse, **kwargs):
    return run(warehouse, tickers=kwargs.pop("tickers", TICKERS), now=kwargs.pop("now", NOW), **kwargs)


# --- Scenarios requested for the extraction -----------------------------------


def test_empty_response_is_retried_and_recovers(fake_yf, warehouse):
    fake_yf.behaviors["BBB"] = ["empty", "empty", "ok"]

    result = run_pipeline(warehouse)

    assert result.status == "success"
    assert fake_yf.calls_for("BBB") == 3
    assert warehouse.count("BBB") > 0


def test_network_error_is_retried_and_recovers(fake_yf, warehouse):
    fake_yf.behaviors["AAA"] = [ConnectionError("connection reset"), TimeoutError("timed out"), "ok"]

    result = run_pipeline(warehouse)

    assert result.status == "success"
    assert fake_yf.calls_for("AAA") == 3


def test_persistent_network_error_skips_only_that_ticker(fake_yf, warehouse):
    fake_yf.behaviors["AAA"] = [ConnectionError("connection reset")]

    result = run_pipeline(warehouse)

    assert result.status == "partial"
    assert fake_yf.calls_for("AAA") == config.MAX_ATTEMPTS
    assert "connection reset" in result.tickers_failed["AAA"]
    assert result.tickers_ok == ["BBB", "CCC"]
    assert warehouse.count("AAA") == 0
    assert warehouse.count("BBB") > 0


def test_nonexistent_ticker_is_logged_and_the_rest_load(fake_yf, warehouse):
    # yfinance answers an unknown symbol with an empty DataFrame, not an exception.
    fake_yf.behaviors["NOPE"] = ["empty"]

    result = run_pipeline(warehouse, tickers=["AAA", "NOPE", "BBB"])

    assert result.status == "partial"
    assert "empty response" in result.tickers_failed["NOPE"]
    assert result.tickers_ok == ["AAA", "BBB"]
    assert warehouse.runs[-1]["tickers_failed"] == [{"ticker": "NOPE", "error": result.tickers_failed["NOPE"]}]


def test_repeated_load_on_the_same_day_does_not_duplicate(fake_yf, warehouse):
    first = run_pipeline(warehouse)
    rows_after_first = warehouse.count()
    snapshot = {k: v["adj_close"] for k, v in warehouse.rows.items()}

    second = run_pipeline(warehouse)

    assert first.status == second.status == "success"
    assert warehouse.count() == rows_after_first
    assert {k: v["adj_close"] for k, v in warehouse.rows.items()} == snapshot
    # The second run only re-reads the overlap window, not all history.
    assert second.rows_upserted < first.rows_upserted
    assert second.tickers_rebased == []


# --- Behavior the scenarios depend on -------------------------------------------


def test_first_run_backfills_from_history_start(fake_yf, warehouse):
    run_pipeline(warehouse, tickers=["AAA"])

    dates = sorted(d for _, d in warehouse.rows)
    assert dates[0] == date(2021, 1, 1)
    assert dates[-1] == LAST_SESSION


def test_failed_ticker_keeps_its_stored_history(fake_yf, warehouse):
    run_pipeline(warehouse)
    rows_before = warehouse.count("AAA")
    fake_yf.behaviors["AAA"] = [ConnectionError("down")]

    result = run_pipeline(warehouse)

    assert result.status == "partial"
    assert warehouse.count("AAA") == rows_before


def test_all_tickers_failing_marks_the_run_failed(fake_yf, warehouse):
    for ticker in TICKERS:
        fake_yf.behaviors[ticker] = ["empty"]

    result = run_pipeline(warehouse)

    assert result.status == "failed"
    assert result.rows_upserted == 0
    assert warehouse.runs[-1]["status"] == "failed"


def test_rebased_adjusted_prices_trigger_a_full_reload(fake_yf, warehouse):
    run_pipeline(warehouse)
    fake_yf.factor["BBB"] = 0.98  # a dividend re-scales all past adj_close values

    result = run_pipeline(warehouse)

    assert result.tickers_rebased == ["BBB"]
    bbb = [v for (t, _), v in warehouse.rows.items() if t == "BBB"]
    assert all(v["adj_close"] == pytest.approx(v["close"] * 0.98) for v in bbb)


def test_invalid_prices_fail_the_ticker(fake_yf, warehouse):
    def negative_prices(start, end):
        frame = yahoo_frame(start, end)
        frame.iloc[-1, frame.columns.get_loc("Close")] = -1.0
        return frame

    fake_yf.behaviors["CCC"] = [negative_prices]

    result = run_pipeline(warehouse)

    assert result.status == "partial"
    assert "non-positive prices" in result.tickers_failed["CCC"]


def test_placeholder_rows_without_prices_are_dropped(fake_yf, warehouse):
    def with_placeholder(start, end):
        frame = yahoo_frame(start, end)
        frame.iloc[-1, :5] = float("nan")
        return frame

    fake_yf.behaviors["AAA"] = [with_placeholder]

    result = run_pipeline(warehouse, tickers=["AAA"])

    assert result.status == "success"
    assert (("AAA", LAST_SESSION)) not in warehouse.rows


def test_intraday_bar_is_not_loaded(fake_yf, warehouse):
    during_session = datetime(2026, 9, 30, 15, 0, tzinfo=timezone.utc)  # 11:00 in New York

    run_pipeline(warehouse, tickers=["AAA"], now=during_session)

    assert max(d for _, d in warehouse.rows) == date(2026, 9, 29)


def test_yfinance_is_asked_for_unadjusted_and_adjusted_close(fake_yf, warehouse):
    run_pipeline(warehouse, tickers=["AAA"])

    _, kwargs = fake_yf.calls[0]
    assert kwargs["auto_adjust"] is False
    row = next(iter(warehouse.rows.values()))
    assert {"close", "adj_close"} <= set(row)


@pytest.mark.parametrize(
    ("now_utc", "expected"),
    [
        (datetime(2026, 9, 30, 20, 59, tzinfo=timezone.utc), date(2026, 9, 29)),  # 16:59 EDT
        (datetime(2026, 9, 30, 21, 0, tzinfo=timezone.utc), date(2026, 9, 30)),  # 17:00 EDT
        (datetime(2026, 12, 1, 21, 30, tzinfo=timezone.utc), date(2026, 11, 30)),  # 16:30 EST
        (datetime(2026, 12, 1, 22, 30, tzinfo=timezone.utc), date(2026, 12, 1)),  # 17:30 EST
    ],
)
def test_last_complete_session_follows_new_york_time(now_utc, expected):
    assert last_complete_session(now_utc) == expected


def test_tickers_come_from_the_dbt_seed():
    tickers = config.load_tickers()

    assert len(tickers) == len(set(tickers)) == 20
    assert {"SPY", "^GSPC", "XLK", "XLRE", "AAPL"} <= set(tickers)
