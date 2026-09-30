"""Daily extraction: Yahoo Finance -> raw_finance.daily_prices.

    python -m extraction.pipeline                 # incremental run
    python -m extraction.pipeline --full-refresh  # reload all history
    python -m extraction.pipeline --tickers AAPL XLK

Each ticker resumes from its last loaded date minus a short overlap. If the
overlap no longer matches what is stored, Yahoo has re-based the adjusted prices
(new dividend or split) and that ticker is reloaded from the start, so adjusted
history is always on one basis. A ticker that fails is logged and skipped; the
others are still loaded.
"""

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Protocol
from zoneinfo import ZoneInfo

import pandas as pd

from extraction import config
from extraction.warehouse import TickerState, utc_now
from extraction.yahoo import get_prices

log = logging.getLogger("extraction")


class Warehouse(Protocol):
    def ensure_tables(self) -> None: ...
    def loaded_state(self, today: date) -> dict[str, TickerState]: ...
    def upsert_prices(self, df: pd.DataFrame) -> int: ...
    def log_run(self, record: dict) -> None: ...


@dataclass
class RunResult:
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"
    tickers_ok: list[str] = field(default_factory=list)
    tickers_failed: dict[str, str] = field(default_factory=dict)
    tickers_rebased: list[str] = field(default_factory=list)
    rows_upserted: int = 0
    max_market_date: date | None = None
    error: str | None = None

    def as_record(self, trigger: str) -> dict:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": (self.finished_at or utc_now()).isoformat(),
            "status": self.status,
            "trigger": trigger,
            "tickers_ok": self.tickers_ok,
            "tickers_failed": [{"ticker": t, "error": e} for t, e in self.tickers_failed.items()],
            "tickers_rebased": self.tickers_rebased,
            "rows_upserted": self.rows_upserted,
            "max_market_date": self.max_market_date.isoformat() if self.max_market_date else None,
            "error": self.error,
        }


def last_complete_session(now: datetime) -> date:
    """Latest calendar date whose daily bar is final. Weekends and holidays
    need no special case: Yahoo simply has no bar for them."""
    now_ny = now.astimezone(ZoneInfo(config.MARKET_TZ))
    if now_ny.time() >= config.DATA_READY:
        return now_ny.date()
    return now_ny.date() - timedelta(days=1)


def needs_rebase(fetched: pd.DataFrame, stored: TickerState) -> bool:
    new = dict(zip(fetched["date"], fetched["adj_close"]))
    overlap = [d for d in stored.recent_adj_close if d in new]
    if not overlap:
        return True  # nothing to compare against: reload to be safe
    return any(
        abs(new[d] / stored.recent_adj_close[d] - 1) > config.REBASE_TOLERANCE for d in overlap
    )


def run(
    warehouse: Warehouse,
    *,
    tickers: list[str] | None = None,
    now: datetime | None = None,
    full_refresh: bool = False,
    fetch_prices: Callable[..., pd.DataFrame] = get_prices,
    trigger: str = "manual",
) -> RunResult:
    now = now or utc_now()
    tickers = tickers or config.load_tickers()
    result = RunResult(run_id=str(uuid.uuid4()), started_at=now)
    last_session = last_complete_session(now)
    log.info("Run %s: %d tickers, sessions up to %s", result.run_id, len(tickers), last_session)

    try:
        warehouse.ensure_tables()
        state = {} if full_refresh else warehouse.loaded_state(last_session)

        frames = []
        for ticker in tickers:
            try:
                stored = state.get(ticker)
                if stored is None:
                    df = fetch_prices(ticker, config.HISTORY_START, last_session)
                else:
                    start = stored.last_date - timedelta(days=config.LOOKBACK_DAYS)
                    df = fetch_prices(ticker, start, last_session)
                    if needs_rebase(df, stored):
                        log.info("%s: adjusted prices re-based, reloading full history", ticker)
                        df = fetch_prices(ticker, config.HISTORY_START, last_session)
                        result.tickers_rebased.append(ticker)
                frames.append(df)
                result.tickers_ok.append(ticker)
                log.info("%s: %d rows up to %s", ticker, len(df), df["date"].max())
            except Exception as exc:
                result.tickers_failed[ticker] = f"{type(exc).__name__}: {exc}"[:500]
                log.error("%s: skipped: %s", ticker, exc)

        if frames:
            prices = pd.concat(frames, ignore_index=True)
            prices["loaded_at"] = now
            prices["run_id"] = result.run_id
            result.rows_upserted = warehouse.upsert_prices(prices)
            result.max_market_date = prices["date"].max()

        if not result.tickers_ok:
            result.status = "failed"
        elif result.tickers_failed:
            result.status = "partial"
        else:
            result.status = "success"
    except Exception as exc:
        result.status = "failed"
        result.error = f"{type(exc).__name__}: {exc}"[:1000]
        log.exception("Run failed")
    finally:
        result.finished_at = utc_now()
        try:
            warehouse.log_run(result.as_record(trigger))
        except Exception:
            log.exception("Could not write the run log")
            result.status = "failed"
    return result


def report_to_github(result: RunResult) -> None:
    """Expose the status to later workflow steps and write a job summary."""
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as f:
            f.write(f"status={result.status}\n")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        lines = [
            "### Extraction",
            f"- Status: **{result.status}**",
            f"- Tickers loaded: {len(result.tickers_ok)} · rows upserted: {result.rows_upserted}",
            f"- Latest market date: {result.max_market_date}",
        ]
        if result.tickers_rebased:
            lines.append(f"- Re-based (full reload): {', '.join(result.tickers_rebased)}")
        for ticker, error in result.tickers_failed.items():
            lines.append(f"- ❌ `{ticker}`: {error}")
        if result.error:
            lines.append(f"- ❌ {result.error}")
        with open(summary, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    for ticker, error in result.tickers_failed.items():
        print(f"::warning title=Ticker failed: {ticker}::{error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--full-refresh", action="store_true", help="reload every ticker from HISTORY_START")
    parser.add_argument("--tickers", nargs="+", help="subset of tickers (default: dbt seed)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from extraction.warehouse import BigQueryWarehouse  # the client needs credentials

    trigger = os.environ.get("GITHUB_EVENT_NAME", "manual")
    result = run(BigQueryWarehouse(), tickers=args.tickers, full_refresh=args.full_refresh, trigger=trigger)
    report_to_github(result)
    print(json.dumps(result.as_record(trigger), indent=2, default=str))
    # A partial run exits 0 so dbt still builds the tickers that loaded; the
    # workflow fails at the end on status != success.
    return 1 if result.status == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
