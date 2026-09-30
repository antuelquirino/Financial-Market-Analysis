"""Pipeline settings. The ticker universe lives in the dbt seed, not here."""

import csv
import os
from datetime import date, time
from pathlib import Path

PROJECT_ID = os.environ.get("GCP_PROJECT", "financial-market-analysis")
RAW_DATASET = "raw_finance"

PRICES_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.daily_prices"
RUNS_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.pipeline_runs"
LOAD_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._daily_prices_load"  # scratch table for MERGE

# First day of history for a ticker that has never been loaded.
HISTORY_START = date(2021, 1, 1)

# Each run re-downloads this many calendar days before the last loaded date.
# The overlap is compared with what is stored to detect re-based adjusted prices.
LOOKBACK_DAYS = 10

# Relative difference in adj_close above which the stored history is considered
# re-based (a new dividend or split) and the ticker is reloaded from HISTORY_START.
# A quarterly dividend moves the adjustment factor by ~1e-3; float noise is ~1e-9.
REBASE_TOLERANCE = 1e-4

# Retries against Yahoo Finance: waits of 2, 4 and 8 seconds between 4 attempts.
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2.0

# US market calendar. A session's daily bar is treated as final after DATA_READY.
MARKET_TZ = "America/New_York"
DATA_READY = time(17, 0)

TICKERS_CSV = Path(__file__).resolve().parent.parent / "dbt_project" / "seeds" / "tickers.csv"


def load_tickers(path: Path = TICKERS_CSV) -> list[str]:
    with open(path, encoding="utf-8", newline="") as f:
        return [row["ticker"] for row in csv.DictReader(f)]
