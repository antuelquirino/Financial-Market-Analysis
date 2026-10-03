"""BigQuery side of the pipeline: table setup, load state, idempotent upsert, run log."""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from google.cloud import bigquery

from extraction import config

log = logging.getLogger(__name__)

PRICES_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("open", "FLOAT64"),
    bigquery.SchemaField("high", "FLOAT64"),
    bigquery.SchemaField("low", "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED", description="Raw close"),
    bigquery.SchemaField(
        "adj_close", "FLOAT64", mode="REQUIRED",
        description="Close adjusted for splits and dividends (use for returns)",
    ),
    bigquery.SchemaField("volume", "INT64"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
]

RUNS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("finished_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("trigger", "STRING"),
    bigquery.SchemaField("tickers_ok", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "tickers_failed", "RECORD", mode="REPEATED",
        fields=[bigquery.SchemaField("ticker", "STRING"), bigquery.SchemaField("error", "STRING")],
    ),
    bigquery.SchemaField("tickers_rebased", "STRING", mode="REPEATED"),
    bigquery.SchemaField("rows_upserted", "INT64"),
    bigquery.SchemaField("max_market_date", "DATE"),
    bigquery.SchemaField("error", "STRING"),
]

# Only the most recent year is read to decide where each ticker resumes; a
# ticker with nothing in that window is simply backfilled from HISTORY_START.
STATE_WINDOW_DAYS = 400


@dataclass
class TickerState:
    last_date: date
    recent_adj_close: dict[date, float] = field(default_factory=dict)


class BigQueryWarehouse:
    def __init__(self, client: bigquery.Client | None = None):
        self.client = client or bigquery.Client(project=config.PROJECT_ID)

    def ensure_tables(self) -> None:
        prices = bigquery.Table(config.PRICES_TABLE, schema=PRICES_SCHEMA)
        # ~20 tickers x 252 sessions a year is tiny: monthly partitions keep them
        # reasonably sized while still pruning scans by date.
        prices.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="date"
        )
        prices.clustering_fields = ["ticker"]
        prices.description = "Daily OHLCV from Yahoo Finance. One row per (ticker, date)."

        runs = bigquery.Table(config.RUNS_TABLE, schema=RUNS_SCHEMA)
        runs.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="started_at"
        )
        runs.description = "One row per extraction run."

        for table in (prices, runs):
            self.client.create_table(table, exists_ok=True)

    def loaded_state(self, today: date, lookback_days: int = config.LOOKBACK_DAYS) -> dict[str, TickerState]:
        """Last loaded date per ticker, plus the stored adj_close of the overlap window."""
        sql = f"""
            with recent as (
                select ticker, date, adj_close
                from `{config.PRICES_TABLE}`
                where date >= @since
            ),
            last_loaded as (
                select ticker, max(date) as last_date from recent group by ticker
            )
            select r.ticker, l.last_date, r.date, r.adj_close
            from recent r
            join last_loaded l using (ticker)
            where r.date >= date_sub(l.last_date, interval @lookback day)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("since", "DATE", today - timedelta(days=STATE_WINDOW_DAYS)),
                bigquery.ScalarQueryParameter("lookback", "INT64", lookback_days),
            ]
        )
        state: dict[str, TickerState] = {}
        for row in self.client.query(sql, job_config=job_config).result():
            ticker_state = state.setdefault(row.ticker, TickerState(last_date=row.last_date))
            ticker_state.recent_adj_close[row.date] = row.adj_close
        return state

    def upsert_prices(self, df: pd.DataFrame) -> int:
        """Load into a scratch table, then MERGE on (ticker, date).

        Re-running the same day updates rows instead of duplicating them, and a
        ticker missing from `df` (because it failed) keeps its stored history.
        """
        load_config = bigquery.LoadJobConfig(
            schema=PRICES_SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        self.client.load_table_from_dataframe(df, config.LOAD_TABLE, job_config=load_config).result()
        try:
            merge = f"""
                merge `{config.PRICES_TABLE}` t
                using `{config.LOAD_TABLE}` s
                on t.ticker = s.ticker and t.date = s.date
                when matched then update set
                    open = s.open, high = s.high, low = s.low, close = s.close,
                    adj_close = s.adj_close, volume = s.volume,
                    loaded_at = s.loaded_at, run_id = s.run_id
                when not matched then insert row
            """
            job = self.client.query(merge)
            job.result()
            return job.num_dml_affected_rows or 0
        finally:
            self.client.delete_table(config.LOAD_TABLE, not_found_ok=True)

    def log_run(self, record: dict) -> None:
        # A load job (not a streaming insert) keeps the table free for DML and costs nothing.
        job_config = bigquery.LoadJobConfig(
            schema=RUNS_SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        self.client.load_table_from_json([record], config.RUNS_TABLE, job_config=job_config).result()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
