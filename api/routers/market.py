"""Market endpoints. Every query is fixed SQL over the marts with bound parameters."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from api.bigquery import MartsClient, Row
from api.deps import KnownTicker, Marts, benchmark_of, load_tickers
from api.distribution import return_distribution
from api.schemas import (
    DrawdownPoint,
    PerformancePoint,
    PerformanceResponse,
    Period,
    PeriodMetrics,
    RiskResponse,
    SectorRow,
    SectorsResponse,
    StatusResponse,
    TickerRef,
    TickersResponse,
    VolatilityPoint,
)

router = APIRouter(tags=["market"])

NOT_FOUND = {404: {"description": "Unknown ticker, or no data for it in the period."}}

METRIC_COLUMNS = """
    start_date, end_date, sessions, has_full_history, total_return, cagr, volatility,
    max_drawdown, sharpe_ratio, beta, benchmark_total_return, excess_return
"""


@router.get("/status", response_model=StatusResponse, summary="How fresh the data is")
def status(marts: Marts) -> StatusResponse:
    """From `mart_pipeline_status`: the latest market session in the data and the latest
    extraction runs. The dashboard shows "Data updated: <last_market_date> market close"."""
    rows = marts.query(
        f"""
        select last_market_date, last_successful_run_at, last_run_at, last_run_status,
               tickers_expected, tickers_current
        from {marts.settings.marts}.mart_pipeline_status
        """
    )
    if not rows:
        raise HTTPException(status_code=503, detail="The pipeline has not produced a status yet.")
    return StatusResponse(**rows[0])


@router.get("/tickers", response_model=TickersResponse, summary="Tickers and sectors available")
def tickers(marts: Marts) -> TickersResponse:
    """Stocks first, then the SPDR sector ETFs, then benchmarks; from `dim_tickers`."""
    rows = load_tickers(marts)
    return TickersResponse(benchmark=benchmark_of(rows)["ticker"], tickers=rows)


@router.get(
    "/performance/{ticker}",
    response_model=PerformanceResponse,
    summary="Cumulative return against the benchmark",
    responses=NOT_FOUND,
)
def performance(marts: Marts, ticker: KnownTicker, period: Period = Period.one_year) -> PerformanceResponse:
    """Headline metrics of the period (`mart_period_metrics`) and both series re-based to the
    period's first session (`mart_period_series`)."""
    params = {"ticker": ticker["ticker"], "period": period.value}
    metrics = _period_metrics(marts, params)
    series = marts.query(
        f"""
        select date, cum_return, benchmark_cum_return
        from {marts.settings.marts}.mart_period_series
        where ticker = @ticker and period = @period
        order by date
        """,
        params,
    )
    return PerformanceResponse(
        ticker=_ref(ticker),
        benchmark=_ref(benchmark_of(load_tickers(marts))),
        period=period,
        metrics=metrics,
        series=[PerformancePoint(**row) for row in series],
    )


@router.get(
    "/risk/{ticker}",
    response_model=RiskResponse,
    summary="Volatility, drawdown, Sharpe and their series",
    responses=NOT_FOUND,
)
def risk(marts: Marts, ticker: KnownTicker, period: Period = Period.one_year) -> RiskResponse:
    """Risk metrics of the period, the drawdown series within it, the rolling 1-month and
    1-year volatility (`mart_daily_metrics`), and the distribution of daily returns."""
    params = {"ticker": ticker["ticker"], "period": period.value}
    metrics = _period_metrics(marts, params)
    series = marts.query(
        f"""
        select date, drawdown, daily_return
        from {marts.settings.marts}.mart_period_series
        where ticker = @ticker and period = @period
        order by date
        """,
        params,
    )
    volatility = marts.query(
        f"""
        select date, rolling_volatility_1m as volatility_1m, rolling_volatility_1y as volatility_1y
        from {marts.settings.marts}.mart_daily_metrics
        where ticker = @ticker and date between @start_date and @end_date
        order by date
        """,
        {"ticker": ticker["ticker"], "start_date": metrics.start_date, "end_date": metrics.end_date},
    )
    returns = [(row["date"], row["daily_return"]) for row in series if row["daily_return"] is not None]
    return RiskResponse(
        ticker=_ref(ticker),
        period=period,
        metrics=metrics,
        drawdown=[DrawdownPoint(date=row["date"], drawdown=row["drawdown"]) for row in series],
        rolling_volatility=[VolatilityPoint(**row) for row in volatility],
        return_distribution=return_distribution(returns),
    )


@router.get("/sectors", response_model=SectorsResponse, summary="Sector ETFs compared")
def sectors(marts: Marts, period: Period = Period.one_year) -> SectorsResponse:
    """Return and risk of the 11 SPDR sector ETFs over the period, plus the benchmark on the
    same dates, from `mart_period_metrics`."""
    rows = marts.query(
        f"""
        select ticker, name, sector, asset_type, start_date, end_date, total_return, cagr,
               volatility, max_drawdown, sharpe_ratio, beta, excess_return
        from {marts.settings.marts}.mart_period_metrics m
        where period = @period
          and (asset_type = 'sector_etf' or ticker = (
              select ticker from {marts.settings.marts}.dim_tickers where is_benchmark
          ))
        order by total_return desc
        """,
        {"period": period.value},
    )
    benchmark = next((row for row in rows if row["asset_type"] == "benchmark"), None)
    sector_rows = [row for row in rows if row["asset_type"] == "sector_etf"]
    if benchmark is None or not sector_rows:
        raise HTTPException(status_code=503, detail="Sector metrics are not available yet.")
    return SectorsResponse(
        period=period,
        start_date=benchmark["start_date"],
        end_date=benchmark["end_date"],
        benchmark=SectorRow(**benchmark),
        sectors=[SectorRow(**row) for row in sector_rows],
    )


def _period_metrics(marts: MartsClient, params: dict) -> PeriodMetrics:
    rows = marts.query(
        f"""
        select {METRIC_COLUMNS}
        from {marts.settings.marts}.mart_period_metrics
        where ticker = @ticker and period = @period
        """,
        params,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {params['ticker']} over {params['period']}.")
    return PeriodMetrics(**rows[0])


def _ref(row: Row) -> TickerRef:
    return TickerRef(ticker=row["ticker"], name=row["name"])
