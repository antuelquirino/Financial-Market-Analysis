"""Request filters and response models. Field examples feed the OpenAPI docs.

Returns, volatility and drawdowns are fractions (0.25 = 25%); formatting is left
to the client.
"""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field


class Period(str, Enum):
    one_year = "1Y"
    three_years = "3Y"
    five_years = "5Y"
    max = "MAX"


class AssetType(str, Enum):
    stock = "stock"
    sector_etf = "sector_etf"
    benchmark = "benchmark"


# --- /status ----------------------------------------------------------------------

class StatusResponse(BaseModel):
    last_market_date: date = Field(
        description="Latest session in the data (the benchmark's). Prices are as of its close.",
        examples=["2026-10-02"],
    )
    last_successful_run_at: datetime | None = Field(
        description="When the last fully successful extraction finished (UTC).",
        examples=["2026-10-02T22:41:07Z"],
    )
    last_run_at: datetime | None = Field(description="When the latest extraction finished (UTC).")
    last_run_status: str | None = Field(description="success, partial or failed.", examples=["success"])
    tickers_expected: int = Field(examples=[20])
    tickers_current: int = Field(description="Tickers with data for last_market_date.", examples=[20])


# --- /tickers ---------------------------------------------------------------------

class Ticker(BaseModel):
    ticker: str = Field(examples=["NVDA"])
    name: str = Field(examples=["NVIDIA Corporation"])
    asset_type: AssetType
    sector: str | None = Field(description="GICS sector. Null for benchmarks.", examples=["Information Technology"])
    sector_etf: str | None = Field(description="SPDR ETF of the ticker's sector.", examples=["XLK"])
    is_benchmark: bool = Field(description="True for the benchmark every comparison uses.")
    first_date: date | None
    last_date: date | None


class TickersResponse(BaseModel):
    benchmark: str = Field(examples=["SPY"])
    tickers: list[Ticker]


# --- Shared by /performance, /risk and /sectors ------------------------------------

class PeriodMetrics(BaseModel):
    start_date: date = Field(description="First session of the period.")
    end_date: date = Field(description="Last session of the period.")
    sessions: int = Field(examples=[251])
    has_full_history: bool = Field(description="False when the ticker has less history than the period.")
    total_return: float = Field(examples=[0.252])
    cagr: float | None = Field(description="Compound annual growth rate.", examples=[0.253])
    volatility: float | None = Field(description="Annualized std of daily returns.", examples=[0.378])
    max_drawdown: float = Field(description="Deepest peak-to-trough fall, <= 0.", examples=[-0.202])
    sharpe_ratio: float | None = Field(examples=[0.68])
    beta: float | None = Field(description="Against the benchmark's daily returns.", examples=[1.89])
    benchmark_total_return: float | None = Field(examples=[0.221])
    excess_return: float | None = Field(description="total_return - benchmark_total_return.", examples=[0.031])


class TickerRef(BaseModel):
    ticker: str = Field(examples=["NVDA"])
    name: str = Field(examples=["NVIDIA Corporation"])


# --- /performance/{ticker} ----------------------------------------------------------

class PerformancePoint(BaseModel):
    date: date
    cum_return: float = Field(description="Growth since the period's first session.", examples=[0.12])
    benchmark_cum_return: float | None = Field(examples=[0.08])


class PerformanceResponse(BaseModel):
    ticker: TickerRef
    benchmark: TickerRef
    period: Period
    metrics: PeriodMetrics
    series: list[PerformancePoint]


# --- /risk/{ticker} -------------------------------------------------------------------

class DrawdownPoint(BaseModel):
    date: date
    drawdown: float = Field(description="Price over its peak since the period started, minus 1.", examples=[-0.08])


class VolatilityPoint(BaseModel):
    date: date
    volatility_1m: float | None = Field(description="Annualized, rolling 21 sessions.")
    volatility_1y: float | None = Field(description="Annualized, rolling 252 sessions.")


class ReturnBin(BaseModel):
    lower: float = Field(description="Inclusive lower edge of the daily return bin.", examples=[-0.01])
    upper: float = Field(description="Exclusive upper edge.", examples=[-0.005])
    sessions: int = Field(examples=[31])


class DayReturn(BaseModel):
    date: date
    daily_return: float


class ReturnDistribution(BaseModel):
    bin_width: float = Field(examples=[0.005])
    bins: list[ReturnBin]
    sessions: int
    mean: float | None
    std: float | None
    share_negative: float | None = Field(description="Fraction of sessions with a loss.", examples=[0.46])
    worst_day: DayReturn | None
    best_day: DayReturn | None


class RiskResponse(BaseModel):
    ticker: TickerRef
    period: Period
    metrics: PeriodMetrics
    drawdown: list[DrawdownPoint]
    rolling_volatility: list[VolatilityPoint]
    return_distribution: ReturnDistribution


# --- /sectors ---------------------------------------------------------------------------

class SectorRow(BaseModel):
    ticker: str = Field(examples=["XLK"])
    name: str = Field(examples=["Technology Select Sector SPDR Fund"])
    sector: str | None = Field(examples=["Information Technology"])
    total_return: float
    cagr: float | None
    volatility: float | None
    max_drawdown: float
    sharpe_ratio: float | None
    beta: float | None
    excess_return: float | None


class SectorsResponse(BaseModel):
    period: Period
    start_date: date
    end_date: date
    benchmark: SectorRow
    sectors: list[SectorRow] = Field(description="The 11 SPDR sector ETFs.")
