// Response shapes of the Financial Market API (api/schemas.py). Dates are ISO
// strings; returns, volatility and drawdowns are fractions (0.25 = 25%).

export const PERIODS = ["1Y", "3Y", "5Y", "MAX"] as const
export type Period = (typeof PERIODS)[number]

export type AssetType = "stock" | "sector_etf" | "benchmark"

export interface StatusResponse {
  last_market_date: string
  last_successful_run_at: string | null
  last_run_at: string | null
  last_run_status: "success" | "partial" | "failed" | null
  tickers_expected: number
  tickers_current: number
}

export interface Ticker {
  ticker: string
  name: string
  asset_type: AssetType
  sector: string | null
  sector_etf: string | null
  is_benchmark: boolean
  first_date: string | null
  last_date: string | null
}

export interface TickersResponse {
  benchmark: string
  tickers: Ticker[]
}

export interface TickerRef {
  ticker: string
  name: string
}

export interface PeriodMetrics {
  start_date: string
  end_date: string
  sessions: number
  has_full_history: boolean
  total_return: number
  cagr: number | null
  volatility: number | null
  max_drawdown: number
  sharpe_ratio: number | null
  beta: number | null
  benchmark_total_return: number | null
  excess_return: number | null
}

export interface PerformanceResponse {
  ticker: TickerRef
  benchmark: TickerRef
  period: Period
  metrics: PeriodMetrics
  series: { date: string; cum_return: number; benchmark_cum_return: number | null }[]
}

export interface ReturnBin {
  lower: number
  upper: number
  sessions: number
}

export interface DayReturn {
  date: string
  daily_return: number
}

export interface RiskResponse {
  ticker: TickerRef
  period: Period
  metrics: PeriodMetrics
  drawdown: { date: string; drawdown: number }[]
  rolling_volatility: {
    date: string
    volatility_1m: number | null
    volatility_1y: number | null
  }[]
  return_distribution: {
    bin_width: number
    bins: ReturnBin[]
    sessions: number
    mean: number | null
    std: number | null
    share_negative: number | null
    worst_day: DayReturn | null
    best_day: DayReturn | null
  }
}

export interface SectorRow {
  ticker: string
  name: string
  sector: string | null
  total_return: number
  cagr: number | null
  volatility: number | null
  max_drawdown: number
  sharpe_ratio: number | null
  beta: number | null
  excess_return: number | null
}

export interface SectorsResponse {
  period: Period
  start_date: string
  end_date: string
  benchmark: SectorRow
  sectors: SectorRow[]
}
