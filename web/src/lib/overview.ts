// What the Overview screen says: its lead sentence, KPIs against the benchmark,
// and the finding of the performance chart. Pure functions over API responses.

import type { KpiItem } from "@/components/market/KpiStrip"
import type { ChartDatum } from "./chartUtils"
import {
  formatDate,
  formatNumber,
  formatPercent,
  formatPoints,
  toneOf,
} from "./format"
import { periodPhrase } from "./selection"
import type { PerformanceResponse, PeriodMetrics } from "./types"

export interface Sentence {
  before: string
  mark: string // the key figure, shown with the marker
  after: string
}

// Differences smaller than this read as "in line".
const SAME = 0.0005

export function describePeriod(response: PerformanceResponse): string {
  return periodPhrase(response.period, formatDate(response.metrics.start_date))
}

const isBenchmark = (response: PerformanceResponse) =>
  response.ticker.ticker === response.benchmark.ticker

/** "NVDA returned +24.2% over the past year, 2.1 pts ahead of SPY." */
export function overviewLead(response: PerformanceResponse): Sentence {
  const { ticker, benchmark, metrics } = response
  const before = `${ticker.ticker} returned `
  const mark = formatPercent(metrics.total_return, { signed: true })
  const period = describePeriod(response)
  if (isBenchmark(response) || metrics.excess_return === null) {
    return { before, mark, after: ` ${period}.` }
  }
  const excess = metrics.excess_return
  const versus =
    Math.abs(excess) < SAME
      ? `in line with ${benchmark.ticker}`
      : `${formatPoints(Math.abs(excess)).slice(1)} ${excess > 0 ? "ahead of" : "behind"} ${benchmark.ticker}`
  return { before, mark, after: ` ${period}, ${versus}.` }
}

function change(
  difference: number | null,
  text: string,
  higherIsBetter: boolean,
  benchmark: string,
): KpiItem["change"] {
  if (difference === null) return undefined
  const flat = Math.abs(difference) < SAME
  return {
    text: flat ? "Same" : text,
    direction: flat ? "flat" : difference > 0 ? "up" : "down",
    tone: flat ? "neutral" : toneOf(difference, higherIsBetter),
    comparison: `vs ${benchmark}`,
  }
}

const diff = (a: number | null, b: number | null) =>
  a === null || b === null ? null : a - b

const signedNumber = (value: number) =>
  (value > 0 ? "+" : "") + formatNumber(value)

/**
 * Five KPIs, each compared with the benchmark over the same dates: more return
 * and Sharpe are good news; more volatility and a deeper drawdown are not.
 */
export function overviewKpis(
  response: PerformanceResponse,
  benchmark: PeriodMetrics | null,
): KpiItem[] {
  const m = response.metrics
  const b = isBenchmark(response) ? null : benchmark
  const name = response.benchmark.ticker
  const note = isBenchmark(response) ? "The benchmark" : undefined
  const vol = diff(m.volatility, b?.volatility ?? null)
  const dd = diff(m.max_drawdown, b?.max_drawdown ?? null)
  const sharpe = diff(m.sharpe_ratio, b?.sharpe_ratio ?? null)
  const excess = isBenchmark(response) ? null : m.excess_return
  return [
    {
      label: "Return",
      value: formatPercent(m.total_return, { signed: true }),
      change: excess === null ? undefined : change(excess, formatPoints(excess), true, name),
      note,
    },
    {
      label: "Annualized return",
      value: formatPercent(m.cagr, { signed: true }),
      note: "CAGR",
    },
    {
      label: "Volatility",
      value: formatPercent(m.volatility),
      change: vol === null ? undefined : change(vol, formatPoints(vol), false, name),
      note: note ?? "Annualized",
    },
    {
      label: "Max drawdown",
      value: formatPercent(m.max_drawdown),
      change: dd === null ? undefined : change(dd, formatPoints(dd), true, name),
      note,
    },
    {
      label: "Sharpe ratio",
      value: formatNumber(m.sharpe_ratio),
      change: sharpe === null ? undefined : change(sharpe, signedNumber(sharpe), true, name),
      note,
    },
  ]
}

/** One row per session, keyed by symbol, for the line chart. */
export function performanceChartData(response: PerformanceResponse): ChartDatum[] {
  const own = response.ticker.ticker
  const other = response.benchmark.ticker
  return response.series.map((point) =>
    isBenchmark(response)
      ? { date: point.date, [own]: point.cum_return }
      : {
          date: point.date,
          [own]: point.cum_return,
          [other]: point.benchmark_cum_return,
        },
  )
}

// Giving back less than this from the peak still counts as "near its high".
const NEAR_HIGH = 0.03

/** The chart's title: where the path peaked and how much of it was kept. */
export function performanceFinding(response: PerformanceResponse): string {
  const { series, ticker } = response
  if (!series.length) return `${ticker.ticker} has no data for this period`
  const last = series[series.length - 1]
  const peak = series.reduce((best, p) => (p.cum_return > best.cum_return ? p : best))
  const end = formatPercent(last.cum_return, { signed: true })
  if (peak.cum_return <= 0) {
    return `${ticker.ticker} never rose above its starting price, ending at ${end}`
  }
  // Like a drawdown: the fall measured from the peak, not from the end.
  const givenBack = 1 - (1 + last.cum_return) / (1 + peak.cum_return)
  if (givenBack < NEAR_HIGH) {
    return `${ticker.ticker} ended near its high for the period, at ${end}`
  }
  return `${ticker.ticker} peaked at ${formatPercent(peak.cum_return, { signed: true })} on ${formatDate(peak.date)}, then fell ${formatPercent(givenBack)} to ${end}`
}
