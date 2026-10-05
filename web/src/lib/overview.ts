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
import { MESSAGES, type Sentence } from "./i18n"
import type { Locale } from "./locale"
import type { PerformanceResponse, PeriodMetrics } from "./types"

export type { Sentence } from "./i18n"

// Differences smaller than this read as "in line".
const SAME = 0.0005

export function describePeriod(response: PerformanceResponse, locale: Locale = "en"): string {
  return MESSAGES[locale].period.phrase(
    response.period,
    formatDate(response.metrics.start_date, "day", { locale }),
  )
}

const isBenchmark = (response: PerformanceResponse) =>
  response.ticker.ticker === response.benchmark.ticker

/** "NVDA returned +24.2% over the past year, 2.1 pts ahead of SPY." */
export function overviewLead(response: PerformanceResponse, locale: Locale = "en"): Sentence {
  const t = MESSAGES[locale].overview
  const { ticker, benchmark, metrics } = response
  const ret = formatPercent(metrics.total_return, { signed: true, locale })
  const period = describePeriod(response, locale)
  if (isBenchmark(response) || metrics.excess_return === null) {
    return t.lead(ticker.ticker, ret, period, null)
  }
  const excess = metrics.excess_return
  const points = formatPoints(Math.abs(excess), { locale }).slice(1) // no sign: "ahead" says it
  const versus =
    Math.abs(excess) < SAME
      ? t.inLine(benchmark.ticker)
      : excess > 0
        ? t.ahead(points, benchmark.ticker)
        : t.behind(points, benchmark.ticker)
  return t.lead(ticker.ticker, ret, period, versus)
}

function change(
  difference: number | null,
  text: string,
  higherIsBetter: boolean,
  benchmark: string,
  locale: Locale,
): KpiItem["change"] {
  if (difference === null) return undefined
  const k = MESSAGES[locale].kpi
  const flat = Math.abs(difference) < SAME
  return {
    text: flat ? k.same : text,
    direction: flat ? "flat" : difference > 0 ? "up" : "down",
    tone: flat ? "neutral" : toneOf(difference, higherIsBetter),
    comparison: k.versus(benchmark),
  }
}

const diff = (a: number | null, b: number | null) =>
  a === null || b === null ? null : a - b

/**
 * Five KPIs, each compared with the benchmark over the same dates: more return
 * and Sharpe are good news; more volatility and a deeper drawdown are not.
 */
export function overviewKpis(
  response: PerformanceResponse,
  benchmark: PeriodMetrics | null,
  locale: Locale = "en",
): KpiItem[] {
  const k = MESSAGES[locale].kpi
  const m = response.metrics
  const b = isBenchmark(response) ? null : benchmark
  const name = response.benchmark.ticker
  const note = isBenchmark(response) ? k.theBenchmark : undefined
  const vol = diff(m.volatility, b?.volatility ?? null)
  const dd = diff(m.max_drawdown, b?.max_drawdown ?? null)
  const sharpe = diff(m.sharpe_ratio, b?.sharpe_ratio ?? null)
  const excess = isBenchmark(response) ? null : m.excess_return
  const points = (value: number) => formatPoints(value, { locale })
  return [
    {
      label: k.return,
      value: formatPercent(m.total_return, { signed: true, locale }),
      change: excess === null ? undefined : change(excess, points(excess), true, name, locale),
      note,
    },
    {
      label: k.cagr,
      value: formatPercent(m.cagr, { signed: true, locale }),
      note: k.cagrNote,
    },
    {
      label: k.volatility,
      value: formatPercent(m.volatility, { locale }),
      change: vol === null ? undefined : change(vol, points(vol), false, name, locale),
      note: note ?? k.annualized,
    },
    {
      label: k.maxDrawdown,
      value: formatPercent(m.max_drawdown, { locale }),
      change: dd === null ? undefined : change(dd, points(dd), true, name, locale),
      note,
    },
    {
      label: k.sharpe,
      value: formatNumber(m.sharpe_ratio, { locale }),
      change:
        sharpe === null
          ? undefined
          : change(sharpe, formatNumber(sharpe, { signed: true, locale }), true, name, locale),
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
export function performanceFinding(response: PerformanceResponse, locale: Locale = "en"): string {
  const t = MESSAGES[locale].overview
  const { series, ticker } = response
  if (!series.length) return t.noData(ticker.ticker)
  const pct = (value: number, signed = true) => formatPercent(value, { signed, locale })
  const last = series[series.length - 1]
  const peak = series.reduce((best, p) => (p.cum_return > best.cum_return ? p : best))
  const end = pct(last.cum_return)
  if (peak.cum_return <= 0) return t.neverAbove(ticker.ticker, end)
  // Like a drawdown: the fall measured from the peak, not from the end.
  const givenBack = 1 - (1 + last.cum_return) / (1 + peak.cum_return)
  if (givenBack < NEAR_HIGH) return t.nearHigh(ticker.ticker, end)
  return t.peaked(
    ticker.ticker,
    pct(peak.cum_return),
    formatDate(peak.date, "day", { locale }),
    pct(givenBack, false),
    end,
  )
}
