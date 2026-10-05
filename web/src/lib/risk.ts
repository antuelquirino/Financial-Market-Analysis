// What the Risk screen says. Pure functions over the /risk response.

import type { KpiItem } from "@/components/market/KpiStrip"
import type { ChartDatum } from "./chartUtils"
import {
  formatCount,
  formatDate,
  formatNumber,
  formatPercent,
  formatPoints,
  toneOf,
} from "./format"
import { MESSAGES, type Sentence } from "./i18n"
import type { Locale } from "./locale"
import type { PeriodMetrics, RiskResponse } from "./types"

// Drawdowns closer to zero than this count as "at the peak".
const AT_PEAK = 1e-9

export interface WorstFall {
  peak: string // last session at the high before the fall
  trough: string // the low
  depth: number // drawdown at the trough, <= 0
  recovered: string | null // first session back at the old high
}

/** The deepest peak-to-trough fall in the drawdown series. */
export function worstFall(points: RiskResponse["drawdown"]): WorstFall | null {
  if (!points.length) return null
  let troughIndex = 0
  points.forEach((p, i) => {
    if (p.drawdown < points[troughIndex].drawdown) troughIndex = i
  })
  const trough = points[troughIndex]
  if (trough.drawdown > -AT_PEAK) return null
  let peakIndex = troughIndex
  while (peakIndex > 0 && points[peakIndex].drawdown < -AT_PEAK) peakIndex--
  const recovery = points.slice(troughIndex).find((p) => p.drawdown >= -AT_PEAK)
  return {
    peak: points[peakIndex].date,
    trough: trough.date,
    depth: trough.drawdown,
    recovered: recovery?.date ?? null,
  }
}

/** "NVDA's deepest fall over the past year was −20.2%, from Jan 6 to Mar 30, 2026; it was back at its high by Apr 24, 2026." */
export function riskLead(risk: RiskResponse, locale: Locale = "en"): Sentence {
  const t = MESSAGES[locale].risk
  const date = (iso: string) => formatDate(iso, "day", { locale })
  const symbol = risk.ticker.ticker
  const period = MESSAGES[locale].period.phrase(risk.period, date(risk.metrics.start_date))
  const fall = worstFall(risk.drawdown)
  if (!fall) return t.neverFell(symbol, period)
  return t.lead(
    symbol,
    period,
    formatPercent(fall.depth, { locale }),
    date(fall.peak),
    date(fall.trough),
    fall.recovered ? date(fall.recovered) : null,
  )
}

/** Title of the drawdown chart: where the period ends relative to its peak. */
export function drawdownFinding(risk: RiskResponse, locale: Locale = "en"): string {
  const t = MESSAGES[locale].risk
  const last = risk.drawdown[risk.drawdown.length - 1]
  const symbol = risk.ticker.ticker
  if (!last) return t.noData(symbol)
  if (last.drawdown > -0.005) return t.atHigh(symbol)
  return t.belowPeak(symbol, formatPercent(-last.drawdown, { locale }))
}

/** Series names are shown in the legend and tooltip, so they are translated. */
export function volatilityChartData(risk: RiskResponse, locale: Locale = "en"): ChartDatum[] {
  const t = MESSAGES[locale].risk
  return risk.rolling_volatility.map((p) => ({
    date: p.date,
    [t.oneMonth]: p.volatility_1m,
    [t.oneYear]: p.volatility_1y,
  }))
}

/** Title of the volatility chart: the short-term peak and where it stands now. */
export function volatilityFinding(risk: RiskResponse, locale: Locale = "en"): string {
  const t = MESSAGES[locale].risk
  const points = risk.rolling_volatility.filter((p) => p.volatility_1m !== null)
  if (!points.length) return t.notEnoughSessions
  const peak = points.reduce((best, p) => (p.volatility_1m! > best.volatility_1m! ? p : best))
  const now = points[points.length - 1]
  const pct = (value: number | null) => formatPercent(value, { decimals: 0, locale })
  return t.volatilityPeak(pct(peak.volatility_1m), formatDate(peak.date, "day", { locale }), pct(now.volatility_1m))
}

/** Title of the histogram: how often the ticker lost money, and its worst day. */
export function distributionFinding(risk: RiskResponse, locale: Locale = "en"): string {
  const t = MESSAGES[locale].risk
  const d = risk.return_distribution
  const symbol = risk.ticker.ticker
  if (!d.sessions || !d.worst_day) return t.noReturns(symbol)
  return t.distribution(
    symbol,
    formatPercent(d.share_negative, { decimals: 0, locale }),
    formatPercent(d.worst_day.daily_return, { locale }),
    formatDate(d.worst_day.date, "day", { locale }),
  )
}

export interface HistogramBar {
  label: string // "−1.0% to −0.5%"
  center: number
  sessions: number
  sessionsLabel: string // "31 sessions"
}

export function histogramData(risk: RiskResponse, locale: Locale = "en"): HistogramBar[] {
  const t = MESSAGES[locale].risk
  const pct = (value: number) => formatPercent(value, { decimals: 1, locale })
  return risk.return_distribution.bins.map((b) => ({
    label: t.binLabel(pct(b.lower), pct(b.upper)),
    center: (b.lower + b.upper) / 2,
    sessions: b.sessions,
    sessionsLabel: t.sessions(b.sessions, formatCount(b.sessions, { locale })),
  }))
}

/** Risk KPIs. Volatility and drawdown are compared with the benchmark over the same dates. */
export function riskKpis(
  risk: RiskResponse,
  benchmark: PeriodMetrics | null,
  benchmarkSymbol: string,
  locale: Locale = "en",
): KpiItem[] {
  const k = MESSAGES[locale].kpi
  const m = risk.metrics
  const self = risk.ticker.ticker === benchmarkSymbol
  const b = self ? null : benchmark
  const compare = (value: number | null, other: number | null | undefined, higherIsBetter: boolean) => {
    if (value === null || other === null || other === undefined) return undefined
    const difference = value - other
    return {
      text: formatPoints(difference, { locale }),
      direction: (difference > 0 ? "up" : difference < 0 ? "down" : "flat") as "up" | "down" | "flat",
      tone: toneOf(difference, higherIsBetter),
      comparison: k.versus(benchmarkSymbol),
    }
  }
  return [
    {
      label: k.volatility,
      value: formatPercent(m.volatility, { locale }),
      change: compare(m.volatility, b?.volatility, false),
      note: k.annualized,
    },
    {
      label: k.maxDrawdown,
      value: formatPercent(m.max_drawdown, { locale }),
      change: compare(m.max_drawdown, b?.max_drawdown, true),
      note: self ? k.theBenchmark : k.peakToTrough,
    },
    {
      label: k.sharpe,
      value: formatNumber(m.sharpe_ratio, { locale }),
      note: k.perUnitOfRisk,
    },
    {
      label: k.beta,
      value: formatNumber(self ? 1 : m.beta, { locale }),
      note: k.betaNote(benchmarkSymbol),
    },
    {
      label: k.losingSessions,
      value: formatPercent(risk.return_distribution.share_negative, { decimals: 0, locale }),
      note: k.ofSessions(formatCount(risk.return_distribution.sessions, { locale })),
    },
  ]
}

/** Round ticks from 0 down past the deepest value: every 5% for shallow falls, 10% or 20% for deep ones. */
export function drawdownTicks(deepest: number): number[] {
  const depth = Math.max(-deepest, 0.01)
  const step = depth <= 0.3 ? 0.05 : depth <= 0.6 ? 0.1 : 0.2
  const count = Math.ceil(depth / step - 1e-9)
  return Array.from({ length: count + 1 }, (_, i) => (i === 0 ? 0 : -Math.round(i * step * 100) / 100))
}
