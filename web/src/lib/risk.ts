// What the Risk screen says. Pure functions over the /risk response.

import type { KpiItem } from "@/components/market/KpiStrip"
import type { ChartDatum } from "./chartUtils"
import { formatDate, formatNumber, formatPercent, formatPoints, toneOf } from "./format"
import type { Sentence } from "./overview"
import { periodPhrase } from "./selection"
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
export function riskLead(risk: RiskResponse): Sentence {
  const symbol = risk.ticker.ticker
  const period = periodPhrase(risk.period, formatDate(risk.metrics.start_date))
  const fall = worstFall(risk.drawdown)
  if (!fall) {
    return {
      before: `${symbol} never fell below a previous high `,
      mark: period,
      after: ".",
    }
  }
  const recovery = fall.recovered
    ? `it was back at its high by ${formatDate(fall.recovered)}`
    : "it has not recovered yet"
  return {
    before: `${symbol}’s deepest fall ${period} was `,
    mark: formatPercent(fall.depth),
    after: `, from ${formatDate(fall.peak)} to ${formatDate(fall.trough)}; ${recovery}.`,
  }
}

/** Title of the drawdown chart: where the period ends relative to its peak. */
export function drawdownFinding(risk: RiskResponse): string {
  const last = risk.drawdown[risk.drawdown.length - 1]
  const symbol = risk.ticker.ticker
  if (!last) return `${symbol} has no data for this period`
  if (last.drawdown > -0.005) return `${symbol} ends the period at or near its high`
  return `${symbol} ends the period ${formatPercent(-last.drawdown)} below its peak`
}

export function volatilityChartData(risk: RiskResponse): ChartDatum[] {
  return risk.rolling_volatility.map((p) => ({
    date: p.date,
    "1-month": p.volatility_1m,
    "1-year": p.volatility_1y,
  }))
}

/** Title of the volatility chart: the short-term peak and where it stands now. */
export function volatilityFinding(risk: RiskResponse): string {
  const points = risk.rolling_volatility.filter((p) => p.volatility_1m !== null)
  if (!points.length) return "Not enough sessions to measure volatility"
  const peak = points.reduce((best, p) => (p.volatility_1m! > best.volatility_1m! ? p : best))
  const now = points[points.length - 1]
  return `Short-term volatility peaked at ${formatPercent(peak.volatility_1m, { decimals: 0 })} on ${formatDate(peak.date)}; it is ${formatPercent(now.volatility_1m, { decimals: 0 })} now`
}

/** Title of the histogram: how often the ticker lost money, and its worst day. */
export function distributionFinding(risk: RiskResponse): string {
  const d = risk.return_distribution
  const symbol = risk.ticker.ticker
  if (!d.sessions || !d.worst_day) return `${symbol} has no daily returns in this period`
  return `${symbol} fell on ${formatPercent(d.share_negative, { decimals: 0 })} of sessions; its worst day was ${formatPercent(d.worst_day.daily_return)} on ${formatDate(d.worst_day.date)}`
}

export interface HistogramBar {
  label: string // "−1.0% to −0.5%"
  center: number
  sessions: number
}

export function histogramData(risk: RiskResponse): HistogramBar[] {
  return risk.return_distribution.bins.map((b) => ({
    label: `${formatPercent(b.lower, { decimals: 1 })} to ${formatPercent(b.upper, { decimals: 1 })}`,
    center: (b.lower + b.upper) / 2,
    sessions: b.sessions,
  }))
}

/** Risk KPIs. Volatility and drawdown are compared with the benchmark over the same dates. */
export function riskKpis(
  risk: RiskResponse,
  benchmark: PeriodMetrics | null,
  benchmarkSymbol: string,
): KpiItem[] {
  const m = risk.metrics
  const self = risk.ticker.ticker === benchmarkSymbol
  const b = self ? null : benchmark
  const compare = (value: number | null, other: number | null | undefined, higherIsBetter: boolean) => {
    if (value === null || other === null || other === undefined) return undefined
    const difference = value - other
    return {
      text: formatPoints(difference),
      direction: (difference > 0 ? "up" : difference < 0 ? "down" : "flat") as "up" | "down" | "flat",
      tone: toneOf(difference, higherIsBetter),
      comparison: `vs ${benchmarkSymbol}`,
    }
  }
  return [
    {
      label: "Volatility",
      value: formatPercent(m.volatility),
      change: compare(m.volatility, b?.volatility, false),
      note: "Annualized",
    },
    {
      label: "Max drawdown",
      value: formatPercent(m.max_drawdown),
      change: compare(m.max_drawdown, b?.max_drawdown, true),
      note: self ? "The benchmark" : "Peak to trough",
    },
    {
      label: "Sharpe ratio",
      value: formatNumber(m.sharpe_ratio),
      note: "Return per unit of risk",
    },
    {
      label: "Beta",
      value: self ? "1.00" : formatNumber(m.beta),
      note: `Moves vs ${benchmarkSymbol} (1.00 = same)`,
    },
    {
      label: "Losing sessions",
      value: formatPercent(risk.return_distribution.share_negative, { decimals: 0 }),
      note: `of ${risk.return_distribution.sessions.toLocaleString("en-US")} sessions`,
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
