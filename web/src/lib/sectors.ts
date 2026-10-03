// What the Sectors screen says, and how its table sorts. Pure functions.

import { formatDate, formatNumber, formatPercent } from "./format"
import type { Sentence } from "./overview"
import { periodPhrase } from "./selection"
import type { SectorRow, SectorsResponse, Ticker } from "./types"

/** "Energy led over the past year with +40.2%; 4 of 11 sectors beat SPY." */
export function sectorsLead(response: SectorsResponse): Sentence {
  const ranked = [...response.sectors].sort((a, b) => b.total_return - a.total_return)
  const best = ranked[0]
  const beat = ranked.filter((s) => s.total_return > response.benchmark.total_return).length
  const period = periodPhrase(response.period, formatDate(response.start_date))
  return {
    before: `${best.sector ?? best.ticker} led ${period} with `,
    mark: formatPercent(best.total_return, { signed: true }),
    after: `; ${beat} of ${ranked.length} sectors beat ${response.benchmark.ticker}.`,
  }
}

/** Title of the scatter: the best return per unit of risk. */
export function scatterFinding(response: SectorsResponse): string {
  const withSharpe = response.sectors.filter((s) => s.sharpe_ratio !== null)
  if (!withSharpe.length) return "Not enough data to compare risk and return"
  const best = withSharpe.reduce((a, b) => (b.sharpe_ratio! > a.sharpe_ratio! ? b : a))
  const worst = withSharpe.reduce((a, b) => (b.sharpe_ratio! < a.sharpe_ratio! ? b : a))
  return `${best.sector} earned the most per unit of risk (Sharpe ${formatNumber(best.sharpe_ratio)}); ${worst.sector} the least (${formatNumber(worst.sharpe_ratio)})`
}

/** The sector ETF to highlight for the selected ticker: itself, its sector's ETF, or none. */
export function highlightedSector(selected: string, tickers: Ticker[]): string | null {
  const ticker = tickers.find((t) => t.ticker === selected)
  if (!ticker || ticker.asset_type === "benchmark") return null
  return ticker.sector_etf
}

export interface ScatterPoint {
  ticker: string
  label: string // sector name
  volatility: number
  cagr: number
  sharpe: number | null
  role: "sector" | "highlight" | "benchmark"
}

export function scatterData(response: SectorsResponse, highlight: string | null): ScatterPoint[] {
  const point = (row: SectorRow, role: ScatterPoint["role"]): ScatterPoint | null =>
    row.volatility === null || row.cagr === null
      ? null
      : {
          ticker: row.ticker,
          label: row.sector ?? row.name,
          volatility: row.volatility,
          cagr: row.cagr,
          sharpe: row.sharpe_ratio,
          role,
        }
  return [
    ...response.sectors.map((s) => point(s, s.ticker === highlight ? "highlight" : "sector")),
    point(response.benchmark, "benchmark"),
  ].filter((p): p is ScatterPoint => p !== null)
}

export type SortKey =
  | "sector"
  | "total_return"
  | "cagr"
  | "volatility"
  | "max_drawdown"
  | "sharpe_ratio"
  | "beta"
  | "excess_return"

export type SortDirection = "ascending" | "descending"

/** Sorted copy; nulls always last. Text sorts A→Z first, numbers high→low first. */
export function sortSectors(rows: SectorRow[], key: SortKey, direction: SortDirection): SectorRow[] {
  const value = (row: SectorRow) => (key === "sector" ? (row.sector ?? row.name) : row[key])
  const sign = direction === "ascending" ? 1 : -1
  return [...rows].sort((a, b) => {
    const x = value(a)
    const y = value(b)
    if (x === null) return 1
    if (y === null) return -1
    if (typeof x === "string" && typeof y === "string") return sign * x.localeCompare(y)
    return sign * ((x as number) - (y as number))
  })
}

export const defaultDirection = (key: SortKey): SortDirection =>
  key === "sector" ? "ascending" : "descending"

/**
 * Which side each scatter label goes on: left when another dot sits just to
 * the right at about the same height, so a label never runs into a neighbor.
 * Distances are fractions of each axis's range.
 */
export function labelSides(points: ScatterPoint[]): ("left" | "right")[] {
  const span = (values: number[]) => Math.max(...values) - Math.min(...values) || 1
  const xSpan = span(points.map((p) => p.volatility))
  const ySpan = span(points.map((p) => p.cagr))
  return points.map((p) =>
    points.some((q) => {
      const dx = (q.volatility - p.volatility) / xSpan
      const dy = Math.abs(q.cagr - p.cagr) / ySpan
      return q !== p && dx > 0 && dx < 0.08 && dy < 0.06
    })
      ? "left"
      : "right",
  )
}
