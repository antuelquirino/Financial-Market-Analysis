// The global selection lives in the URL (?ticker=NVDA&period=3Y), so every view
// can be reloaded and shared as a link. Anything invalid falls back to a default.

import { PERIODS, type Period, type Ticker } from "./types"

export const DEFAULT_TICKER = "NVDA"
export const DEFAULT_PERIOD: Period = "1Y"

export type SearchParams = Record<string, string | string[] | undefined>

export interface Selection {
  ticker: string
  period: Period
}

const first = (value: string | string[] | undefined) =>
  Array.isArray(value) ? value[0] : value

export function parsePeriod(value: string | string[] | undefined): Period {
  const period = first(value)?.toUpperCase()
  return PERIODS.find((p) => p === period) ?? DEFAULT_PERIOD
}

/** The ticker as listed by the API (case-insensitive match), or the default. */
export function parseTicker(
  value: string | string[] | undefined,
  tickers: Pick<Ticker, "ticker">[],
): string {
  const wanted = first(value)?.toUpperCase()
  const match = tickers.find((t) => t.ticker.toUpperCase() === wanted)
  return match?.ticker ?? DEFAULT_TICKER
}

/** A link to `path` keeping the selection, with `changes` applied. Defaults stay out of the URL. */
export function hrefFor(
  path: string,
  selection: Selection,
  changes: Partial<Selection> = {},
): string {
  const next = { ...selection, ...changes }
  const params = new URLSearchParams()
  if (next.ticker !== DEFAULT_TICKER) params.set("ticker", next.ticker)
  if (next.period !== DEFAULT_PERIOD) params.set("period", next.period)
  const query = params.toString()
  return query ? `${path}?${query}` : path
}
