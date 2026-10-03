// Data every screen needs: the ticker list, the pipeline status and the
// selection parsed from the URL. Each part fails on its own.

import { api, loadOrNull } from "./api"
import { parsePeriod, parseTicker, type SearchParams, type Selection } from "./selection"
import type { StatusResponse, Ticker } from "./types"

export interface Shell {
  selection: Selection
  tickers: Ticker[]
  benchmark: string
  status: StatusResponse | null
}

export async function loadShell(searchParams: Promise<SearchParams>): Promise<Shell> {
  const [params, tickers, status] = await Promise.all([
    searchParams,
    loadOrNull(() => api.tickers()),
    loadOrNull(() => api.status()),
  ])
  const list = tickers?.tickers ?? []
  return {
    selection: {
      ticker: parseTicker(params.ticker, list),
      period: parsePeriod(params.period),
    },
    tickers: list,
    benchmark: tickers?.benchmark ?? "SPY",
    status,
  }
}
