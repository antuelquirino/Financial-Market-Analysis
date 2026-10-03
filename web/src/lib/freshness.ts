// "Data updated: Sep 29, 2026 market close", and whether that is late.

import { formatDate } from "./format"
import type { StatusResponse } from "./types"

// A Friday close is 3 days old on Monday, 4 after a Monday holiday. Beyond
// that, a weekday run was missed.
const MAX_AGE_DAYS = 4

export interface Freshness {
  label: string
  stale: boolean
  note: string | null
}

export function freshness(status: StatusResponse, today: Date = new Date()): Freshness {
  const last = Date.parse(`${status.last_market_date}T00:00:00Z`)
  const ageDays = (today.getTime() - last) / 864e5
  const stale = ageDays > MAX_AGE_DAYS + 1 // +1: the close is in the evening
  const missing = status.tickers_expected - status.tickers_current
  return {
    label: `Data updated: ${formatDate(status.last_market_date)} market close`,
    stale,
    note: stale
      ? "The daily update is late"
      : missing > 0
        ? `${missing} of ${status.tickers_expected} tickers not updated`
        : null,
  }
}
