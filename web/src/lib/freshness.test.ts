import { describe, expect, it } from "vitest"

import { freshness } from "./freshness"
import type { StatusResponse } from "./types"

const status: StatusResponse = {
  last_market_date: "2026-10-02", // a Friday
  last_successful_run_at: "2026-10-02T22:41:00Z",
  last_run_at: "2026-10-02T22:41:00Z",
  last_run_status: "success",
  tickers_expected: 20,
  tickers_current: 20,
}

describe("freshness", () => {
  it("labels the latest close", () => {
    expect(freshness(status, new Date("2026-10-03T12:00:00Z")).label).toBe(
      "Data updated: Oct 2, 2026 market close",
    )
  })

  it("does not call a weekend or a Monday holiday late", () => {
    expect(freshness(status, new Date("2026-10-05T20:00:00Z")).stale).toBe(false)
    expect(freshness(status, new Date("2026-10-06T20:00:00Z")).stale).toBe(false)
  })

  it("flags a missed weekday update", () => {
    const result = freshness(status, new Date("2026-10-08T20:00:00Z"))
    expect(result.stale).toBe(true)
    expect(result.note).toBe("The daily update is late")
  })

  it("mentions tickers that did not update", () => {
    const partial = { ...status, tickers_current: 19 }
    expect(freshness(partial, new Date("2026-10-03T12:00:00Z")).note).toBe(
      "1 of 20 tickers not updated",
    )
  })
})
