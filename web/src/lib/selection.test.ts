import { describe, expect, it } from "vitest"

import { hrefFor, parsePeriod, parseTicker } from "./selection"

const tickers = [{ ticker: "NVDA" }, { ticker: "XLK" }, { ticker: "^GSPC" }]

describe("parsing the URL", () => {
  it("accepts known values in any case and falls back otherwise", () => {
    expect(parsePeriod("3y")).toBe("3Y")
    expect(parsePeriod(["MAX", "1Y"])).toBe("MAX")
    expect(parsePeriod("2Y")).toBe("1Y")
    expect(parsePeriod(undefined)).toBe("1Y")
    expect(parseTicker("xlk", tickers)).toBe("XLK")
    expect(parseTicker("^gspc", tickers)).toBe("^GSPC")
    expect(parseTicker("ZZZ", tickers)).toBe("NVDA")
  })
})

describe("hrefFor", () => {
  const selection = { ticker: "XLK", period: "3Y" as const }

  it("keeps the selection and applies changes", () => {
    expect(hrefFor("/risk", selection)).toBe("/risk?ticker=XLK&period=3Y")
    expect(hrefFor("/", selection, { period: "MAX" })).toBe(
      "/?ticker=XLK&period=MAX",
    )
  })

  it("leaves defaults out of the URL and encodes symbols", () => {
    expect(hrefFor("/", { ticker: "NVDA", period: "1Y" })).toBe("/")
    expect(hrefFor("/", selection, { ticker: "^GSPC" })).toBe(
      "/?ticker=%5EGSPC&period=3Y",
    )
  })
})
