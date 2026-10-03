import { describe, expect, it } from "vitest"

import {
  highlightedSector,
  labelSides,
  scatterData,
  scatterFinding,
  sectorsLead,
  sortSectors,
} from "./sectors"
import type { SectorRow, SectorsResponse, Ticker } from "./types"

function row(ticker: string, sector: string, total_return: number, extra: Partial<SectorRow> = {}): SectorRow {
  return {
    ticker,
    name: `${sector} SPDR`,
    sector,
    total_return,
    cagr: total_return,
    volatility: 0.2,
    max_drawdown: -0.1,
    sharpe_ratio: 1,
    beta: 1,
    excess_return: total_return - 0.15,
    ...extra,
  }
}

const response: SectorsResponse = {
  period: "1Y",
  start_date: "2025-10-02",
  end_date: "2026-10-02",
  benchmark: { ...row("SPY", "", 0.15), sector: null, name: "SPDR S&P 500 ETF Trust" },
  sectors: [
    row("XLK", "Information Technology", 0.3, { sharpe_ratio: 1.2 }),
    row("XLE", "Energy", 0.4, { sharpe_ratio: 1.6 }),
    row("XLRE", "Real Estate", -0.05, { sharpe_ratio: -0.4, beta: null }),
  ],
}

const tickers = [
  { ticker: "NVDA", asset_type: "stock", sector_etf: "XLK" },
  { ticker: "XLE", asset_type: "sector_etf", sector_etf: "XLE" },
  { ticker: "SPY", asset_type: "benchmark", sector_etf: null },
] as Ticker[]

describe("findings", () => {
  it("leads with the best sector and how many beat the benchmark", () => {
    expect(sectorsLead(response)).toEqual({
      before: "Energy led over the past year with ",
      mark: "+40.0%",
      after: "; 2 of 3 sectors beat SPY.",
    })
  })

  it("titles the scatter with the best and worst Sharpe", () => {
    expect(scatterFinding(response)).toBe(
      "Energy earned the most per unit of risk (Sharpe 1.60); Real Estate the least (−0.40)",
    )
  })
})

describe("highlight", () => {
  it("follows the selected ticker to its sector ETF", () => {
    expect(highlightedSector("NVDA", tickers)).toBe("XLK")
    expect(highlightedSector("XLE", tickers)).toBe("XLE")
    expect(highlightedSector("SPY", tickers)).toBeNull()
  })

  it("marks roles in the scatter", () => {
    const points = scatterData(response, "XLK")
    expect(points.map((p) => [p.ticker, p.role])).toEqual([
      ["XLK", "highlight"],
      ["XLE", "sector"],
      ["XLRE", "sector"],
      ["SPY", "benchmark"],
    ])
  })
})

describe("sortSectors", () => {
  it("sorts numbers and text in both directions", () => {
    const tickersOf = (rows: SectorRow[]) => rows.map((r) => r.ticker)
    expect(tickersOf(sortSectors(response.sectors, "total_return", "descending"))).toEqual(["XLE", "XLK", "XLRE"])
    expect(tickersOf(sortSectors(response.sectors, "total_return", "ascending"))).toEqual(["XLRE", "XLK", "XLE"])
    expect(tickersOf(sortSectors(response.sectors, "sector", "ascending"))).toEqual(["XLE", "XLK", "XLRE"])
  })

  it("keeps missing values last either way", () => {
    expect(sortSectors(response.sectors, "beta", "ascending").at(-1)?.ticker).toBe("XLRE")
    expect(sortSectors(response.sectors, "beta", "descending").at(-1)?.ticker).toBe("XLRE")
  })
})

describe("labelSides", () => {
  it("moves a label left when a neighbor sits just to its right", () => {
    const at = (ticker: string, volatility: number, cagr: number) => ({
      ticker, label: ticker, volatility, cagr, sharpe: null, role: "sector" as const,
    })
    const points = [at("XLE", 0.255, 0.22), at("XLK", 0.26, 0.225), at("XLP", 0.13, 0.06)]
    expect(labelSides(points)).toEqual(["left", "right", "right"])
  })
})
