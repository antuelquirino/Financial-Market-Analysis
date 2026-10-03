import { describe, expect, it } from "vitest"

import {
  overviewKpis,
  overviewLead,
  performanceChartData,
  performanceFinding,
} from "./overview"
import type { PerformanceResponse, PeriodMetrics } from "./types"

const metrics: PeriodMetrics = {
  start_date: "2025-10-02",
  end_date: "2026-10-02",
  sessions: 252,
  has_full_history: true,
  total_return: 0.2415,
  cagr: 0.242,
  volatility: 0.378,
  max_drawdown: -0.202,
  sharpe_ratio: 0.66,
  beta: 1.9,
  benchmark_total_return: 0.21,
  excess_return: 0.0315,
}

const spy: PeriodMetrics = {
  ...metrics,
  total_return: 0.21,
  volatility: 0.15,
  max_drawdown: -0.1,
  sharpe_ratio: 1.1,
  excess_return: 0,
}

function response(overrides: Partial<PerformanceResponse> = {}): PerformanceResponse {
  return {
    ticker: { ticker: "NVDA", name: "NVIDIA Corporation" },
    benchmark: { ticker: "SPY", name: "SPDR S&P 500 ETF Trust" },
    period: "1Y",
    metrics,
    series: [
      { date: "2025-10-02", cum_return: 0, benchmark_cum_return: 0 },
      { date: "2026-03-02", cum_return: 0.5, benchmark_cum_return: 0.1 },
      { date: "2026-10-02", cum_return: 0.2415, benchmark_cum_return: 0.21 },
    ],
    ...overrides,
  }
}

describe("overviewLead", () => {
  it("compares with the benchmark", () => {
    expect(overviewLead(response())).toEqual({
      before: "NVDA returned ",
      mark: "+24.2%",
      after: " over the past year, 3.2 pts ahead of SPY.",
    })
  })

  it("says behind when the ticker lagged", () => {
    const lagging = response({ metrics: { ...metrics, total_return: 0.1, excess_return: -0.11 } })
    expect(overviewLead(lagging).after).toBe(" over the past year, 11.0 pts behind SPY.")
  })

  it("names the start date for the whole history", () => {
    const all = response({ period: "MAX", metrics: { ...metrics, start_date: "2021-01-04" } })
    expect(overviewLead(all).after).toBe(" since Jan 4, 2021, 3.2 pts ahead of SPY.")
  })

  it("does not compare the benchmark with itself", () => {
    const self = response({ ticker: { ticker: "SPY", name: "SPDR S&P 500 ETF Trust" } })
    expect(overviewLead(self).after).toBe(" over the past year.")
  })
})

describe("overviewKpis", () => {
  it("colors each difference by whether it is good news", () => {
    const [ret, cagr, vol, dd, sharpe] = overviewKpis(response(), spy)
    expect(ret.change).toMatchObject({ text: "+3.2 pts", tone: "gain", direction: "up" })
    expect(cagr.note).toBe("CAGR")
    expect(vol.change).toMatchObject({ text: "+22.8 pts", tone: "loss", direction: "up" })
    expect(dd.change).toMatchObject({ text: "−10.2 pts", tone: "loss", direction: "down" })
    expect(sharpe.change).toMatchObject({ text: "−0.44", tone: "loss", comparison: "vs SPY" })
  })

  it("shows no comparison for the benchmark itself", () => {
    const self = response({ ticker: { ticker: "SPY", name: "SPY" } })
    expect(overviewKpis(self, spy).every((kpi) => kpi.change === undefined)).toBe(true)
  })
})

describe("performance chart", () => {
  it("keys both series by symbol", () => {
    expect(performanceChartData(response())[1]).toEqual({
      date: "2026-03-02",
      NVDA: 0.5,
      SPY: 0.1,
    })
  })

  it("titles the chart with the path's peak", () => {
    expect(performanceFinding(response())).toBe(
      "NVDA peaked at +50.0% on Mar 2, 2026, then fell 17.2% to +24.2%",
    )
  })

  it("says when the period ended near its high", () => {
    const steady = response({
      series: [
        { date: "2025-10-02", cum_return: 0, benchmark_cum_return: 0 },
        { date: "2026-10-02", cum_return: 0.24, benchmark_cum_return: 0.2 },
      ],
    })
    expect(performanceFinding(steady)).toBe("NVDA ended near its high for the period, at +24.0%")
  })
})
