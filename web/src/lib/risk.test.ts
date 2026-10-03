import { describe, expect, it } from "vitest"

import {
  distributionFinding,
  drawdownFinding,
  drawdownTicks,
  histogramData,
  riskKpis,
  riskLead,
  volatilityFinding,
  worstFall,
} from "./risk"
import type { PeriodMetrics, RiskResponse } from "./types"

const metrics: PeriodMetrics = {
  start_date: "2026-01-02",
  end_date: "2026-01-09",
  sessions: 6,
  has_full_history: true,
  total_return: 0.05,
  cagr: null,
  volatility: 0.4,
  max_drawdown: -0.2,
  sharpe_ratio: 0.8,
  beta: 1.5,
  benchmark_total_return: 0.02,
  excess_return: 0.03,
}

const drawdown = (values: number[]) =>
  values.map((drawdown, i) => ({ date: `2026-01-0${i + 2}`, drawdown }))

function risk(overrides: Partial<RiskResponse> = {}): RiskResponse {
  return {
    ticker: { ticker: "NVDA", name: "NVIDIA Corporation" },
    period: "1Y",
    metrics,
    drawdown: drawdown([0, 0, -0.1, -0.2, -0.05, 0]),
    rolling_volatility: [
      { date: "2026-01-02", volatility_1m: null, volatility_1y: null },
      { date: "2026-01-05", volatility_1m: 0.85, volatility_1y: 0.4 },
      { date: "2026-01-09", volatility_1m: 0.31, volatility_1y: 0.41 },
    ],
    return_distribution: {
      bin_width: 0.005,
      bins: [
        { lower: -0.01, upper: -0.005, sessions: 2 },
        { lower: -0.005, upper: 0, sessions: 1 },
        { lower: 0, upper: 0.005, sessions: 2 },
      ],
      sessions: 5,
      mean: 0.001,
      std: 0.02,
      share_negative: 0.6,
      worst_day: { date: "2026-01-05", daily_return: -0.0972 },
      best_day: { date: "2026-01-08", daily_return: 0.05 },
    },
    ...overrides,
  }
}

describe("worstFall", () => {
  it("finds the peak before the trough and the recovery after it", () => {
    expect(worstFall(drawdown([0, 0, -0.1, -0.2, -0.05, 0]))).toEqual({
      peak: "2026-01-03",
      trough: "2026-01-05",
      depth: -0.2,
      recovered: "2026-01-07",
    })
  })

  it("reports no recovery when the period ends below the peak", () => {
    expect(worstFall(drawdown([0, -0.1, -0.3, -0.2]))?.recovered).toBeNull()
  })

  it("returns null when the price only went up", () => {
    expect(worstFall(drawdown([0, 0, 0]))).toBeNull()
  })
})

describe("findings", () => {
  it("leads with the deepest fall", () => {
    expect(riskLead(risk())).toEqual({
      before: "NVDA’s deepest fall over the past year was ",
      mark: "−20.0%",
      after: ", from Jan 3, 2026 to Jan 5, 2026; it was back at its high by Jan 7, 2026.",
    })
  })

  it("titles each chart with what it shows", () => {
    expect(drawdownFinding(risk())).toBe("NVDA ends the period at or near its high")
    expect(drawdownFinding(risk({ drawdown: drawdown([0, -0.042]) }))).toBe(
      "NVDA ends the period 4.2% below its peak",
    )
    expect(volatilityFinding(risk())).toBe(
      "Short-term volatility peaked at 85% on Jan 5, 2026; it is 31% now",
    )
    expect(distributionFinding(risk())).toBe(
      "NVDA fell on 60% of sessions; its worst day was −9.7% on Jan 5, 2026",
    )
  })

  it("labels histogram bars by their range", () => {
    expect(histogramData(risk())[0]).toEqual({
      label: "−1.0% to −0.5%",
      center: -0.0075,
      sessions: 2,
    })
  })
})

describe("riskKpis", () => {
  const spy = { ...metrics, volatility: 0.15, max_drawdown: -0.1 }

  it("compares volatility and drawdown with the benchmark", () => {
    const [vol, dd, , beta, losing] = riskKpis(risk(), spy, "SPY")
    expect(vol.change).toMatchObject({ text: "+25.0 pts", tone: "loss" })
    expect(dd.change).toMatchObject({ text: "−10.0 pts", tone: "loss" })
    expect(beta.value).toBe("1.50")
    expect(losing).toMatchObject({ value: "60%", note: "of 5 sessions" })
  })

  it("has no comparison for the benchmark itself", () => {
    const self = risk({ ticker: { ticker: "SPY", name: "SPY" } })
    const [vol, , , beta] = riskKpis(self, spy, "SPY")
    expect(vol.change).toBeUndefined()
    expect(beta.value).toBe("1.00")
  })
})

describe("drawdown axis", () => {
  it("uses round steps that cover the deepest fall", () => {
    expect(drawdownTicks(-0.202)).toEqual([0, -0.05, -0.1, -0.15, -0.2, -0.25])
    expect(drawdownTicks(-0.663)).toEqual([0, -0.2, -0.4, -0.6, -0.8])
    expect(drawdownTicks(0)).toEqual([0, -0.05])
  })
})
