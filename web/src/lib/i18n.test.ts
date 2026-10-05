import { describe, expect, it } from "vitest"

import { freshness } from "./freshness"
import { MESSAGES, sectorName } from "./i18n"
import { localePath } from "./locale"
import { overviewKpis, overviewLead, performanceFinding } from "./overview"
import { distributionFinding, riskLead } from "./risk"
import { hrefFor } from "./selection"
import { scatterFinding, sectorsLead, sortSectors } from "./sectors"
import type {
  PerformanceResponse,
  PeriodMetrics,
  RiskResponse,
  SectorRow,
  SectorsResponse,
} from "./types"

const es = "es" as const

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
  benchmark_total_return: 0.162,
  excess_return: 0.0795,
}

const performance: PerformanceResponse = {
  ticker: { ticker: "NVDA", name: "NVIDIA Corporation" },
  benchmark: { ticker: "SPY", name: "SPDR S&P 500 ETF Trust" },
  period: "1Y",
  metrics,
  series: [
    { date: "2025-10-02", cum_return: 0, benchmark_cum_return: 0 },
    { date: "2026-03-02", cum_return: 0.5, benchmark_cum_return: 0.1 },
    { date: "2026-10-02", cum_return: 0.2415, benchmark_cum_return: 0.162 },
  ],
}

describe("both languages define the same words", () => {
  it("has the same keys in English and Spanish", () => {
    const keys = (value: object): string[] =>
      Object.entries(value).flatMap(([key, v]) =>
        v && typeof v === "object" && !Array.isArray(v) && key !== "sectorNames"
          ? keys(v).map((k) => `${key}.${k}`)
          : [key],
      )
    expect(keys(MESSAGES.es).sort()).toEqual(keys(MESSAGES.en).sort())
  })
})

describe("Overview in Spanish", () => {
  it("writes the lead with Argentine formats", () => {
    expect(overviewLead(performance, es)).toEqual({
      before: "NVDA rindió ",
      mark: "+24,2%",
      after: " en el último año, 8,0 pp por encima de SPY.",
    })
  })

  it("names the start date for the whole history", () => {
    const all = { ...performance, period: "MAX" as const, metrics: { ...metrics, start_date: "2021-01-04" } }
    expect(overviewLead(all, es).after).toBe(" desde el 4 de ene de 2021, 8,0 pp por encima de SPY.")
  })

  it("titles the chart and labels the KPIs", () => {
    expect(performanceFinding(performance, es)).toBe(
      "NVDA llegó a +50,0% el 2 de mar de 2026 y después cayó 17,2%, hasta +24,2%",
    )
    const [ret, , vol] = overviewKpis(performance, { ...metrics, volatility: 0.13 }, es)
    expect(ret).toMatchObject({ label: "Rendimiento", value: "+24,2%" })
    expect(vol.change).toMatchObject({ text: "+24,8 pp", comparison: "vs SPY" })
  })
})

describe("Risk in Spanish", () => {
  const risk: RiskResponse = {
    ticker: { ticker: "NVDA", name: "NVIDIA Corporation" },
    period: "1Y",
    metrics,
    drawdown: [
      { date: "2026-01-02", drawdown: 0 },
      { date: "2026-01-05", drawdown: -0.2 },
      { date: "2026-01-07", drawdown: 0 },
    ],
    rolling_volatility: [],
    return_distribution: {
      bin_width: 0.005,
      bins: [],
      sessions: 251,
      mean: 0,
      std: 0.02,
      share_negative: 0.49,
      worst_day: { date: "2026-06-05", daily_return: -0.062 },
      best_day: null,
    },
  }

  it("describes the deepest fall and the worst day", () => {
    expect(riskLead(risk, es)).toEqual({
      before: "La peor caída de NVDA en el último año fue de ",
      mark: "−20,0%",
      after: ", del 2 de ene de 2026 al 5 de ene de 2026; volvió a su máximo el 7 de ene de 2026.",
    })
    expect(distributionFinding(risk, es)).toBe(
      "NVDA bajó en el 49% de las ruedas; su peor día fue −6,2%, el 5 de jun de 2026",
    )
  })
})

describe("Sectors in Spanish", () => {
  const row = (ticker: string, sector: string, total_return: number, sharpe_ratio: number): SectorRow => ({
    ticker, name: `${sector} SPDR`, sector, total_return, cagr: total_return, volatility: 0.2,
    max_drawdown: -0.1, sharpe_ratio, beta: 1, excess_return: total_return - 0.16,
  })
  const sectors: SectorsResponse = {
    period: "1Y",
    start_date: "2025-10-02",
    end_date: "2026-10-02",
    benchmark: { ...row("SPY", "", 0.16, 0.9), sector: null },
    sectors: [row("XLE", "Energy", 0.46, 1.67), row("XLU", "Utilities", -0.07, -0.65), row("XLK", "Information Technology", 0.4, 1.2)],
  }

  it("translates sector names in sentences", () => {
    expect(sectorsLead(sectors, es)).toEqual({
      before: "Energía lideró en el último año con ",
      mark: "+46,0%",
      after: "; 2 de 3 sectores le ganaron a SPY.",
    })
    expect(scatterFinding(sectors, es)).toBe(
      "Energía ganó más por unidad de riesgo (Sharpe 1,67); Servicios públicos, menos (−0,65)",
    )
  })

  it("sorts sector names in the reader's language", () => {
    const names = (rows: SectorRow[], locale: "en" | "es") => rows.map((r) => sectorName(r.sector, locale))
    expect(names(sortSectors(sectors.sectors, "sector", "ascending", es), es)).toEqual([
      "Energía", "Servicios públicos", "Tecnología de la información",
    ])
    expect(names(sortSectors(sectors.sectors, "sector", "ascending"), "en")).toEqual([
      "Energy", "Information Technology", "Utilities",
    ])
  })
})

describe("paths and freshness", () => {
  it("prefixes Spanish paths and keeps the selection", () => {
    expect(localePath("es", "/")).toBe("/es")
    expect(localePath("es", "/risk")).toBe("/es/risk")
    expect(localePath("en", "/risk")).toBe("/risk")
    expect(hrefFor(localePath("es", "/sectors"), { ticker: "XLE", period: "5Y" })).toBe(
      "/es/sectors?ticker=XLE&period=5Y",
    )
  })

  it("says when the data was updated", () => {
    const status = {
      last_market_date: "2026-10-02",
      last_successful_run_at: null,
      last_run_at: null,
      last_run_status: null,
      tickers_expected: 20,
      tickers_current: 19,
    }
    const info = freshness(status, new Date("2026-10-03T12:00:00Z"), es)
    expect(info.label).toBe("Datos al cierre del 2 de oct de 2026")
    expect(info.note).toBe("1 de 20 tickers sin actualizar")
  })
})
