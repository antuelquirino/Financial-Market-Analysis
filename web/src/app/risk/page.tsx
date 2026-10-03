import type { Metadata } from "next"
import { Suspense } from "react"

import { ChartSection } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { KpiStrip } from "@/components/market/KpiStrip"
import { MetricLineChart } from "@/components/market/MetricChart"
import { DrawdownChart, ReturnHistogram } from "@/components/market/RiskCharts"
import { Screen } from "@/components/market/Screen"
import { ChartEmpty, ChartError, ChartLoading } from "@/components/market/States"
import { api, loadOrNull } from "@/lib/api"
import { formatDate } from "@/lib/format"
import {
  distributionFinding,
  drawdownFinding,
  histogramData,
  riskKpis,
  riskLead,
  volatilityChartData,
  volatilityFinding,
} from "@/lib/risk"
import type { SearchParams, Selection } from "@/lib/selection"
import { loadShell } from "@/lib/shell"

export const metadata: Metadata = { title: "Risk" }

export const dynamic = "force-dynamic"

export default async function RiskPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>
}) {
  const shell = await loadShell(searchParams)
  return (
    <Screen id="risk" shell={shell}>
      <Suspense
        key={`${shell.selection.ticker}-${shell.selection.period}`}
        fallback={<ChartLoading className="h-96" />}
      >
        <Risk selection={shell.selection} benchmark={shell.benchmark} />
      </Suspense>
    </Screen>
  )
}

async function Risk({ selection, benchmark }: { selection: Selection; benchmark: string }) {
  const [risk, benchmarkPerformance] = await Promise.all([
    loadOrNull(() => api.risk(selection.ticker, selection.period)),
    loadOrNull(() => api.performance(benchmark, selection.period)),
  ])
  if (!risk) return <ChartError message="The risk view could not be loaded." />
  const lead = riskLead(risk)
  const range = `${formatDate(risk.metrics.start_date)} to ${formatDate(risk.metrics.end_date)}`
  const volatility = volatilityChartData(risk)
  const hasVolatility = volatility.some((p) => p["1-month"] !== null)
  return (
    <div className="space-y-12">
      <section aria-label="Summary" className="space-y-6">
        <div>
          <p className="mb-2 text-sm text-graphite">{risk.ticker.name}</p>
          <LeadFinding>
            {lead.before}
            <Mark>{lead.mark}</Mark>
            {lead.after}
          </LeadFinding>
        </div>
        <KpiStrip items={riskKpis(risk, benchmarkPerformance?.metrics ?? null, benchmark)} />
      </section>

      <ChartSection
        finding={drawdownFinding(risk)}
        metric={`Drawdown: distance below the highest close since the period began · ${range}`}
      >
        <DrawdownChart data={risk.drawdown} />
      </ChartSection>

      <div className="grid grid-cols-1 gap-x-12 gap-y-12 xl:grid-cols-2">
        <ChartSection
          finding={volatilityFinding(risk)}
          metric="Annualized volatility of daily returns, over rolling 1-month (21 sessions) and 1-year (252 sessions) windows"
        >
          {hasVolatility ? (
            <MetricLineChart
              data={volatility}
              categories={["1-month", "1-year"]}
              colors={["cobalt", "ochre"]}
              valueFormat="percent"
            />
          ) : (
            <ChartEmpty message="Not enough sessions for rolling volatility." />
          )}
        </ChartSection>

        <ChartSection
          finding={distributionFinding(risk)}
          metric={`Daily returns grouped in half-point ranges · ${risk.return_distribution.sessions.toLocaleString("en-US")} sessions`}
        >
          {risk.return_distribution.bins.length ? (
            <ReturnHistogram data={histogramData(risk)} />
          ) : (
            <ChartEmpty />
          )}
        </ChartSection>
      </div>
    </div>
  )
}
