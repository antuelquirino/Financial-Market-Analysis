import { Suspense } from "react"

import { ChartSection } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { KpiStrip } from "@/components/market/KpiStrip"
import { MetricLineChart } from "@/components/market/MetricChart"
import { Screen } from "@/components/market/Screen"
import { ChartError, ChartLoading } from "@/components/market/States"
import { api, loadOrNull } from "@/lib/api"
import { formatDate } from "@/lib/format"
import {
  overviewKpis,
  overviewLead,
  performanceChartData,
  performanceFinding,
} from "@/lib/overview"
import type { SearchParams, Selection } from "@/lib/selection"
import { loadShell } from "@/lib/shell"

// Rendered per request: the build may run without the API, and a build-time
// error state would be cached. API responses themselves stay cached.
export const dynamic = "force-dynamic"

export default async function OverviewPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>
}) {
  const shell = await loadShell(searchParams)
  return (
    <Screen id="overview" shell={shell}>
      <Suspense
        key={`${shell.selection.ticker}-${shell.selection.period}`}
        fallback={<ChartLoading className="h-96" />}
      >
        <Overview selection={shell.selection} benchmark={shell.benchmark} />
      </Suspense>
    </Screen>
  )
}

async function Overview({
  selection,
  benchmark,
}: {
  selection: Selection
  benchmark: string
}) {
  const [performance, benchmarkPerformance] = await Promise.all([
    loadOrNull(() => api.performance(selection.ticker, selection.period)),
    loadOrNull(() => api.performance(benchmark, selection.period)),
  ])
  if (!performance) {
    return <ChartError message="The overview could not be loaded." />
  }
  const lead = overviewLead(performance)
  const { ticker, benchmark: bench, metrics } = performance
  return (
    <div className="space-y-12">
      <section aria-label="Summary" className="space-y-6">
        <div>
          <p className="mb-2 text-sm text-graphite">{ticker.name}</p>
          <LeadFinding>
            {lead.before}
            <Mark>{lead.mark}</Mark>
            {lead.after}
          </LeadFinding>
          {!metrics.has_full_history ? (
            <p className="mt-2 text-sm text-muted">
              Data starts on {formatDate(metrics.start_date)}, so this period
              is shorter than selected.
            </p>
          ) : null}
        </div>
        <KpiStrip
          items={overviewKpis(performance, benchmarkPerformance?.metrics ?? null)}
        />
      </section>

      <ChartSection
        finding={performanceFinding(performance)}
        metric={`Cumulative return from ${formatDate(metrics.start_date)} to ${formatDate(metrics.end_date)}${
          ticker.ticker === bench.ticker ? "" : `, against ${bench.ticker} (${bench.name})`
        } · daily closes adjusted for splits and dividends`}
      >
        <MetricLineChart
          data={performanceChartData(performance)}
          categories={
            ticker.ticker === bench.ticker
              ? [ticker.ticker]
              : [ticker.ticker, bench.ticker]
          }
          colors={["cobalt", "muted"]}
          valueFormat="signedPercent"
          className="h-80"
        />
      </ChartSection>
    </div>
  )
}
