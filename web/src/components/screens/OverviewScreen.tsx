import { Suspense } from "react"

import { ChartSection } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { KpiStrip } from "@/components/market/KpiStrip"
import { MetricLineChart } from "@/components/market/MetricChart"
import { Screen } from "@/components/market/Screen"
import { ChartError, ChartLoading } from "@/components/market/States"
import { api, loadOrNull } from "@/lib/api"
import { formatDate } from "@/lib/format"
import { MESSAGES } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import {
  overviewKpis,
  overviewLead,
  performanceChartData,
  performanceFinding,
} from "@/lib/overview"
import type { SearchParams, Selection } from "@/lib/selection"
import { loadShell } from "@/lib/shell"

export async function OverviewScreen({
  locale,
  searchParams,
}: {
  locale: Locale
  searchParams: Promise<SearchParams>
}) {
  const shell = await loadShell(searchParams)
  return (
    <Screen id="overview" shell={shell} locale={locale}>
      <Suspense
        key={`${shell.selection.ticker}-${shell.selection.period}`}
        fallback={<ChartLoading locale={locale} className="h-96" />}
      >
        <Overview selection={shell.selection} benchmark={shell.benchmark} locale={locale} />
      </Suspense>
    </Screen>
  )
}

async function Overview({
  selection,
  benchmark,
  locale,
}: {
  selection: Selection
  benchmark: string
  locale: Locale
}) {
  const t = MESSAGES[locale]
  const [performance, benchmarkPerformance] = await Promise.all([
    loadOrNull(() => api.performance(selection.ticker, selection.period)),
    loadOrNull(() => api.performance(benchmark, selection.period)),
  ])
  if (!performance) return <ChartError locale={locale} message={t.states.overview} />
  const lead = overviewLead(performance, locale)
  const { ticker, benchmark: bench, metrics } = performance
  const date = (iso: string) => formatDate(iso, "day", { locale })
  const self = ticker.ticker === bench.ticker
  return (
    <div className="space-y-12">
      <section aria-label={t.header.overview} className="space-y-6">
        <div>
          <p className="mb-2 text-sm text-graphite">{ticker.name}</p>
          <LeadFinding>
            {lead.before}
            <Mark>{lead.mark}</Mark>
            {lead.after}
          </LeadFinding>
          {!metrics.has_full_history ? (
            <p className="mt-2 text-sm text-muted">
              {t.shorterHistory(date(metrics.start_date))}
            </p>
          ) : null}
        </div>
        <KpiStrip
          items={overviewKpis(performance, benchmarkPerformance?.metrics ?? null, locale)}
        />
      </section>

      <ChartSection
        finding={performanceFinding(performance, locale)}
        metric={t.overview.chartMetric(
          t.dateRange(date(metrics.start_date), date(metrics.end_date)),
          self ? null : `${bench.ticker} (${bench.name})`,
        )}
      >
        <MetricLineChart
          data={performanceChartData(performance)}
          categories={self ? [ticker.ticker] : [ticker.ticker, bench.ticker]}
          colors={["cobalt", "muted"]}
          valueFormat="signedPercent"
          locale={locale}
          className="h-80"
        />
      </ChartSection>
    </div>
  )
}
