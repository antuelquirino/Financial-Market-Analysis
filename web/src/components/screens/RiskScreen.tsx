import { Suspense } from "react"

import { ChartSection } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { KpiStrip } from "@/components/market/KpiStrip"
import { MetricLineChart } from "@/components/market/MetricChart"
import { DrawdownChart, ReturnHistogram } from "@/components/market/RiskCharts"
import { Screen } from "@/components/market/Screen"
import { ChartEmpty, ChartError, ChartLoading } from "@/components/market/States"
import { api, loadOrNull } from "@/lib/api"
import { formatCount, formatDate } from "@/lib/format"
import { MESSAGES } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
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

export async function RiskScreen({
  locale,
  searchParams,
}: {
  locale: Locale
  searchParams: Promise<SearchParams>
}) {
  const shell = await loadShell(searchParams)
  return (
    <Screen id="risk" shell={shell} locale={locale}>
      <Suspense
        key={`${shell.selection.ticker}-${shell.selection.period}`}
        fallback={<ChartLoading locale={locale} className="h-96" />}
      >
        <Risk selection={shell.selection} benchmark={shell.benchmark} locale={locale} />
      </Suspense>
    </Screen>
  )
}

async function Risk({
  selection,
  benchmark,
  locale,
}: {
  selection: Selection
  benchmark: string
  locale: Locale
}) {
  const t = MESSAGES[locale]
  const [risk, benchmarkPerformance] = await Promise.all([
    loadOrNull(() => api.risk(selection.ticker, selection.period)),
    loadOrNull(() => api.performance(benchmark, selection.period)),
  ])
  if (!risk) return <ChartError locale={locale} message={t.states.risk} />
  const lead = riskLead(risk, locale)
  const date = (iso: string) => formatDate(iso, "day", { locale })
  const range = t.dateRange(date(risk.metrics.start_date), date(risk.metrics.end_date))
  const volatility = volatilityChartData(risk, locale)
  const hasVolatility = risk.rolling_volatility.some((p) => p.volatility_1m !== null)
  return (
    <div className="space-y-12">
      <section aria-label={t.header.risk} className="space-y-6">
        <div>
          <p className="mb-2 text-sm text-graphite">{risk.ticker.name}</p>
          <LeadFinding>
            {lead.before}
            <Mark>{lead.mark}</Mark>
            {lead.after}
          </LeadFinding>
        </div>
        <KpiStrip
          items={riskKpis(risk, benchmarkPerformance?.metrics ?? null, benchmark, locale)}
        />
      </section>

      <ChartSection finding={drawdownFinding(risk, locale)} metric={t.risk.drawdownMetric(range)}>
        <DrawdownChart data={risk.drawdown} locale={locale} />
      </ChartSection>

      <div className="grid grid-cols-1 gap-x-12 gap-y-12 xl:grid-cols-2">
        <ChartSection finding={volatilityFinding(risk, locale)} metric={t.risk.volatilityMetric}>
          {hasVolatility ? (
            <MetricLineChart
              data={volatility}
              categories={[t.risk.oneMonth, t.risk.oneYear]}
              colors={["cobalt", "ochre"]}
              valueFormat="percent"
              locale={locale}
            />
          ) : (
            <ChartEmpty locale={locale} message={t.states.noVolatility} />
          )}
        </ChartSection>

        <ChartSection
          finding={distributionFinding(risk, locale)}
          metric={t.risk.distributionMetric(
            formatCount(risk.return_distribution.sessions, { locale }),
          )}
        >
          {risk.return_distribution.bins.length ? (
            <ReturnHistogram data={histogramData(risk, locale)} locale={locale} />
          ) : (
            <ChartEmpty locale={locale} />
          )}
        </ChartSection>
      </div>
    </div>
  )
}
