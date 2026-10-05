import { Suspense } from "react"

import { ChartSection } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { Screen } from "@/components/market/Screen"
import { SectorScatter } from "@/components/market/SectorScatter"
import { SectorTable } from "@/components/market/SectorTable"
import { ChartError, ChartLoading } from "@/components/market/States"
import { api, loadOrNull } from "@/lib/api"
import { formatDate } from "@/lib/format"
import { MESSAGES, sectorName } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import {
  highlightedSector,
  scatterData,
  scatterFinding,
  sectorsLead,
} from "@/lib/sectors"
import type { SearchParams } from "@/lib/selection"
import { loadShell, type Shell } from "@/lib/shell"

export async function SectorsScreen({
  locale,
  searchParams,
}: {
  locale: Locale
  searchParams: Promise<SearchParams>
}) {
  const shell = await loadShell(searchParams)
  return (
    <Screen id="sectors" shell={shell} locale={locale}>
      <Suspense
        key={shell.selection.period}
        fallback={<ChartLoading locale={locale} className="h-96" />}
      >
        <Sectors shell={shell} locale={locale} />
      </Suspense>
    </Screen>
  )
}

async function Sectors({ shell, locale }: { shell: Shell; locale: Locale }) {
  const t = MESSAGES[locale]
  const sectors = await loadOrNull(() => api.sectors(shell.selection.period))
  if (!sectors) return <ChartError locale={locale} message={t.states.sectors} />
  const lead = sectorsLead(sectors, locale)
  const highlight = highlightedSector(shell.selection.ticker, shell.tickers)
  const highlightRow = sectors.sectors.find((s) => s.ticker === highlight)
  const date = (iso: string) => formatDate(iso, "day", { locale })
  const range = t.dateRange(date(sectors.start_date), date(sectors.end_date))
  const highlightNote = highlightRow
    ? shell.selection.ticker === highlightRow.ticker
      ? t.sectors.highlightSelected(sectorName(highlightRow.sector, locale), highlightRow.ticker)
      : t.sectors.highlightSectorOf(
          sectorName(highlightRow.sector, locale),
          highlightRow.ticker,
          shell.selection.ticker,
        )
    : null
  return (
    <div className="space-y-12">
      <section aria-label={t.header.sectors}>
        <p className="mb-2 text-sm text-graphite">{t.sectors.kicker}</p>
        <LeadFinding>
          {lead.before}
          <Mark>{lead.mark}</Mark>
          {lead.after}
        </LeadFinding>
      </section>

      <ChartSection
        finding={scatterFinding(sectors, locale)}
        metric={t.sectors.scatterMetric(range, sectors.benchmark.ticker, highlightNote)}
      >
        <SectorScatter data={scatterData(sectors, highlight, locale)} locale={locale} />
      </ChartSection>

      <ChartSection
        finding={t.sectors.tableFinding}
        metric={t.sectors.tableMetric(sectors.benchmark.ticker, range)}
      >
        <SectorTable
          rows={sectors.sectors}
          benchmark={sectors.benchmark}
          highlight={highlight}
          locale={locale}
        />
      </ChartSection>
    </div>
  )
}
