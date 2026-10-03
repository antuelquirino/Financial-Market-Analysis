import type { Metadata } from "next"
import { Suspense } from "react"

import { ChartSection } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { Screen } from "@/components/market/Screen"
import { SectorScatter } from "@/components/market/SectorScatter"
import { SectorTable } from "@/components/market/SectorTable"
import { ChartError, ChartLoading } from "@/components/market/States"
import { api, loadOrNull } from "@/lib/api"
import { formatDate } from "@/lib/format"
import {
  highlightedSector,
  scatterData,
  scatterFinding,
  sectorsLead,
} from "@/lib/sectors"
import type { SearchParams } from "@/lib/selection"
import { loadShell, type Shell } from "@/lib/shell"

export const metadata: Metadata = { title: "Sectors" }

export const dynamic = "force-dynamic"

export default async function SectorsPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>
}) {
  const shell = await loadShell(searchParams)
  return (
    <Screen id="sectors" shell={shell}>
      <Suspense key={shell.selection.period} fallback={<ChartLoading className="h-96" />}>
        <Sectors shell={shell} />
      </Suspense>
    </Screen>
  )
}

async function Sectors({ shell }: { shell: Shell }) {
  const sectors = await loadOrNull(() => api.sectors(shell.selection.period))
  if (!sectors) return <ChartError message="The sector comparison could not be loaded." />
  const lead = sectorsLead(sectors)
  const highlight = highlightedSector(shell.selection.ticker, shell.tickers)
  const highlightName = sectors.sectors.find((s) => s.ticker === highlight)?.sector
  const range = `${formatDate(sectors.start_date)} to ${formatDate(sectors.end_date)}`
  return (
    <div className="space-y-12">
      <section aria-label="Summary">
        <p className="mb-2 text-sm text-graphite">The 11 SPDR sector ETFs</p>
        <LeadFinding>
          {lead.before}
          <Mark>{lead.mark}</Mark>
          {lead.after}
        </LeadFinding>
      </section>

      <ChartSection
        finding={scatterFinding(sectors)}
        metric={`Annualized return against annualized volatility · ${range}. Dashed lines cross at ${sectors.benchmark.ticker}${
          highlightName ? `; ${highlightName} (${highlight}) is ${shell.selection.ticker === highlight ? "selected" : `${shell.selection.ticker}’s sector`}` : ""
        }`}
      >
        <SectorScatter data={scatterData(sectors, highlight)} />
      </ChartSection>

      <ChartSection
        finding="Every sector, side by side"
        metric={`Returns, risk and the gap to ${sectors.benchmark.ticker} · ${range}`}
      >
        <SectorTable rows={sectors.sectors} benchmark={sectors.benchmark} highlight={highlight} />
      </ChartSection>
    </div>
  )
}
