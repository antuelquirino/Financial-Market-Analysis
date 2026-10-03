import type { Metadata } from "next"
import Link from "next/link"
import { Suspense } from "react"

import { Badge } from "@/components/Badge"
import { Button } from "@/components/Button"
import { ChartSection, PageHeader } from "@/components/market/ChartSection"
import { LeadFinding, Mark } from "@/components/market/Finding"
import { KpiStrip } from "@/components/market/KpiStrip"
import { MetricLineChart } from "@/components/market/MetricChart"
import { PeriodFilter } from "@/components/market/PeriodFilter"
import { DrawdownChart, ReturnHistogram } from "@/components/market/RiskCharts"
import { SectorScatter } from "@/components/market/SectorScatter"
import { SectorTable } from "@/components/market/SectorTable"
import { DataFreshness, Disclaimer } from "@/components/market/SiteChrome"
import { ChartEmpty, ChartError, ChartLoading } from "@/components/market/States"
import { TickerSelect } from "@/components/market/TickerSelect"
import { api, loadOrNull } from "@/lib/api"
import {
  overviewKpis,
  overviewLead,
  performanceChartData,
  performanceFinding,
} from "@/lib/overview"
import {
  distributionFinding,
  drawdownFinding,
  histogramData,
} from "@/lib/risk"
import { highlightedSector, scatterData, scatterFinding } from "@/lib/sectors"
import { DEFAULT_PERIOD, DEFAULT_TICKER, type Selection } from "@/lib/selection"
import { INTERFACE_COLORS, SERIES_COLORS, type ColorToken } from "@/lib/tokens"
import type { StatusResponse } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"

export const metadata: Metadata = { title: "Design system" }

// Rendered per request, not at build time: the build may run where the API is
// unreachable, and a build-time error state would be cached.
export const dynamic = "force-dynamic"

const SAMPLE: Selection = { ticker: DEFAULT_TICKER, period: DEFAULT_PERIOD }

const PRINCIPLES = [
  [
    "One bold element",
    "Each screen opens with its finding as a sentence; the key figure wears a cobalt marker. Everything else stays quiet.",
  ],
  [
    "Titles state the finding",
    "“NVDA’s deepest fall was −20.2%”, not “Drawdown”. The metric and its dates go underneath, small.",
  ],
  [
    "Always against a benchmark",
    "A return means little alone. Every KPI that can be compared is compared with SPY over the same dates.",
  ],
  [
    "Hairlines, not boxes",
    "No shadows, no gradients, no card around everything. Rules and white space do the separating.",
  ],
  [
    "Color has one job each",
    "Cobalt is the subject. Gain and loss only mean better and worse, always with an arrow and a sign. The benchmark is context gray.",
  ],
  [
    "Honest about the data",
    "The latest close is always visible, late data says so, and every screen says it is not investment advice.",
  ],
]

// Shared with InsightFlow: same author, same system, a different accent.
const SAMPLE_STATUS: StatusResponse = {
  last_market_date: "2026-09-25",
  last_successful_run_at: "2026-09-25T22:41:00Z",
  last_run_at: "2026-09-25T22:41:00Z",
  last_run_status: "success",
  tickers_expected: 20,
  tickers_current: 19,
}

export default function StyleguidePage() {
  return (
    <div className="space-y-14 pb-16">
      <Link
        href="/"
        className={cx("text-sm text-graphite hover:text-ink", focusRing)}
      >
        ← Back to the dashboard
      </Link>
      <PageHeader
        title="Design system"
        note="The rules behind Financial Market’s interface. It shares InsightFlow’s system with its own accent. Examples use live data from the API."
      />

      <section aria-labelledby="concept" className="space-y-6">
        <LeadFinding>
          An analyst’s market note, <Mark>not a trading screen</Mark>.
        </LeadFinding>
        <p className="max-w-2xl text-sm text-graphite">
          The dashboard explains what happened to a price, so it reads like a
          well-edited note: warm paper, ink instead of black, and every chart
          titled with what it shows. Light mode is the default; dark mode is a
          designed counterpart, not an inversion.
        </p>
        <h2 id="concept" className="sr-only">
          Principles
        </h2>
        <dl className="grid gap-x-10 gap-y-5 sm:grid-cols-2 lg:grid-cols-3">
          {PRINCIPLES.map(([title, text]) => (
            <div key={title} className="border-t border-rule pt-3">
              <dt className="text-sm font-medium text-ink">{title}</dt>
              <dd className="mt-1 text-sm text-graphite">{text}</dd>
            </div>
          ))}
        </dl>
      </section>

      <StyleSection
        title="Color"
        intro="Warm neutrals for almost everything, cobalt as the one accent, and two semantic colors used for nothing else. Each token has a light and a dark value; text tokens pass 4.5:1."
      >
        <SwatchGrid tokens={INTERFACE_COLORS} />
      </StyleSection>

      <StyleSection
        title="Data colors"
        intro="Four series colors in a fixed order, validated together for colorblind separation (worst neighbor ΔE 12.9 light, 11.6 dark) and 3:1 contrast. The selected ticker is always cobalt and the benchmark always context gray."
      >
        <SwatchGrid tokens={SERIES_COLORS} />
      </StyleSection>

      <StyleSection
        title="Typography"
        intro="Newsreader, a serif made for reading news on screens, only for findings. IBM Plex Sans for the interface and every number, with tabular figures so columns of returns align."
      >
        <div className="divide-y divide-rule border-y border-rule">
          <TypeSpecimen spec="Newsreader 32 / 400 · lead finding">
            <p className="font-serif text-finding-lead text-ink">
              Energy led over the past year with <Mark>+46.1%</Mark>.
            </p>
          </TypeSpecimen>
          <TypeSpecimen spec="Newsreader 20 / 400 · chart finding">
            <p className="font-serif text-finding text-ink">
              NVDA ends the period 4.2% below its peak
            </p>
          </TypeSpecimen>
          <TypeSpecimen spec="Plex Sans 28 / 600 · KPI value, tabular figures">
            <p className="text-kpi font-semibold text-ink tabular-nums">
              +24.2% · 37.7% · −20.2% · 0.66
            </p>
          </TypeSpecimen>
          <TypeSpecimen spec="Plex Sans 14 / 400 · body">
            <p className="max-w-xl text-sm text-ink">
              Drawdown is the distance below the highest close since the period
              began.
            </p>
          </TypeSpecimen>
          <TypeSpecimen spec="Plex Sans 12 / 400 · captions and axes">
            <p className="text-xs text-muted tabular-nums">
              Oct 2025 · Jan 2026 · Apr 2026 · Jul 2026
            </p>
          </TypeSpecimen>
        </div>
      </StyleSection>

      <StyleSection
        title="Findings and KPIs"
        intro="The lead finding is built from API data with a sentence template. KPIs are one ruled band; each difference against SPY is colored by whether it is good news, so more volatility is a loss even though the number went up."
      >
        <Suspense fallback={<ChartLoading className="h-40" />}>
          <LiveLeadAndKpis />
        </Suspense>
      </StyleSection>

      <StyleSection
        title="Charts"
        intro="Lines for paths over time, an area below zero for drawdowns, bars for distributions, dots for comparisons. One series needs no legend; tooltips show the exact date and one more decimal than the axis."
      >
        <Suspense fallback={<ChartLoading />}>
          <LiveCharts />
        </Suspense>
      </StyleSection>

      <StyleSection
        title="Market components"
        intro="Pieces specific to this project: the data freshness line (here with a late update), the disclaimer every screen carries, the global ticker and period controls, and the sortable sector table."
      >
        <div className="space-y-8">
          <div className="space-y-2">
            <p className="text-xs text-muted">Freshness · a late update</p>
            <DataFreshness status={SAMPLE_STATUS} />
          </div>
          <Disclaimer />
          <Suspense fallback={<ChartLoading className="h-12" />}>
            <LiveControls />
          </Suspense>
          <Suspense fallback={<ChartLoading />}>
            <LiveTable />
          </Suspense>
        </div>
      </StyleSection>

      <StyleSection
        title="States"
        intro="Every chart has designed loading, empty and error states at the chart’s height. Loading is static; errors use ink and an icon, never the loss color."
      >
        <div className="grid gap-6 lg:grid-cols-3">
          <StateSpecimen label="Loading">
            <ChartLoading />
          </StateSpecimen>
          <StateSpecimen label="Empty">
            <ChartEmpty />
          </StateSpecimen>
          <StateSpecimen label="Error">
            <ChartError />
          </StateSpecimen>
        </div>
      </StyleSection>

      <StyleSection
        title="Controls"
        intro="Buttons and badges from the shared system, in this project’s accent."
      >
        <div className="flex flex-wrap items-center gap-3">
          <Button>Primary</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="ghost">Ghost</Button>
        </div>
        <div className="mt-5 flex flex-wrap items-center gap-2">
          <Badge variant="gain">▲ +3.2 pts vs SPY</Badge>
          <Badge variant="loss">▲ +24.7 pts volatility</Badge>
          <Badge variant="neutral">Same</Badge>
        </div>
      </StyleSection>
    </div>
  )
}

// --- Live examples (server components; each fails on its own) -----------------

async function LiveLeadAndKpis() {
  const [performance, benchmark] = await Promise.all([
    loadOrNull(() => api.performance(SAMPLE.ticker, SAMPLE.period)),
    loadOrNull(() => api.performance("SPY", SAMPLE.period)),
  ])
  if (!performance) {
    return <ChartError className="h-40" message="The KPIs could not be loaded." />
  }
  const lead = overviewLead(performance)
  return (
    <div className="space-y-6">
      <LeadFinding>
        {lead.before}
        <Mark>{lead.mark}</Mark>
        {lead.after}
      </LeadFinding>
      <KpiStrip items={overviewKpis(performance, benchmark?.metrics ?? null)} />
    </div>
  )
}

async function LiveCharts() {
  const [performance, risk, sectors, tickers] = await Promise.all([
    loadOrNull(() => api.performance(SAMPLE.ticker, SAMPLE.period)),
    loadOrNull(() => api.risk(SAMPLE.ticker, SAMPLE.period)),
    loadOrNull(() => api.sectors(SAMPLE.period)),
    loadOrNull(() => api.tickers()),
  ])
  if (!performance || !risk || !sectors) return <ChartError />
  const highlight = highlightedSector(SAMPLE.ticker, tickers?.tickers ?? [])
  return (
    <div className="grid gap-x-10 gap-y-10 xl:grid-cols-2">
      <ChartSection
        finding={performanceFinding(performance)}
        metric="Line · the selected ticker in cobalt, the benchmark in context gray"
      >
        <MetricLineChart
          data={performanceChartData(performance)}
          categories={[performance.ticker.ticker, performance.benchmark.ticker]}
          colors={["cobalt", "muted"]}
          valueFormat="signedPercent"
        />
      </ChartSection>
      <ChartSection
        finding={drawdownFinding(risk)}
        metric="Area below zero · round ticks down past the deepest fall"
      >
        <DrawdownChart data={risk.drawdown} />
      </ChartSection>
      <ChartSection
        finding={distributionFinding(risk)}
        metric="Histogram · half-point bins aligned on zero, a rule at zero"
      >
        <ReturnHistogram data={histogramData(risk)} />
      </ChartSection>
      <ChartSection
        finding={scatterFinding(sectors)}
        metric="Scatter · context dots muted, the subject cobalt, the benchmark an ink ring with guides"
      >
        <SectorScatter data={scatterData(sectors, highlight)} className="h-72" />
      </ChartSection>
    </div>
  )
}

async function LiveControls() {
  const tickers = await loadOrNull(() => api.tickers())
  return (
    <div className="flex flex-wrap items-center gap-3">
      {tickers ? <TickerSelect tickers={tickers.tickers} selection={SAMPLE} /> : null}
      <PeriodFilter selection={SAMPLE} path="/styleguide" />
    </div>
  )
}

async function LiveTable() {
  const [sectors, tickers] = await Promise.all([
    loadOrNull(() => api.sectors(SAMPLE.period)),
    loadOrNull(() => api.tickers()),
  ])
  if (!sectors) return <ChartError />
  return (
    <SectorTable
      rows={sectors.sectors}
      benchmark={sectors.benchmark}
      highlight={highlightedSector(SAMPLE.ticker, tickers?.tickers ?? [])}
    />
  )
}

// --- Page pieces ------------------------------------------------------------------

function StyleSection({
  title,
  intro,
  children,
}: {
  title: string
  intro: string
  children: React.ReactNode
}) {
  const id = title.toLowerCase().replace(/\s+/g, "-")
  return (
    <section aria-labelledby={id} className="border-t border-rule pt-8">
      <h2 id={id} className="text-lg font-semibold text-ink">
        {title}
      </h2>
      <p className="mt-1 max-w-2xl text-sm text-graphite">{intro}</p>
      <div className="mt-6">{children}</div>
    </section>
  )
}

function SwatchGrid({ tokens }: { tokens: ColorToken[] }) {
  return (
    <ul className="grid grid-cols-2 gap-x-6 gap-y-5 lg:grid-cols-3 xl:grid-cols-5">
      {tokens.map((token) => (
        <li key={token.variable}>
          {/* light and dark values side by side, whatever the current mode */}
          <div className="flex h-14 overflow-hidden rounded-md border border-rule">
            <div className="flex-1" style={{ backgroundColor: token.light }} />
            <div className="flex-1" style={{ backgroundColor: token.dark }} />
          </div>
          <p className="mt-2 text-sm font-medium text-ink">{token.name}</p>
          <p className="text-xs text-graphite">{token.role}</p>
          <p className="mt-0.5 font-mono text-xs text-muted">
            {token.light} · {token.dark}
          </p>
        </li>
      ))}
    </ul>
  )
}

function TypeSpecimen({
  spec,
  children,
}: {
  spec: string
  children: React.ReactNode
}) {
  return (
    <div className="grid gap-2 py-4 md:grid-cols-[14rem_1fr] md:items-baseline">
      <p className="text-xs text-muted">{spec}</p>
      {children}
    </div>
  )
}

function StateSpecimen({
  label,
  children,
}: {
  label: string
  children: React.ReactNode
}) {
  return (
    <figure>
      {children}
      <figcaption className="mt-2 text-xs text-muted">{label}</figcaption>
    </figure>
  )
}
