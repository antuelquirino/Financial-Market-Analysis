import { RiErrorWarningLine } from "@remixicon/react"
import Link from "next/link"

import { freshness } from "@/lib/freshness"
import { hrefFor, type Selection } from "@/lib/selection"
import type { StatusResponse, Ticker } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"
import { PeriodFilter } from "./PeriodFilter"
import { ThemeToggle } from "./ThemeToggle"
import { TickerSelect } from "./TickerSelect"

export type Screen = "overview" | "risk" | "sectors"

const SCREENS: { id: Screen; label: string; path: string }[] = [
  { id: "overview", label: "Overview", path: "/" },
  { id: "risk", label: "Risk", path: "/risk" },
  { id: "sectors", label: "Sectors", path: "/sectors" },
]

export const pathOf = (screen: Screen) =>
  SCREENS.find((s) => s.id === screen)!.path

/** Brand, data freshness, the three screens and the global selection. */
export function SiteHeader({
  screen,
  selection,
  tickers,
  status,
}: {
  screen: Screen
  selection: Selection
  tickers: Ticker[]
  status: StatusResponse | null
}) {
  return (
    <header className="space-y-5 border-b border-rule pb-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-serif text-2xl text-ink">Financial Market</p>
          <p className="mt-0.5 text-sm text-muted">
            US stocks, sector ETFs and the S&amp;P 500, refreshed after every
            close
          </p>
          <DataFreshness status={status} />
        </div>
        <ThemeToggle />
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <nav aria-label="Screens">
          <ul className="flex gap-1 text-sm">
            {SCREENS.map((s) => (
              <li key={s.id}>
                <Link
                  href={hrefFor(s.path, selection)}
                  aria-current={s.id === screen ? "page" : undefined}
                  className={cx(
                    "block border-b-2 px-2 py-1.5 transition-colors",
                    s.id === screen
                      ? "border-cobalt font-medium text-ink"
                      : "border-transparent text-graphite hover:text-ink",
                    focusRing,
                  )}
                >
                  {s.label}
                </Link>
              </li>
            ))}
          </ul>
        </nav>
        <div className="flex flex-wrap items-center gap-3">
          {tickers.length ? (
            <TickerSelect tickers={tickers} selection={selection} />
          ) : null}
          <PeriodFilter selection={selection} path={pathOf(screen)} />
        </div>
      </div>
    </header>
  )
}

/** "Data updated: Sep 29, 2026 market close", with a warning when it is late. */
export function DataFreshness({ status }: { status: StatusResponse | null }) {
  if (!status) return null
  const info = freshness(status)
  return (
    <p className="mt-2 flex flex-wrap items-center gap-x-2 text-xs text-graphite">
      <span className="tabular-nums">{info.label}</span>
      {info.note ? (
        <span className="inline-flex items-center gap-1 text-ink">
          <RiErrorWarningLine className="size-3.5" aria-hidden="true" />
          {info.note}
        </span>
      ) : null}
    </p>
  )
}

/** Shown on every screen. */
export function Disclaimer({ className }: { className?: string }) {
  return (
    <p className={cx("max-w-3xl text-xs text-muted", className)}>
      <strong className="font-medium text-graphite">
        Not investment advice.
      </strong>{" "}
      For information and education only. Prices come from Yahoo Finance, an
      unofficial source, adjusted for splits and dividends. Past performance
      does not predict future returns.
    </p>
  )
}

export function SiteFooter() {
  return (
    <footer className="space-y-3 border-t border-rule pt-5">
      <Disclaimer />
      <p className="text-xs text-muted">
        Built by Antuel Quirino ·{" "}
        <Link
          href="/styleguide"
          className={cx(
            "underline decoration-rule underline-offset-2 hover:text-ink",
            focusRing,
          )}
        >
          Design system
        </Link>
      </p>
    </footer>
  )
}
