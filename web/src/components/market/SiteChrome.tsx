import { RiErrorWarningLine } from "@remixicon/react"
import Link from "next/link"

import { freshness } from "@/lib/freshness"
import { MESSAGES } from "@/lib/i18n"
import { LOCALES, localePath, type Locale } from "@/lib/locale"
import { hrefFor, type Selection } from "@/lib/selection"
import type { StatusResponse, Ticker } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"
import { PeriodFilter, Segmented } from "./PeriodFilter"
import { ThemeToggle } from "./ThemeToggle"
import { TickerSelect } from "./TickerSelect"

export type Screen = "overview" | "risk" | "sectors"

const SCREENS: { id: Screen; path: string }[] = [
  { id: "overview", path: "/" },
  { id: "risk", path: "/risk" },
  { id: "sectors", path: "/sectors" },
]

/** The screen's path in a language: "/risk" or "/es/risk". */
export const pathOf = (screen: Screen, locale: Locale = "en") =>
  localePath(locale, SCREENS.find((s) => s.id === screen)!.path)

/** Brand, data freshness, the three screens and the global selection. */
export function SiteHeader({
  screen,
  selection,
  tickers,
  status,
  locale = "en",
}: {
  screen: Screen
  selection: Selection
  tickers: Ticker[]
  status: StatusResponse | null
  locale?: Locale
}) {
  const t = MESSAGES[locale].header
  return (
    <header className="space-y-5 border-b border-rule pb-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-serif text-2xl text-ink">Financial Market</p>
          <p className="mt-0.5 text-sm text-muted">{t.subtitle}</p>
          <DataFreshness status={status} locale={locale} />
        </div>
        <div className="flex items-center gap-3">
          <LanguageSwitcher current={locale} screen={screen} selection={selection} />
          <ThemeToggle locale={locale} />
        </div>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <nav aria-label={t.screens}>
          <ul className="flex gap-1 text-sm">
            {SCREENS.map((s) => (
              <li key={s.id}>
                <Link
                  href={hrefFor(pathOf(s.id, locale), selection)}
                  aria-current={s.id === screen ? "page" : undefined}
                  className={cx(
                    "block border-b-2 px-2 py-1.5 transition-colors",
                    s.id === screen
                      ? "border-cobalt font-medium text-ink"
                      : "border-transparent text-graphite hover:text-ink",
                    focusRing,
                  )}
                >
                  {t[s.id]}
                </Link>
              </li>
            ))}
          </ul>
        </nav>
        <div className="flex flex-wrap items-center gap-3">
          {tickers.length ? (
            <TickerSelect tickers={tickers} selection={selection} locale={locale} />
          ) : null}
          <PeriodFilter selection={selection} path={pathOf(screen, locale)} locale={locale} />
        </div>
      </div>
    </header>
  )
}

const LANGUAGE_NAMES: Record<Locale, string> = { en: "EN", es: "ES" }

/** EN | ES. Each language has its own URL (/ and /es); screen, ticker and period carry over. */
export function LanguageSwitcher({
  current,
  screen,
  selection,
}: {
  current: Locale
  screen: Screen
  selection: Selection
}) {
  return (
    <Segmented
      label={MESSAGES[current].header.language}
      options={LOCALES.map((locale) => ({
        href: hrefFor(pathOf(screen, locale), selection),
        text: LANGUAGE_NAMES[locale],
        active: locale === current,
        lang: locale,
      }))}
    />
  )
}

/** "Data updated: Sep 29, 2026 market close", with a warning when it is late. */
export function DataFreshness({
  status,
  locale = "en",
}: {
  status: StatusResponse | null
  locale?: Locale
}) {
  if (!status) return null
  const info = freshness(status, new Date(), locale)
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
export function Disclaimer({
  locale = "en",
  className,
}: {
  locale?: Locale
  className?: string
}) {
  const t = MESSAGES[locale].disclaimer
  return (
    <p className={cx("max-w-3xl text-xs text-muted", className)}>
      <strong className="font-medium text-graphite">{t.title}</strong> {t.body}
    </p>
  )
}

export function SiteFooter({ locale = "en" }: { locale?: Locale }) {
  const t = MESSAGES[locale].footer
  return (
    <footer className="space-y-3 border-t border-rule pt-5">
      <Disclaimer locale={locale} />
      <p className="text-xs text-muted">
        {t.builtBy} ·{" "}
        <Link
          href="/styleguide"
          className={cx(
            "underline decoration-rule underline-offset-2 hover:text-ink",
            focusRing,
          )}
        >
          {t.designSystem}
        </Link>
      </p>
    </footer>
  )
}
