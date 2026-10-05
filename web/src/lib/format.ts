// The only place in the app where numbers and dates become text, so every
// screen reads the same: +25.2%, −20.2%, +3.1 pts, $412.37, 1.16, Sep 29, 2026
// in English, and +25,2%, −20,2%, +3,1 pp, US$412,37, 1,16, 29 de sep de 2026
// in Spanish (Argentina).

import { INTL_LOCALES, type Locale } from "./locale"

const MISSING = "—"
const MINUS = "−" // same width as a digit, so tabular figures stay aligned

type Maybe = number | null | undefined

interface Options {
  locale?: Locale
}

const isMissing = (value: Maybe): value is null | undefined =>
  value === null || value === undefined || Number.isNaN(value)

const fixed = (value: number, decimals: number, locale: Locale = "en") =>
  new Intl.NumberFormat(INTL_LOCALES[locale], {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value)

// Decide the sign from the rounded value, so −0.01% never reads "−0.0%".
function signed(
  value: number,
  decimals: number,
  scale: number,
  plus: boolean,
  locale: Locale,
) {
  const text = fixed(Math.abs(value) * scale, decimals, locale)
  // No digit other than zero: the value rounds to zero in either language.
  if (!/[1-9]/.test(text)) return text
  return (value < 0 ? MINUS : plus ? "+" : "") + text
}

/** A fraction as a percentage: 0.252 -> 25.2% (es: 25,2%), signed -> +25.2%, −20.2%. */
export function formatPercent(
  fraction: Maybe,
  {
    decimals = 1,
    signed: plus = false,
    locale = "en",
  }: Options & { decimals?: number; signed?: boolean } = {},
): string {
  if (isMissing(fraction)) return MISSING
  return `${signed(fraction, decimals, 100, plus, locale)}%`
}

/** A difference between two returns, in percentage points: +3.1 pts (es: +3,1 pp). */
export function formatPoints(
  fraction: Maybe,
  { decimals = 1, locale = "en" }: Options & { decimals?: number } = {},
): string {
  if (isMissing(fraction)) return MISSING
  const unit = locale === "es" ? "pp" : "pts"
  return `${signed(fraction, decimals, 100, true, locale)} ${unit}`
}

/** A price in dollars: $412.37 · $5,734.12 (es: US$412,37 · US$5.734,12) */
export function formatPrice(value: Maybe, { locale = "en" }: Options = {}): string {
  if (isMissing(value)) return MISSING
  const prefix = locale === "es" ? "US$" : "$"
  return (value < 0 ? MINUS : "") + prefix + fixed(Math.abs(value), 2, locale)
}

/** A plain number, for Sharpe ratios and betas: 1.16 · −0.30 (es: 1,16 · −0,30) */
export function formatNumber(
  value: Maybe,
  {
    decimals = 2,
    signed: plus = false,
    locale = "en",
  }: Options & { decimals?: number; signed?: boolean } = {},
): string {
  if (isMissing(value)) return MISSING
  return signed(value, decimals, 1, plus, locale)
}

/** A whole count: 1,254 (es: 1.254) */
export function formatCount(value: number, { locale = "en" }: Options = {}): string {
  return fixed(value, 0, locale)
}

// The API sends dates as "2026-09-29". Parse as UTC so no time zone can move
// the date to the previous day.
function toDate(iso: string): Date {
  const [year, month, day = 1] = iso.slice(0, 10).split("-").map(Number)
  return new Date(Date.UTC(year, month - 1, day))
}

const DATE_STYLES: Record<"day" | "month" | "long", Intl.DateTimeFormatOptions> =
  {
    day: { month: "short", day: "numeric", year: "numeric" }, // Sep 29, 2026
    month: { month: "short", year: "numeric" }, // Sep 2026
    long: { month: "long", day: "numeric", year: "numeric" }, // September 29, 2026
  }

/**
 * "2026-09-29" -> Sep 29, 2026 (day) · Sep 2026 (month) · September 29, 2026 (long).
 * Spanish: 29 de sep de 2026 · sep 2026 · 29 de septiembre de 2026.
 */
export function formatDate(
  iso: string | null | undefined,
  style: keyof typeof DATE_STYLES = "day",
  { locale = "en" }: Options = {},
): string {
  if (!iso) return MISSING
  return new Intl.DateTimeFormat(INTL_LOCALES[locale], {
    ...DATE_STYLES[style],
    timeZone: "UTC",
  }).format(toDate(iso))
}

/** Whole years between two ISO dates, for "over 3 years". */
export function yearsBetween(startIso: string, endIso: string): number {
  return (
    (toDate(endIso).getTime() - toDate(startIso).getTime()) / (365.25 * 864e5)
  )
}

export type Tone = "gain" | "loss" | "neutral"

/** Whether a change is good news: its direction times whether up is good. */
export function toneOf(change: Maybe, higherIsBetter = true): Tone {
  if (isMissing(change) || change === 0) return "neutral"
  return change > 0 === higherIsBetter ? "gain" : "loss"
}
