// The only place in the app where numbers and dates become text, so every
// screen reads the same: +25.2%, −20.2%, +3.1 pts, $412.37, 1.16, Sep 29, 2026.

const MISSING = "—"
const MINUS = "−" // same width as a digit, so tabular figures stay aligned

type Maybe = number | null | undefined

const isMissing = (value: Maybe): value is null | undefined =>
  value === null || value === undefined || Number.isNaN(value)

const fixed = (value: number, decimals: number) =>
  new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value)

// Decide the sign from the rounded value, so −0.01% never reads "−0.0%".
function signed(value: number, decimals: number, scale: number, plus: boolean) {
  const text = fixed(Math.abs(value) * scale, decimals)
  const isZero = Number(text.replace(/,/g, "")) === 0
  if (isZero) return text
  return (value < 0 ? MINUS : plus ? "+" : "") + text
}

/** A fraction as a percentage: 0.252 -> 25.2%, signed -> +25.2%, −20.2%. */
export function formatPercent(
  fraction: Maybe,
  { decimals = 1, signed: plus = false }: { decimals?: number; signed?: boolean } = {},
): string {
  if (isMissing(fraction)) return MISSING
  return `${signed(fraction, decimals, 100, plus)}%`
}

/** A difference between two returns, in percentage points: +3.1 pts. */
export function formatPoints(
  fraction: Maybe,
  { decimals = 1 }: { decimals?: number } = {},
): string {
  if (isMissing(fraction)) return MISSING
  return `${signed(fraction, decimals, 100, true)} pts`
}

/** A price in dollars: $412.37 · $5,734.12 */
export function formatPrice(value: Maybe): string {
  if (isMissing(value)) return MISSING
  return (value < 0 ? MINUS : "") + "$" + fixed(Math.abs(value), 2)
}

/** A plain number, for Sharpe ratios and betas: 1.16 · −0.30 */
export function formatNumber(
  value: Maybe,
  { decimals = 2 }: { decimals?: number } = {},
): string {
  if (isMissing(value)) return MISSING
  return signed(value, decimals, 1, false)
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

/** "2026-09-29" -> Sep 29, 2026 (day) · Sep 2026 (month) · September 29, 2026 (long). */
export function formatDate(
  iso: string | null | undefined,
  style: keyof typeof DATE_STYLES = "day",
): string {
  if (!iso) return MISSING
  return new Intl.DateTimeFormat("en-US", {
    ...DATE_STYLES[style],
    timeZone: "UTC",
  }).format(toDate(iso))
}

/** Whole years between two ISO dates, for "over 3 years". */
export function yearsBetween(startIso: string, endIso: string): number {
  return (toDate(endIso).getTime() - toDate(startIso).getTime()) / (365.25 * 864e5)
}

export type Tone = "gain" | "loss" | "neutral"

/** Whether a change is good news: its direction times whether up is good. */
export function toneOf(change: Maybe, higherIsBetter = true): Tone {
  if (isMissing(change) || change === 0) return "neutral"
  return change > 0 === higherIsBetter ? "gain" : "loss"
}
