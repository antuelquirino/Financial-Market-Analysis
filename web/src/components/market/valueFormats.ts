import { formatDate, formatNumber, formatPercent, formatPrice } from "@/lib/format"

// Server components cannot pass functions to client components, so charts
// receive the name of a format and look the formatter up here.
export type ValueFormat = "percent" | "signedPercent" | "price" | "number"

// Axis ticks land on round values and tooltips on arbitrary ones, and Tremor
// uses one formatter for both: whole percents print without decimals (20%),
// anything else with one (25.2%).
const percentDecimals = (value: number) =>
  Math.abs(value * 100 - Math.round(value * 100)) < 1e-9 ? 0 : 1

export function formatterFor(format: ValueFormat): (value: number) => string {
  switch (format) {
    case "percent":
      return (value) => formatPercent(value, { decimals: percentDecimals(value) })
    case "signedPercent":
      return (value) =>
        formatPercent(value, { decimals: percentDecimals(value), signed: true })
    case "price":
      return (value) => formatPrice(value)
    case "number":
      return (value) => formatNumber(value)
  }
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/

export const isIsoDate = (value: unknown): value is string =>
  typeof value === "string" && ISO_DATE.test(value)

/** Axis ticks of daily data: "Sep 2026". */
export const formatAxisDate = (value: unknown): string =>
  isIsoDate(value) ? formatDate(value, "month") : String(value ?? "")

/** Tooltip titles of daily data: "Sep 29, 2026". */
export const formatTooltipDate = (value: unknown): string =>
  isIsoDate(value) ? formatDate(value) : String(value ?? "")
