"use client"

import { RiArrowDownLine, RiArrowUpLine } from "@remixicon/react"
import { useState } from "react"

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRoot,
  TableRow,
} from "@/components/Table"
import { formatNumber, formatPercent, formatPoints, toneOf } from "@/lib/format"
import { MESSAGES, sectorName } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import {
  defaultDirection,
  sortSectors,
  type SortDirection,
  type SortKey,
} from "@/lib/sectors"
import type { SectorRow } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"

// Column order; labels come from i18n.
const COLUMNS: { key: SortKey; numeric: boolean }[] = [
  { key: "sector", numeric: false },
  { key: "total_return", numeric: true },
  { key: "cagr", numeric: true },
  { key: "volatility", numeric: true },
  { key: "max_drawdown", numeric: true },
  { key: "sharpe_ratio", numeric: true },
  { key: "beta", numeric: true },
  { key: "excess_return", numeric: true },
]

/** The sector comparison as a sortable table; the benchmark stays pinned at the bottom. */
export function SectorTable({
  rows,
  benchmark,
  highlight,
  locale = "en",
}: {
  rows: SectorRow[]
  benchmark: SectorRow
  highlight: string | null
  locale?: Locale
}) {
  const t = MESSAGES[locale].sectors
  const [sort, setSort] = useState<{ key: SortKey; direction: SortDirection }>(
    { key: "total_return", direction: "descending" },
  )
  const sorted = sortSectors(rows, sort.key, sort.direction, locale)

  const toggle = (key: SortKey) =>
    setSort((current) =>
      current.key === key
        ? {
            key,
            direction:
              current.direction === "ascending" ? "descending" : "ascending",
          }
        : { key, direction: defaultDirection(key) },
    )

  return (
    <TableRoot>
      <Table>
        <caption className="sr-only">
          {t.tableCaption}
        </caption>
        <TableHead>
          <TableRow>
            {COLUMNS.map((column) => {
              const active = sort.key === column.key
              const Arrow =
                sort.direction === "ascending" ? RiArrowUpLine : RiArrowDownLine
              return (
                <TableHeaderCell
                  key={column.key}
                  aria-sort={active ? sort.direction : "none"}
                  className={cx(column.numeric && "text-right")}
                >
                  <button
                    type="button"
                    onClick={() => toggle(column.key)}
                    className={cx(
                      "inline-flex items-center gap-1 rounded-sm hover:text-ink",
                      active ? "text-ink" : "text-graphite",
                      focusRing,
                    )}
                  >
                    {t.columns[column.key]}
                    <Arrow
                      className={cx(
                        "size-3.5",
                        active ? "opacity-100" : "opacity-0",
                      )}
                      aria-hidden="true"
                    />
                  </button>
                </TableHeaderCell>
              )
            })}
          </TableRow>
        </TableHead>
        <TableBody>
          {sorted.map((row) => (
            <SectorTableRow
              key={row.ticker}
              row={row}
              highlight={row.ticker === highlight}
              locale={locale}
            />
          ))}
          <SectorTableRow row={benchmark} isBenchmark locale={locale} />
        </TableBody>
      </Table>
    </TableRoot>
  )
}

function SectorTableRow({
  row,
  highlight = false,
  isBenchmark = false,
  locale,
}: {
  row: SectorRow
  highlight?: boolean
  isBenchmark?: boolean
  locale: Locale
}) {
  const pct = (value: number | null, signed = false) =>
    formatPercent(value, { signed, locale })
  const tone = toneOf(row.excess_return)
  return (
    <TableRow
      className={cx(
        isBenchmark && "border-t-2 border-rule",
        highlight && "bg-wash",
      )}
    >
      <TableCell>
        <span
          className={cx("font-medium", highlight ? "text-cobalt" : "text-ink")}
        >
          {isBenchmark
            ? MESSAGES[locale].sectors.benchmarkRow
            : sectorName(row.sector, locale)}
        </span>{" "}
        <span className="text-muted">{row.ticker}</span>
      </TableCell>
      <NumberCell>{pct(row.total_return, true)}</NumberCell>
      <NumberCell>{pct(row.cagr, true)}</NumberCell>
      <NumberCell>{pct(row.volatility)}</NumberCell>
      <NumberCell>{pct(row.max_drawdown)}</NumberCell>
      <NumberCell>{formatNumber(row.sharpe_ratio, { locale })}</NumberCell>
      <NumberCell>{formatNumber(row.beta, { locale })}</NumberCell>
      <NumberCell
        className={cx(
          !isBenchmark && tone === "gain" && "text-gain",
          !isBenchmark && tone === "loss" && "text-loss",
        )}
      >
        {isBenchmark ? "—" : formatPoints(row.excess_return, { locale })}
      </NumberCell>
    </TableRow>
  )
}

function NumberCell({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}) {
  return (
    <TableCell className={cx("text-right tabular-nums", className)}>
      {children}
    </TableCell>
  )
}
