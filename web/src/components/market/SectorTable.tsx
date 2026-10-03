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
import {
  defaultDirection,
  sortSectors,
  type SortDirection,
  type SortKey,
} from "@/lib/sectors"
import type { SectorRow } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"

const COLUMNS: { key: SortKey; label: string; numeric: boolean }[] = [
  { key: "sector", label: "Sector", numeric: false },
  { key: "total_return", label: "Return", numeric: true },
  { key: "cagr", label: "Annualized", numeric: true },
  { key: "volatility", label: "Volatility", numeric: true },
  { key: "max_drawdown", label: "Max drawdown", numeric: true },
  { key: "sharpe_ratio", label: "Sharpe", numeric: true },
  { key: "beta", label: "Beta", numeric: true },
  { key: "excess_return", label: "vs benchmark", numeric: true },
]

/** The sector comparison as a sortable table; the benchmark stays pinned at the bottom. */
export function SectorTable({
  rows,
  benchmark,
  highlight,
}: {
  rows: SectorRow[]
  benchmark: SectorRow
  highlight: string | null
}) {
  const [sort, setSort] = useState<{ key: SortKey; direction: SortDirection }>(
    { key: "total_return", direction: "descending" },
  )
  const sorted = sortSectors(rows, sort.key, sort.direction)

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
          Sector ETFs compared over the period. Select a column header to sort.
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
                    {column.label}
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
            />
          ))}
          <SectorTableRow row={benchmark} isBenchmark />
        </TableBody>
      </Table>
    </TableRoot>
  )
}

function SectorTableRow({
  row,
  highlight = false,
  isBenchmark = false,
}: {
  row: SectorRow
  highlight?: boolean
  isBenchmark?: boolean
}) {
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
          {isBenchmark ? "Benchmark" : row.sector}
        </span>{" "}
        <span className="text-muted">{row.ticker}</span>
      </TableCell>
      <NumberCell>{formatPercent(row.total_return, { signed: true })}</NumberCell>
      <NumberCell>{formatPercent(row.cagr, { signed: true })}</NumberCell>
      <NumberCell>{formatPercent(row.volatility)}</NumberCell>
      <NumberCell>{formatPercent(row.max_drawdown)}</NumberCell>
      <NumberCell>{formatNumber(row.sharpe_ratio)}</NumberCell>
      <NumberCell>{formatNumber(row.beta)}</NumberCell>
      <NumberCell
        className={cx(
          !isBenchmark && tone === "gain" && "text-gain",
          !isBenchmark && tone === "loss" && "text-loss",
        )}
      >
        {isBenchmark ? "—" : formatPoints(row.excess_return)}
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
