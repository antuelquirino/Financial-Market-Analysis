import Link from "next/link"

import { hrefFor, type Selection } from "@/lib/selection"
import { PERIODS } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"

/** A row of linked options with the current one filled in ink. */
export function Segmented({
  label,
  options,
}: {
  label: string
  options: { href: string; text: string; active: boolean }[]
}) {
  return (
    <nav aria-label={label}>
      <ul className="flex rounded-md border border-rule bg-surface p-0.5 text-sm">
        {options.map((option) => (
          <li key={option.href}>
            <Link
              href={option.href}
              aria-current={option.active ? "true" : undefined}
              scroll={false}
              className={cx(
                "block rounded-[5px] px-3 py-1 tabular-nums transition-colors",
                option.active
                  ? "bg-ink font-medium text-paper"
                  : "text-graphite hover:bg-wash hover:text-ink",
                focusRing,
              )}
            >
              {option.text}
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  )
}

/** 1, 3 or 5 years, or all history. Plain links that set ?period=. */
export function PeriodFilter({
  selection,
  path,
}: {
  selection: Selection
  path: string
}) {
  return (
    <Segmented
      label="Period"
      options={PERIODS.map((period) => ({
        href: hrefFor(path, selection, { period }),
        text: period === "MAX" ? "Max" : period,
        active: period === selection.period,
      }))}
    />
  )
}
