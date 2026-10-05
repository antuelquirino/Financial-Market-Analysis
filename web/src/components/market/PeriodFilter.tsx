import Link from "next/link"

import { MESSAGES } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import { hrefFor, type Selection } from "@/lib/selection"
import { PERIODS } from "@/lib/types"
import { cx, focusRing } from "@/lib/utils"

/** A row of linked options with the current one filled in ink. */
export function Segmented({
  label,
  options,
}: {
  label: string
  options: { href: string; text: string; active: boolean; lang?: string }[]
}) {
  return (
    <nav aria-label={label}>
      <ul className="flex rounded-md border border-rule bg-surface p-0.5 text-sm">
        {options.map((option) => (
          <li key={option.href}>
            <Link
              href={option.href}
              hrefLang={option.lang}
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
  locale = "en",
}: {
  selection: Selection
  /** The current screen's path, with its language prefix. */
  path: string
  locale?: Locale
}) {
  const t = MESSAGES[locale].period
  return (
    <Segmented
      label={t.label}
      options={PERIODS.map((period) => ({
        href: hrefFor(path, selection, { period }),
        text: t.short[period],
        active: period === selection.period,
      }))}
    />
  )
}
