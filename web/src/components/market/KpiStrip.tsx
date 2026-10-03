import {
  RiArrowDownLine,
  RiArrowUpLine,
  RiSubtractLine,
} from "@remixicon/react"

import type { Tone } from "@/lib/format"
import { cx } from "@/lib/utils"

export interface KpiItem {
  label: string
  value: string
  /** Optional comparison line, e.g. "+3.1 pts vs SPY" or a plain note. */
  change?: {
    text: string // already formatted: +3.1 pts, −4.2 pts
    direction: "up" | "down" | "flat"
    tone: Tone // gain / loss: whether the difference is good news
    comparison: string // "vs SPY"
  }
  note?: string // shown instead of a change: "Annualized"
}

const TONE_CLASSES: Record<Tone, string> = {
  gain: "text-gain",
  loss: "text-loss",
  neutral: "text-graphite",
}

const ARROWS = {
  up: RiArrowUpLine,
  down: RiArrowDownLine,
  flat: RiSubtractLine,
}

// The last item stretches to close its row, so no empty cell shows the rule
// color. Columns: 2 on phones, 3 on tablets, 5 on desktop.
function lastItemSpan(count: number) {
  return cx(
    count % 2 === 1 && "col-span-2",
    count % 3 === 2 && "sm:col-span-2",
    count % 3 === 1 && "sm:col-span-3",
    count % 3 === 0 && "sm:col-span-1",
    "lg:col-span-1",
  )
}

/**
 * Headline KPIs as one ruled band, not a row of identical cards: hairlines
 * between items come from a 1px gap over the rule color. Values use tabular
 * figures so the numbers read as one line of type.
 */
export function KpiStrip({ items }: { items: KpiItem[] }) {
  return (
    <dl className="-mx-4 grid grid-cols-2 gap-px border-y border-rule bg-rule sm:grid-cols-3 lg:grid-cols-5">
      {items.map((item, index) => (
        <div
          key={item.label}
          className={cx(
            "bg-paper px-4 py-4",
            index === items.length - 1 && lastItemSpan(items.length),
          )}
        >
          <dt className="text-sm text-graphite">{item.label}</dt>
          <dd className="mt-1 text-kpi font-semibold text-ink tabular-nums">
            {item.value}
          </dd>
          <dd className="mt-1 flex min-h-4 items-center gap-1 text-xs">
            {item.change ? (
              <KpiChange change={item.change} />
            ) : (
              <span className="text-muted">{item.note}</span>
            )}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function KpiChange({ change }: { change: NonNullable<KpiItem["change"]> }) {
  const Arrow = ARROWS[change.direction]
  return (
    <>
      <span
        className={cx(
          "inline-flex items-center gap-0.5 font-medium tabular-nums",
          TONE_CLASSES[change.tone],
        )}
      >
        <Arrow className="size-3.5 shrink-0" aria-hidden="true" />
        {change.text}
      </span>
      <span className="text-muted">{change.comparison}</span>
    </>
  )
}
