import { cx } from "@/lib/utils"
import { useId } from "react"

/**
 * A chart and its title. The title states the finding ("Starter churn doubled
 * after the price change"), set in the serif; the metric it measures goes
 * underneath, small. Sections are separated by a hairline, not boxed.
 */
export function ChartSection({
  finding,
  metric,
  children,
  className,
}: {
  finding: React.ReactNode
  metric: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  const id = useId()
  return (
    <section
      aria-labelledby={id}
      className={cx("border-t border-rule pt-6", className)}
    >
      <h2 id={id} className="font-serif text-finding text-balance text-ink">
        {finding}
      </h2>
      <p className="mt-1 text-sm text-muted">{metric}</p>
      <div className="mt-5">{children}</div>
    </section>
  )
}

/** Screen title with an optional note and controls on the right. */
export function PageHeader({
  title,
  note,
  actions,
}: {
  title: string
  note?: React.ReactNode
  actions?: React.ReactNode
}) {
  return (
    <header className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-xl font-semibold text-ink">{title}</h1>
        {note ? <p className="mt-1 text-sm text-muted">{note}</p> : null}
      </div>
      {actions}
    </header>
  )
}
