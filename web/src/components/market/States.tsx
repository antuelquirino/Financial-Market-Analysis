import { RiErrorWarningLine } from "@remixicon/react"

import { cx } from "@/lib/utils"
import { RetryButton } from "./RetryButton"

// Every chart has three designed states besides the chart itself. They keep the
// chart's height so nothing jumps when data arrives, and they never animate.

/** Loading: faint static rules where the chart will be. No shimmer. */
export function ChartLoading({
  className,
}: {
  className?: string
}) {
  return (
    <div
      role="status"
      aria-label="Loading"
      className={cx("flex h-72 flex-col justify-between py-2", className)}
    >
      {[0, 1, 2, 3, 4].map((line) => (
        <div key={line} className="h-px w-full bg-rule" />
      ))}
      <div className="flex justify-between">
        {[0, 1, 2, 3, 4, 5].map((tick) => (
          <div key={tick} className="h-2 w-8 rounded-xs bg-wash" />
        ))}
      </div>
    </div>
  )
}

/** Empty: say what happened and what to try. */
export function ChartEmpty({
  message,
  hint,
  className,
}: {
  message?: string
  hint?: string
  className?: string
}) {
  return (
    <div
      className={cx(
        "flex h-72 flex-col items-center justify-center border border-rule text-center",
        className,
      )}
    >
      <p className="text-sm font-medium text-ink">{message ?? "No data for this selection."}</p>
      <p className="mt-1 text-sm text-muted">{hint ?? "Try a longer period or another ticker."}</p>
    </div>
  )
}

/**
 * Error: plain ink and an icon, not the loss color. Failing to load is not
 * "worse" business news, and loss must keep meaning only that.
 */
export function ChartError({
  message,
  className,
}: {
  message?: string
  className?: string
}) {
  return (
    <div
      role="alert"
      className={cx(
        "flex h-72 flex-col items-center justify-center border border-rule text-center",
        className,
      )}
    >
      <RiErrorWarningLine className="size-5 text-graphite" aria-hidden="true" />
      <p className="mt-2 text-sm font-medium text-ink">{message ?? "This chart could not be loaded."}</p>
      <p className="mt-1 text-sm text-muted">The data service did not answer. It may be waking up; try again in a few seconds.</p>
      <RetryButton className="mt-4" label="Try again" pendingLabel="Retrying…" />
    </div>
  )
}
