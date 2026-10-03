"use client"

import { RiArrowDownSLine } from "@remixicon/react"
import { usePathname, useRouter } from "next/navigation"
import { useTransition } from "react"

import { hrefFor, type Selection } from "@/lib/selection"
import type { AssetType, Ticker } from "@/lib/types"
import { cx, focusInput } from "@/lib/utils"

const GROUPS: { type: AssetType; label: string }[] = [
  { type: "stock", label: "Stocks" },
  { type: "sector_etf", label: "Sector ETFs" },
  { type: "benchmark", label: "Benchmarks" },
]

/**
 * The global ticker choice. A native <select>: keyboard, screen readers and
 * phones get their own well-known control. Changing it navigates, keeping the
 * period and the current screen.
 */
export function TickerSelect({
  tickers,
  selection,
}: {
  tickers: Ticker[]
  selection: Selection
}) {
  const router = useRouter()
  const pathname = usePathname()
  const [pending, startTransition] = useTransition()
  return (
    <div className="relative">
      <label htmlFor="ticker-select" className="sr-only">
        Ticker
      </label>
      <select
        id="ticker-select"
        value={selection.ticker}
        aria-busy={pending}
        onChange={(event) =>
          startTransition(() =>
            router.push(
              hrefFor(pathname, selection, { ticker: event.target.value }),
              { scroll: false },
            ),
          )
        }
        className={cx(
          "h-[34px] w-56 appearance-none rounded-md bg-none border border-rule bg-surface py-1 pr-8 pl-3 text-sm text-ink",
          "hover:bg-wash",
          focusInput,
          pending && "text-muted",
        )}
      >
        {GROUPS.map((group) => (
          <optgroup key={group.type} label={group.label}>
            {tickers
              .filter((t) => t.asset_type === group.type)
              .map((t) => (
                <option key={t.ticker} value={t.ticker}>
                  {t.ticker} · {shortName(t)}
                </option>
              ))}
          </optgroup>
        ))}
      </select>
      <RiArrowDownSLine
        className="pointer-events-none absolute top-1/2 right-2 size-4 -translate-y-1/2 text-graphite"
        aria-hidden="true"
      />
    </div>
  )
}

// "Technology Select Sector SPDR Fund" reads better as its sector.
function shortName(ticker: Ticker): string {
  if (ticker.asset_type === "sector_etf" && ticker.sector) return ticker.sector
  return ticker.name.replace(/,? (Inc\.|Corporation|& Co\.)$/, "")
}
