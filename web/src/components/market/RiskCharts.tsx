"use client"

// Two charts the Tremor template has no equivalent for, on Recharts with the
// same anatomy as LineChart: muted axes, a barely-there grid, a crosshair
// tooltip in ink, and no animation on load.

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { formatDate, formatPercent } from "@/lib/format"
import { MESSAGES } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import { drawdownTicks, type HistogramBar } from "@/lib/risk"
import { formatAxisDate, formatterFor } from "./valueFormats"

const AXIS_TICK = { fill: "var(--muted)", fontSize: 12 }

function TooltipBox({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-md border border-rule bg-surface text-sm">
      <p className="border-b border-rule p-2 font-medium text-ink">{title}</p>
      <div className="p-2 text-graphite tabular-nums">{children}</div>
    </div>
  )
}

/**
 * Drawdown: an area hanging from zero, so the depth of each fall reads as
 * distance below the waterline. Cobalt marks the subject; the fill stays light.
 */
export function DrawdownChart({
  data,
  locale = "en",
  className = "h-72",
}: {
  data: { date: string; drawdown: number }[]
  locale?: Locale
  className?: string
}) {
  const format = formatterFor("percent", locale)
  const ticks = drawdownTicks(Math.min(...data.map((p) => p.drawdown)))
  return (
    <div className={className}>
      <ResponsiveContainer>
        <AreaChart data={data} margin={{ top: 10, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--rule)" />
          <XAxis
            dataKey="date"
            tickFormatter={(value) => formatAxisDate(value, locale)}
            tick={AXIS_TICK}
            tickLine={false}
            axisLine={false}
            minTickGap={48}
            className="tabular-nums"
          />
          <YAxis
            width={56}
            domain={[ticks[ticks.length - 1], 0]}
            ticks={ticks}
            tickFormatter={format}
            tick={AXIS_TICK}
            tickLine={false}
            axisLine={false}
            className="tabular-nums"
          />
          <ReferenceLine y={0} stroke="var(--graphite)" strokeWidth={1} />
          <Tooltip
            cursor={{ stroke: "var(--rule)", strokeWidth: 1 }}
            isAnimationActive={false}
            content={({ active, payload, label }) =>
              active && payload?.length ? (
                <TooltipBox title={formatDate(String(label), "day", { locale })}>
                  {MESSAGES[locale].risk.belowThePeak(
                    formatPercent(Number(payload[0].value), { locale }),
                  )}
                </TooltipBox>
              ) : null
            }
          />
          <Area
            dataKey="drawdown"
            type="linear"
            stroke="var(--series-1)"
            strokeWidth={2}
            fill="var(--series-1)"
            fillOpacity={0.12}
            isAnimationActive={false}
            activeDot={{ r: 4, stroke: "var(--surface)", strokeWidth: 2, fill: "var(--series-1)" }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * Daily returns in half-point bins. Bars touch zero at the bottom, keep a 2px
 * gap between them, and a line marks the zero-return bin edge.
 */
export function ReturnHistogram({
  data,
  locale = "en",
  className = "h-72",
}: {
  data: HistogramBar[]
  locale?: Locale
  className?: string
}) {
  const zeroIndex = data.findIndex((bar) => bar.center > 0)
  return (
    <div className={className}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }} barCategoryGap={1}>
          <CartesianGrid vertical={false} stroke="var(--rule)" />
          <XAxis
            dataKey="center"
            type="number"
            domain={[
              (min: number) => min - 0.0025,
              (max: number) => max + 0.0025,
            ]}
            tickFormatter={(value: number) => formatPercent(value, { decimals: 0, signed: true, locale })}
            tick={AXIS_TICK}
            tickLine={false}
            axisLine={false}
            className="tabular-nums"
          />
          <YAxis
            width={40}
            allowDecimals={false}
            tick={AXIS_TICK}
            tickLine={false}
            axisLine={false}
            className="tabular-nums"
          />
          {zeroIndex >= 0 ? (
            <ReferenceLine x={0} stroke="var(--graphite)" strokeWidth={1} />
          ) : null}
          <Tooltip
            cursor={{ fill: "var(--wash)" }}
            isAnimationActive={false}
            content={({ active, payload }) => {
              const bar = payload?.[0]?.payload as HistogramBar | undefined
              return active && bar ? (
                <TooltipBox title={bar.label}>{bar.sessionsLabel}</TooltipBox>
              ) : null
            }}
          />
          <Bar
            dataKey="sessions"
            fill="var(--series-1)"
            radius={[2, 2, 0, 0]}
            isAnimationActive={false}
            stroke="var(--paper)"
            strokeWidth={1}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
