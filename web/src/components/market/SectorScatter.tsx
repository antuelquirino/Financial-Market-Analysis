"use client"

// Risk against return, one dot per sector ETF. Context dots are muted; the
// selected ticker's sector is cobalt; the benchmark is an ink ring, with
// dashed guides through it so each quadrant reads "more or less return and
// risk than the market". Every dot carries its symbol as a direct label.

import {
  CartesianGrid,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { formatNumber, formatPercent } from "@/lib/format"
import { MESSAGES } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import { labelSides, type ScatterPoint } from "@/lib/sectors"

const AXIS_TICK = { fill: "var(--muted)", fontSize: 12 }

const FILL: Record<ScatterPoint["role"], string> = {
  sector: "var(--series-muted)",
  highlight: "var(--series-1)",
  benchmark: "var(--surface)",
}

function Dot(props: { cx?: number; cy?: number; payload?: ScatterPoint }) {
  const { cx = 0, cy = 0, payload } = props
  if (!payload) return null
  const benchmark = payload.role === "benchmark"
  return (
    <circle
      cx={cx}
      cy={cy}
      r={payload.role === "highlight" ? 7 : 6}
      fill={FILL[payload.role]}
      // a 2px surface ring keeps overlapping dots apart; the benchmark is an ink ring
      stroke={benchmark ? "var(--ink)" : "var(--surface)"}
      strokeWidth={2}
    />
  )
}

export function SectorScatter({
  data,
  locale = "en",
  className = "h-96",
}: {
  data: ScatterPoint[]
  locale?: Locale
  className?: string
}) {
  const t = MESSAGES[locale].sectors
  const benchmark = data.find((p) => p.role === "benchmark")
  // Draw the highlight last so it sits on top.
  const ordered = [...data].sort(
    (a, b) => Number(a.role === "highlight") - Number(b.role === "highlight"),
  )
  const sides = labelSides(ordered)
  return (
    <div className={className}>
      <ResponsiveContainer>
        <ScatterChart margin={{ top: 12, right: 32, bottom: 24, left: 8 }}>
          <CartesianGrid stroke="var(--rule)" />
          <XAxis
            type="number"
            dataKey="volatility"
            name={t.volatility}
            domain={["auto", "auto"]}
            tickFormatter={(v: number) => formatPercent(v, { decimals: 0, locale })}
            tick={AXIS_TICK}
            tickLine={false}
            axisLine={false}
            label={{
              value: t.volatilityAxis,
              position: "insideBottom",
              offset: -16,
              fill: "var(--graphite)",
              fontSize: 12,
            }}
          />
          <YAxis
            type="number"
            dataKey="cagr"
            name={t.annualizedReturn}
            domain={["auto", "auto"]}
            width={56}
            tickFormatter={(v: number) =>
              formatPercent(v, { decimals: 0, signed: true, locale })
            }
            tick={AXIS_TICK}
            tickLine={false}
            axisLine={false}
          />
          {benchmark ? (
            <>
              <ReferenceLine
                x={benchmark.volatility}
                stroke="var(--muted)"
                strokeDasharray="3 3"
              />
              <ReferenceLine
                y={benchmark.cagr}
                stroke="var(--muted)"
                strokeDasharray="3 3"
              />
            </>
          ) : null}
          <ReferenceLine y={0} stroke="var(--graphite)" />
          <Tooltip
            cursor={false}
            isAnimationActive={false}
            content={({ active, payload }) => {
              const p = payload?.[0]?.payload as ScatterPoint | undefined
              return active && p ? (
                <div className="rounded-md border border-rule bg-surface text-sm">
                  <p className="border-b border-rule p-2 font-medium text-ink">
                    {p.ticker} · {p.label}
                  </p>
                  <dl className="grid grid-cols-[auto_auto] gap-x-4 gap-y-0.5 p-2 text-graphite tabular-nums">
                    <dt>{t.annualizedReturn}</dt>
                    <dd className="text-right text-ink">
                      {formatPercent(p.cagr, { signed: true, locale })}
                    </dd>
                    <dt>{t.volatility}</dt>
                    <dd className="text-right text-ink">
                      {formatPercent(p.volatility, { locale })}
                    </dd>
                    <dt>{t.sharpe}</dt>
                    <dd className="text-right text-ink">
                      {formatNumber(p.sharpe, { locale })}
                    </dd>
                  </dl>
                </div>
              ) : null
            }}
          />
          <Scatter data={ordered} shape={<Dot />} isAnimationActive={false}>
            <LabelList
              dataKey="ticker"
              content={(props) => {
                const { x, y, width, height, value, index } = props
                const cx = Number(x) + Number(width ?? 0) / 2
                const cy = Number(y) + Number(height ?? 0) / 2
                const left = sides[Number(index)] === "left"
                return (
                  <text
                    x={cx + (left ? -11 : 11)}
                    y={cy}
                    dy="0.35em"
                    textAnchor={left ? "end" : "start"}
                    fill="var(--graphite)"
                    fontSize={12}
                  >
                    {value}
                  </text>
                )
              }}
            />
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  )
}
