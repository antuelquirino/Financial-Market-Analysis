"use client"

import { LineChart } from "@/components/LineChart"
import type { AvailableChartColorsKeys, ChartDatum } from "@/lib/chartUtils"
import type { Locale } from "@/lib/locale"
import {
  formatAxisDate,
  formatterFor,
  formatTooltipDate,
  type ValueFormat,
} from "./valueFormats"

export type { ValueFormat } from "./valueFormats"

/** A daily line chart on the design system: dates on the x axis, one format for values. */
export function MetricLineChart({
  data,
  categories,
  colors,
  valueFormat,
  locale = "en",
  showLegend,
  className,
}: {
  data: ChartDatum[]
  categories: string[]
  colors?: AvailableChartColorsKeys[]
  valueFormat: ValueFormat
  locale?: Locale
  showLegend?: boolean
  className?: string
}) {
  return (
    <LineChart
      data={data}
      index="date"
      categories={categories}
      colors={colors}
      valueFormatter={formatterFor(valueFormat, locale)}
      indexFormatter={(value) => formatAxisDate(value, locale)}
      tooltipIndexFormatter={(value) => formatTooltipDate(value, locale)}
      // a single series needs no legend: the section title names it
      showLegend={showLegend ?? categories.length > 1}
      autoMinValue
      tickGap={48}
      connectNulls
      className={className ?? "h-72"}
    />
  )
}
