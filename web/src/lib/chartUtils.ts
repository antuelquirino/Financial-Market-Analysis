// Tremor Raw chartColors [v0.0.0]

export type ColorUtility = "bg" | "stroke" | "fill" | "text"

// Chart colors map to the design tokens in globals.css. The four series colors
// were validated together, in this order, for colorblind separation and
// contrast in both modes; "muted" is for context series next to a highlighted
// one, and gain/loss only for changes that mean "improves" / "worsens".
export const chartColors = {
  cobalt: {
    bg: "bg-series-1",
    stroke: "stroke-series-1",
    fill: "fill-series-1",
    text: "text-series-1",
  },
  ochre: {
    bg: "bg-series-2",
    stroke: "stroke-series-2",
    fill: "fill-series-2",
    text: "text-series-2",
  },
  teal: {
    bg: "bg-series-3",
    stroke: "stroke-series-3",
    fill: "fill-series-3",
    text: "text-series-3",
  },
  plum: {
    bg: "bg-series-4",
    stroke: "stroke-series-4",
    fill: "fill-series-4",
    text: "text-series-4",
  },
  muted: {
    bg: "bg-series-muted",
    stroke: "stroke-series-muted",
    fill: "fill-series-muted",
    text: "text-muted",
  },
  gain: {
    bg: "bg-gain",
    stroke: "stroke-gain",
    fill: "fill-gain",
    text: "text-gain",
  },
  loss: {
    bg: "bg-loss",
    stroke: "stroke-loss",
    fill: "fill-loss",
    text: "text-loss",
  },
} as const satisfies {
  [color: string]: {
    [key in ColorUtility]: string
  }
}

export type AvailableChartColorsKeys = keyof typeof chartColors

// Series colors in their validated order.
export const AvailableChartColors: AvailableChartColorsKeys[] = [
  "cobalt",
  "ochre",
  "teal",
  "plum",
]

// Colors are assigned in order and never cycled: a category past the last
// color becomes a muted context series instead of repeating a hue.
export const constructCategoryColors = (
  categories: string[],
  colors: AvailableChartColorsKeys[],
): Map<string, AvailableChartColorsKeys> => {
  const categoryColors = new Map<string, AvailableChartColorsKeys>()
  categories.forEach((category, index) => {
    categoryColors.set(category, colors[index] ?? "muted")
  })
  return categoryColors
}

// The CSS variable behind each chart color, for SVG fills set in code.
const CSS_VARIABLES: Record<AvailableChartColorsKeys, string> = {
  cobalt: "--series-1",
  ochre: "--series-2",
  teal: "--series-3",
  plum: "--series-4",
  muted: "--series-muted",
  gain: "--gain",
  loss: "--loss",
}

export const cssColor = (color: AvailableChartColorsKeys): string =>
  `var(${CSS_VARIABLES[color]})`

export const getColorClassName = (
  color: AvailableChartColorsKeys,
  type: ColorUtility,
): string => chartColors[color]?.[type] ?? chartColors.muted[type]

// Tremor Raw getYAxisDomain [v0.0.0]

export const getYAxisDomain = (
  autoMinValue: boolean,
  minValue: number | undefined,
  maxValue: number | undefined,
) => {
  const minDomain = autoMinValue ? "auto" : (minValue ?? 0)
  const maxDomain = maxValue ?? "auto"
  return [minDomain, maxDomain]
}

// Tremor Raw hasOnlyOneValueForKey [v0.1.0]

// One row of chart data: the x-axis value plus one value per series.
export type ChartDatum = Record<string, string | number | null | undefined>

export function hasOnlyOneValueForKey(
  array: ChartDatum[],
  keyToCheck: string,
): boolean {
  const val: ChartDatum[string][] = []

  for (const obj of array) {
    if (Object.prototype.hasOwnProperty.call(obj, keyToCheck)) {
      val.push(obj[keyToCheck])
      if (val.length > 1) {
        return false
      }
    }
  }

  return true
}
