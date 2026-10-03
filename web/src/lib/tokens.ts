// The design tokens as data, for the /styleguide page. globals.css is the
// source of truth; tokens.test.ts fails if these values drift from it.

export interface ColorToken {
  name: string
  variable: string // CSS custom property, without the leading --
  role: string
  light: string
  dark: string
}

export const INTERFACE_COLORS: ColorToken[] = [
  {
    name: "Paper",
    variable: "paper",
    role: "Page background",
    light: "#f5f3ee",
    dark: "#1a1916",
  },
  {
    name: "Surface",
    variable: "surface",
    role: "Panels, charts and inputs",
    light: "#fdfcf9",
    dark: "#24221e",
  },
  {
    name: "Ink",
    variable: "ink",
    role: "Primary text",
    light: "#1f1d19",
    dark: "#f2eee6",
  },
  {
    name: "Graphite",
    variable: "graphite",
    role: "Secondary text",
    light: "#5b564c",
    dark: "#bbb3a4",
  },
  {
    name: "Muted",
    variable: "muted",
    role: "Axes and captions",
    light: "#726c60",
    dark: "#968f81",
  },
  {
    name: "Rule",
    variable: "rule",
    role: "Hairlines and borders",
    light: "#e3dfd4",
    dark: "#3a3731",
  },
  {
    name: "Cobalt",
    variable: "cobalt",
    role: "The accent: what to look at",
    light: "#2d5b9a",
    dark: "#5b8fe0",
  },
  {
    name: "Highlight",
    variable: "highlight",
    role: "The marker behind a key figure",
    light: "#d9e4f5",
    dark: "#1f3a5f",
  },
  {
    name: "Gain",
    variable: "gain",
    role: "Only for “improves”",
    light: "#1b7342",
    dark: "#5cc48a",
  },
  {
    name: "Loss",
    variable: "loss",
    role: "Only for “worsens”",
    light: "#b3372a",
    dark: "#f07a67",
  },
]

export const SERIES_COLORS: ColorToken[] = [
  {
    name: "Series 1 · Cobalt",
    variable: "series-1",
    role: "The selected ticker",
    light: "#2d5b9a",
    dark: "#5b8fe0",
  },
  {
    name: "Series 2 · Ochre",
    variable: "series-2",
    role: "Second series (1-year volatility)",
    light: "#a86422",
    dark: "#d57c11",
  },
  {
    name: "Series 3 · Teal",
    variable: "series-3",
    role: "Third series",
    light: "#1a9aa0",
    dark: "#1faaa4",
  },
  {
    name: "Series 4 · Plum",
    variable: "series-4",
    role: "Fourth series",
    light: "#8e3c89",
    dark: "#a947a3",
  },
  {
    name: "Context",
    variable: "series-muted",
    role: "The benchmark, next to the selected ticker",
    light: "#b7b0a2",
    dark: "#5d584f",
  },
]
