import { describe, expect, it } from "vitest"

import {
  formatDate,
  formatNumber,
  formatPercent,
  formatPoints,
  formatPrice,
  toneOf,
  yearsBetween,
} from "./format"

describe("formatPercent", () => {
  it("formats fractions", () => {
    expect(formatPercent(0.2515)).toBe("25.2%")
    expect(formatPercent(16.416)).toBe("1,641.6%")
    expect(formatPercent(-0.202)).toBe("−20.2%")
  })

  it("adds a plus sign when asked", () => {
    expect(formatPercent(0.031, { signed: true })).toBe("+3.1%")
    expect(formatPercent(-0.031, { signed: true })).toBe("−3.1%")
  })

  it("never shows a signed zero", () => {
    expect(formatPercent(-0.00001, { signed: true })).toBe("0.0%")
    expect(formatPercent(0, { signed: true })).toBe("0.0%")
  })

  it("shows a dash for missing values", () => {
    expect(formatPercent(null)).toBe("—")
    expect(formatPercent(Number.NaN)).toBe("—")
  })
})

describe("other numbers", () => {
  it("formats points, prices and plain numbers", () => {
    expect(formatPoints(0.0312)).toBe("+3.1 pts")
    expect(formatPoints(-0.1)).toBe("−10.0 pts")
    expect(formatPrice(5734.123)).toBe("$5,734.12")
    expect(formatNumber(1.164)).toBe("1.16")
    expect(formatNumber(-0.3)).toBe("−0.30")
  })
})

describe("dates", () => {
  it("formats ISO dates without time-zone drift", () => {
    expect(formatDate("2026-09-29")).toBe("Sep 29, 2026")
    expect(formatDate("2026-01-01", "month")).toBe("Jan 2026")
    expect(formatDate("2026-09-29", "long")).toBe("September 29, 2026")
    expect(formatDate(null)).toBe("—")
  })

  it("measures years", () => {
    expect(yearsBetween("2023-10-02", "2026-10-02")).toBeCloseTo(3, 2)
  })
})

describe("toneOf", () => {
  it("says whether a change is good news", () => {
    expect(toneOf(0.1)).toBe("gain")
    expect(toneOf(-0.1)).toBe("loss")
    expect(toneOf(0.1, false)).toBe("loss")
    expect(toneOf(0)).toBe("neutral")
    expect(toneOf(null)).toBe("neutral")
  })
})
