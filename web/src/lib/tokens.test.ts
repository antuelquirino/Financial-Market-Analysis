import { readFileSync } from "node:fs"
import { join } from "node:path"
import { describe, expect, it } from "vitest"

import { INTERFACE_COLORS, SERIES_COLORS } from "./tokens"

const css = readFileSync(join(__dirname, "../app/globals.css"), "utf8")

function block(selector: string): string {
  const start = css.indexOf(`${selector} {`)
  return css.slice(start, css.indexOf("\n}", start))
}

describe("the styleguide shows the real token values", () => {
  const light = block(":root")
  const dark = block(".dark")

  it.each([...INTERFACE_COLORS, ...SERIES_COLORS])("$name", (token) => {
    expect(light).toMatch(new RegExp(`--${token.variable}: ${token.light};`))
    expect(dark).toMatch(new RegExp(`--${token.variable}: ${token.dark};`))
  })
})
