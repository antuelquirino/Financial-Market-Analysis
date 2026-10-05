"use client"

import { RiMoonLine, RiSunLine } from "@remixicon/react"
import { useTheme } from "next-themes"

import { MESSAGES } from "@/lib/i18n"
import type { Locale } from "@/lib/locale"
import { cx, focusRing } from "@/lib/utils"

// Both icons are rendered and CSS shows the right one, so the server HTML and
// the first client render always match (no flash, no hydration mismatch).
export function ThemeToggle({
  locale = "en",
  className,
}: {
  locale?: Locale
  className?: string
}) {
  const { resolvedTheme, setTheme } = useTheme()
  const t = MESSAGES[locale].theme
  return (
    <button
      type="button"
      onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
      className={cx(
        "inline-flex items-center gap-2 rounded-md px-2 py-1.5 text-sm text-graphite transition-colors hover:bg-wash hover:text-ink",
        focusRing,
        className,
      )}
    >
      <RiMoonLine className="size-4 dark:hidden" aria-hidden="true" />
      <RiSunLine className="hidden size-4 dark:block" aria-hidden="true" />
      <span className="dark:hidden">{t.toDark}</span>
      <span className="hidden dark:inline">{t.toLight}</span>
    </button>
  )
}
