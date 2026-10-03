import type { Shell } from "@/lib/shell"
import { SiteFooter, SiteHeader, type Screen as ScreenId } from "./SiteChrome"

/** The frame shared by the three screens: header, content, footer with the disclaimer. */
export function Screen({
  id,
  shell,
  children,
}: {
  id: ScreenId
  shell: Shell
  children: React.ReactNode
}) {
  return (
    <div className="space-y-10 pb-16">
      <SiteHeader
        screen={id}
        selection={shell.selection}
        tickers={shell.tickers}
        status={shell.status}
      />
      {children}
      <SiteFooter />
    </div>
  )
}
