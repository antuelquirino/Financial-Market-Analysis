import type { Metadata } from "next"
import { headers } from "next/headers"
import { ThemeProvider } from "next-themes"
import { IBM_Plex_Sans, Newsreader } from "next/font/google"
import "./globals.css"
import { siteConfig } from "./siteConfig"

import type { Locale } from "@/lib/locale"


// Interface and numbers: tabular figures, legible at small sizes.
const plex = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  display: "swap",
  variable: "--font-plex",
})

// Findings only: a serif made for reading news on screens.
const newsreader = Newsreader({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-newsreader",
})

export const metadata: Metadata = {
  title: { default: siteConfig.name, template: `%s · ${siteConfig.name}` },
  description: siteConfig.description,
  authors: [
    { name: "Antuel Quirino", url: "https://github.com/antuelquirino" },
  ],
  creator: "Antuel Quirino",
  openGraph: {
    type: "website",
    locale: "en_US",
    title: siteConfig.name,
    description: siteConfig.description,
    siteName: siteConfig.name,
  },
}

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  // Set by src/proxy.ts from the path: /es is Spanish, everything else English.
  const locale: Locale =
    (await headers()).get("x-locale") === "es" ? "es" : "en"
  return (
    <html
      lang={locale}
      className={`${plex.variable} ${newsreader.variable}`}
      suppressHydrationWarning
    >
      <body className="overflow-y-scroll font-sans antialiased">
        {/* Light by default, whatever the OS says; the reader's choice is remembered. */}
        <ThemeProvider
          attribute="class"
          defaultTheme="light"
          enableSystem={false}
        >
          <main className="mx-auto max-w-7xl px-4 pt-6 sm:px-6 lg:px-10 lg:pt-8">
            {children}
          </main>
        </ThemeProvider>
      </body>
    </html>
  )
}
