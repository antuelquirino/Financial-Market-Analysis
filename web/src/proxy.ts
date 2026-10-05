import { NextResponse, type NextRequest } from "next/server"

// Tells the root layout which language a page is in, so the server renders
// <html lang="es"> for /es. Screen readers use it to pick the pronunciation.
export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl
  const locale = pathname === "/es" || pathname.startsWith("/es/") ? "es" : "en"
  const headers = new Headers(request.headers)
  headers.set("x-locale", locale)
  return NextResponse.next({ request: { headers } })
}

export const config = {
  // pages only: skip Next's assets and files with an extension
  matcher: ["/((?!_next/|.*\\.[a-z0-9]+$).*)"],
}
