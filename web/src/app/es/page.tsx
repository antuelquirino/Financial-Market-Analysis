import type { Metadata } from "next"
import { OverviewScreen } from "@/components/screens/OverviewScreen"
import { MESSAGES } from "@/lib/i18n"
import type { SearchParams } from "@/lib/selection"

export const metadata: Metadata = { description: MESSAGES.es.meta.description }

export const dynamic = "force-dynamic"

export default function Page({
  searchParams,
}: {
  searchParams: Promise<SearchParams>
}) {
  return <OverviewScreen locale="es" searchParams={searchParams} />
}
