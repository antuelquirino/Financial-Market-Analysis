import type { Metadata } from "next"
import { SectorsScreen } from "@/components/screens/SectorsScreen"
import { MESSAGES } from "@/lib/i18n"
import type { SearchParams } from "@/lib/selection"

export const metadata: Metadata = { title: "Sectores", description: MESSAGES.es.meta.description }

export const dynamic = "force-dynamic"

export default function Page({
  searchParams,
}: {
  searchParams: Promise<SearchParams>
}) {
  return <SectorsScreen locale="es" searchParams={searchParams} />
}
