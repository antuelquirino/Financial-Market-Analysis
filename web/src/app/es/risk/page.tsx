import type { Metadata } from "next"
import { RiskScreen } from "@/components/screens/RiskScreen"
import { MESSAGES } from "@/lib/i18n"
import type { SearchParams } from "@/lib/selection"

export const metadata: Metadata = { title: "Riesgo", description: MESSAGES.es.meta.description }

export const dynamic = "force-dynamic"

export default function Page({
  searchParams,
}: {
  searchParams: Promise<SearchParams>
}) {
  return <RiskScreen locale="es" searchParams={searchParams} />
}
