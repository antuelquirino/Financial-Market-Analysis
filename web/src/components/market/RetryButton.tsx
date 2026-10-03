"use client"

import { Button } from "@/components/Button"
import { useRouter } from "next/navigation"
import { useTransition } from "react"

/** Re-runs the page's server fetches without a full reload. */
export function RetryButton({
  label,
  pendingLabel,
  className,
}: {
  label: string
  pendingLabel: string
  className?: string
}) {
  const router = useRouter()
  const [pending, startTransition] = useTransition()
  return (
    <Button
      variant="secondary"
      className={className}
      disabled={pending}
      onClick={() => startTransition(() => router.refresh())}
    >
      {pending ? pendingLabel : label}
    </Button>
  )
}
