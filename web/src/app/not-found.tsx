import { Button } from "@/components/Button"
import { ArrowAnimated } from "@/components/ui/icons/ArrowAnimated"
import Link from "next/link"
import { siteConfig } from "./siteConfig"

export default function NotFound() {
  return (
    <div className="flex h-screen flex-col items-center justify-center">
      <p className="text-4xl font-semibold text-cobalt sm:text-5xl">404</p>
      <h1 className="mt-4 text-2xl font-semibold text-ink">Page not found</h1>
      <p className="mt-2 text-sm text-graphite">
        Sorry, we couldn’t find the page you’re looking for.
      </p>
      <Button asChild className="group mt-8" variant="light">
        <Link href={siteConfig.baseLinks.home}>
          Go to the overview
          <ArrowAnimated className="stroke-ink" aria-hidden="true" />
        </Link>
      </Button>
    </div>
  )
}
