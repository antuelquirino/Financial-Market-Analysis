import { cx } from "@/lib/utils"

/**
 * The one bold element of the design: each screen opens with its finding as a
 * sentence, and the key figure in it wears a cobalt marker stroke, the way an
 * analyst would highlight a printed report.
 */
export function LeadFinding({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}) {
  return (
    <p
      className={cx(
        "max-w-3xl font-serif text-[1.625rem] leading-tight text-balance text-ink sm:text-finding-lead",
        className,
      )}
    >
      {children}
    </p>
  )
}

/** The marker stroke: a <mark>, so assistive tech announces the emphasis too. */
export function Mark({ children }: { children: React.ReactNode }) {
  return <mark className="marker">{children}</mark>
}
