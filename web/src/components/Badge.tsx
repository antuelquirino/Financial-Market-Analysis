// Tremor Raw Badge [v0.0.0], on InsightFlow tokens.

import React from "react"
import { tv, type VariantProps } from "tailwind-variants"

import { cx } from "@/lib/utils"

// gain / loss mean "improves" / "worsens" and nothing else.
const badgeVariants = tv({
  base: cx(
    "inline-flex items-center gap-x-1 rounded-sm px-1.5 py-0.5 text-xs font-medium whitespace-nowrap tabular-nums",
  ),
  variants: {
    variant: {
      default: "bg-highlight text-ink",
      neutral: "bg-wash text-graphite",
      gain: "bg-gain/10 text-gain",
      loss: "bg-loss/10 text-loss",
    },
  },
  defaultVariants: {
    variant: "default",
  },
})

interface BadgeProps
  extends
    React.ComponentPropsWithoutRef<"span">,
    VariantProps<typeof badgeVariants> {}

const Badge = React.forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, variant, ...props }: BadgeProps, forwardedRef) => {
    return (
      <span
        ref={forwardedRef}
        className={cx(badgeVariants({ variant }), className)}
        {...props}
      />
    )
  },
)

Badge.displayName = "Badge"

export { Badge, badgeVariants, type BadgeProps }
