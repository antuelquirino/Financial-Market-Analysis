// Tremor Raw Button [v0.1.1]

import { Slot } from "@radix-ui/react-slot"
import { RiLoader2Fill } from "@remixicon/react"
import React from "react"
import { tv, type VariantProps } from "tailwind-variants"

import { cx, focusRing } from "@/lib/utils"

// No shadows anywhere: buttons are flat, separated by color and a hairline.
const buttonVariants = tv({
  base: [
    // base
    "relative inline-flex items-center justify-center rounded-md border px-3 py-2 text-center text-sm font-medium whitespace-nowrap transition-colors duration-100 ease-in-out",
    // disabled
    "disabled:pointer-events-none disabled:opacity-50",
    // focus
    focusRing,
  ],
  variants: {
    variant: {
      // The one call to action on a view, in the accent.
      primary: "border-transparent bg-cobalt text-on-cobalt hover:bg-cobalt/90",
      secondary: "border-rule bg-surface text-ink hover:bg-wash",
      light: "border-transparent bg-wash text-ink hover:bg-rule",
      ghost: "border-transparent bg-transparent text-ink hover:bg-wash",
    },
  },
  defaultVariants: {
    variant: "primary",
  },
})

interface ButtonProps
  extends
    React.ComponentPropsWithoutRef<"button">,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean
  isLoading?: boolean
  loadingText?: string
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      asChild,
      isLoading = false,
      loadingText,
      className,
      disabled,
      variant,
      children,
      ...props
    }: ButtonProps,
    forwardedRef,
  ) => {
    const Component = asChild ? Slot : "button"
    return (
      <Component
        ref={forwardedRef}
        className={cx(buttonVariants({ variant }), className)}
        disabled={disabled || isLoading}
        {...props}
      >
        {isLoading ? (
          <span className="pointer-events-none flex shrink-0 items-center justify-center gap-1.5">
            <RiLoader2Fill
              className="size-4 shrink-0 animate-spin"
              aria-hidden="true"
            />
            <span className="sr-only">
              {loadingText ? loadingText : "Loading"}
            </span>
            {loadingText ? loadingText : children}
          </span>
        ) : (
          children
        )}
      </Component>
    )
  },
)

Button.displayName = "Button"

export { Button, buttonVariants, type ButtonProps }
