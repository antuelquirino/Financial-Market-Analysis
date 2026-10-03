"use client"

// Tremor Raw Input [v1.0.3]

import { RiEyeFill, RiEyeOffFill, RiSearchLine } from "@remixicon/react"
import React from "react"
import { tv, type VariantProps } from "tailwind-variants"

import { cx, focusInput, focusRing, hasErrorInput } from "@/lib/utils"

const inputStyles = tv({
  base: [
    // base
    "relative block w-full appearance-none truncate rounded-md border px-2.5 py-2 outline-hidden transition sm:text-sm",
    // border color
    "border-rule",
    // text color
    "text-ink",
    // placeholder color
    "placeholder-gray-400",
    // background color
    "bg-surface",
    // disabled
    "disabled:border-rule disabled:bg-wash disabled:text-muted",
    "",
    // file
    [
      "file:-my-2 file:-ml-2.5 file:cursor-pointer file:rounded-l-[5px] file:rounded-r-none file:border-0 file:px-3 file:py-2 file:outline-hidden focus:outline-hidden disabled:pointer-events-none disabled:file:pointer-events-none",
      "file:border-solid file:border-rule file:bg-wash file:text-muted file:hover:bg-wash",
      "file:me-3 file:[border-inline-end-width:1px]",
      "disabled:file:bg-wash disabled:file:text-muted",
    ],
    // focus
    focusInput,
    // invalid (optional)
    // " aria-invalid:ring-2 aria-invalid:ring-loss/20 aria-invalid:border-loss invalid:ring-2 invalid:ring-loss/20 invalid:border-loss"
    // remove search cancel button (optional)
    "[&::--webkit-search-cancel-button]:hidden [&::-webkit-search-cancel-button]:hidden [&::-webkit-search-decoration]:hidden",
  ],
  variants: {
    hasError: {
      true: hasErrorInput,
    },
    // number input
    enableStepper: {
      false:
        "[appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none",
    },
  },
})

interface InputProps
  extends
    React.InputHTMLAttributes<HTMLInputElement>,
    VariantProps<typeof inputStyles> {
  inputClassName?: string
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  (
    {
      className,
      inputClassName,
      hasError,
      enableStepper = true,
      type,
      ...props
    }: InputProps,
    forwardedRef,
  ) => {
    const [typeState, setTypeState] = React.useState(type)

    const isPassword = type === "password"
    const isSearch = type === "search"

    return (
      <div className={cx("relative w-full", className)}>
        <input
          ref={forwardedRef}
          type={isPassword ? typeState : type}
          className={cx(
            inputStyles({ hasError, enableStepper }),
            {
              "pl-8": isSearch,
              "pr-10": isPassword,
            },
            inputClassName,
          )}
          {...props}
        />
        {isSearch && (
          <div
            className={cx(
              // base
              "pointer-events-none absolute bottom-0 left-2 flex h-full items-center justify-center",
              // text color
              "text-muted",
            )}
          >
            <RiSearchLine className="size-4.5 shrink-0" aria-hidden="true" />
          </div>
        )}
        {isPassword && (
          <div
            className={cx(
              "absolute right-0 bottom-0 flex h-full items-center justify-center px-3",
            )}
          >
            <button
              aria-label="Change password visibility"
              className={cx(
                // base
                "h-fit w-fit rounded-xs outline-hidden transition-all",
                // text
                "text-muted",
                // hover
                "hover:text-muted",
                focusRing,
              )}
              type="button"
              onClick={() => {
                setTypeState(typeState === "password" ? "text" : "password")
              }}
            >
              <span className="sr-only">
                {typeState === "password" ? "Show password" : "Hide password"}
              </span>
              {typeState === "password" ? (
                <RiEyeFill aria-hidden="true" className="size-5 shrink-0" />
              ) : (
                <RiEyeOffFill aria-hidden="true" className="size-5 shrink-0" />
              )}
            </button>
          </div>
        )}
      </div>
    )
  },
)

Input.displayName = "Input"

export { Input, inputStyles, type InputProps }
