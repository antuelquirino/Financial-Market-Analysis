// Tremor Raw cx [v0.0.0]

import clsx, { type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cx(...args: ClassValue[]) {
  return twMerge(clsx(...args))
}

// Tremor Raw focusInput [v0.0.1]

export const focusInput = [
  // base
  "focus:ring-2",
  // ring color
  "focus:ring-highlight",
  // border color
  "focus:border-cobalt",
]

// Tremor Raw focusRing [v0.0.1]

export const focusRing = [
  // base
  "outline-solid outline-offset-2 outline-0 focus-visible:outline-2",
  // outline color
  "outline-cobalt",
]

// Tremor Raw hasErrorInput [v0.0.1]

export const hasErrorInput = [
  // base
  "ring-2",
  // border color
  "border-loss",
  // ring color
  "ring-loss/20",
]
