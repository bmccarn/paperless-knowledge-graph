import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const alertVariants = cva(
  "w-full rounded-lg border border-l-2 px-4 py-3 label-13 text-fg [&_a]:underline [&_a]:underline-offset-3",
  {
    variants: {
      variant: {
        info: "border-border border-l-qualified-line bg-qualified-bg",
        warning: "border-border border-l-partial-line bg-partial-bg",
        error: "border-border border-l-error-line bg-error-bg",
        success: "border-border border-l-verified-line bg-verified-bg",
      },
    },
    defaultVariants: {
      variant: "info",
    },
  }
)

function Alert({
  className,
  variant,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof alertVariants>) {
  return (
    <div
      data-slot="alert"
      role={variant === "error" ? "alert" : "status"}
      className={cn(alertVariants({ variant }), className)}
      {...props}
    />
  )
}

export { Alert, alertVariants }
