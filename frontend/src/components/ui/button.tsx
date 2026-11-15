import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"
import { Slot } from "radix-ui"
import { LoaderCircle } from "lucide-react"

import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "relative inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors duration-(--dur-2) active:duration-(--dur-1) max-md:min-h-11 data-[size^=icon]:max-md:min-w-11 disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg:not([class*='size-'])]:size-4 shrink-0 [&_svg]:shrink-0 aria-invalid:border-error",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground hover:bg-primary/90",
        destructive:
          "border border-error-line bg-transparent text-error hover:bg-error-bg",
        outline:
          "border border-input bg-surface hover:bg-raised",
        secondary:
          "border border-input bg-secondary text-secondary-foreground hover:bg-sunken",
        ghost:
          "hover:bg-accent hover:text-accent-foreground",
        link: "text-foreground underline underline-offset-4",
      },
      size: {
        default: "h-9 px-4 py-2 has-[>svg]:px-3 has-[>span:first-child>svg]:px-3",
        xs: "h-7 gap-1 rounded-md px-2 text-xs has-[>svg]:px-1.5 has-[>span:first-child>svg]:px-1.5 [&_svg:not([class*='size-'])]:size-3",
        sm: "h-8 rounded-md gap-1.5 px-3 has-[>svg]:px-2.5 has-[>span:first-child>svg]:px-2.5",
        lg: "h-11 rounded-md px-6 has-[>svg]:px-4 has-[>span:first-child>svg]:px-4",
        icon: "size-9",
        "icon-xs": "size-7 rounded-md [&_svg:not([class*='size-'])]:size-3",
        "icon-sm": "size-8",
        "icon-lg": "size-11",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

function Button({
  className,
  variant = "default",
  size = "default",
  asChild = false,
  loading = false,
  disabled,
  children,
  ...props
}: React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & {
    asChild?: boolean
    loading?: boolean
  }) {
  const Comp = asChild ? Slot.Root : "button"

  return (
    <Comp
      data-slot="button"
      data-variant={variant}
      data-size={size}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      className={cn(buttonVariants({ variant, size, className }))}
      {...props}
    >
      {loading && !asChild ? <>
        <span className="inline-flex items-center gap-[inherit] opacity-0">{children}</span>
        <span className="absolute inset-0 flex items-center justify-center" aria-hidden="true"><LoaderCircle className="animate-spin motion-reduce:animate-none" /></span>
      </> : children}
    </Comp>
  )
}

export { Button, buttonVariants }
