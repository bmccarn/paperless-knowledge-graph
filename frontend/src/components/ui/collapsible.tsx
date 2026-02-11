"use client"

import * as React from "react"
import { cn } from "@/lib/utils"

interface CollapsibleProps {
  open?: boolean
  onOpenChange?: (open: boolean) => void
  children: React.ReactNode
  className?: string
}

function Collapsible({ open, onOpenChange, children, className }: CollapsibleProps) {
  const [isOpen, setIsOpen] = React.useState(open ?? false)
  const actualOpen = open !== undefined ? open : isOpen
  const handleToggle = () => {
    const newOpen = !actualOpen
    setIsOpen(newOpen)
    onOpenChange?.(newOpen)
  }
  return (
    <div className={className} data-state={actualOpen ? "open" : "closed"}>
      {React.Children.map(children, (child) => {
        if (React.isValidElement(child)) {
          if (child.type === CollapsibleTrigger) {
            return React.cloneElement(child as React.ReactElement<{ onClick?: () => void }>, { onClick: handleToggle })
          }
          if (child.type === CollapsibleContent) {
            return actualOpen ? child : null
          }
        }
        return child
      })}
    </div>
  )
}

function CollapsibleTrigger({ children, onClick, className, ...props }: React.HTMLAttributes<HTMLButtonElement> & { onClick?: () => void }) {
  return (
    <button type="button" className={cn("flex w-full items-center", className)} onClick={onClick} {...props}>
      {children}
    </button>
  )
}

function CollapsibleContent({ children, className }: { children: React.ReactNode; className?: string }) {
  return <div className={cn("overflow-hidden", className)}>{children}</div>
}

export { Collapsible, CollapsibleTrigger, CollapsibleContent }
