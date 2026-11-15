"use client"

import * as React from "react"
import { XIcon } from "lucide-react"

import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"

export interface ToastMessage {
  id: number
  text: string
  action?: { label: string; onClick: () => void }
}

function Toast({
  toast,
  onDismiss,
  disabled,
  className,
  ...props
}: Omit<React.ComponentProps<"div">, "children"> & {
  toast: ToastMessage
  onDismiss: () => void
  disabled?: boolean
}) {
  const { action } = toast
  return (
    <div
      data-slot="toast"
      className={cn(
        "pointer-events-auto flex w-full max-w-sm items-center gap-2 rounded-lg border bg-overlay py-2 pr-2 pl-4 shadow-(--shadow-overlay) animate-in fade-in-0 slide-in-from-bottom-2 duration-(--dur-4) ease-(--ease-enter)",
        className
      )}
      {...props}
    >
      <p className="label-13 min-w-0 flex-1 text-fg">{toast.text}</p>
      {action && (
        <Button size="sm" variant="outline" disabled={disabled} onClick={() => { action.onClick(); onDismiss() }}>
          {action.label}
        </Button>
      )}
      <Button size="icon-sm" variant="ghost" aria-label="Dismiss" onClick={onDismiss}>
        <XIcon />
      </Button>
    </div>
  )
}

function TimedToast({ toast, onDismiss, duration, disabled }: { toast: ToastMessage; onDismiss: () => void; duration: number; disabled?: boolean }) {
  const [held, setHeld] = React.useState(false)
  const ref = React.useRef<HTMLDivElement>(null)
  // The control that started the action may be gone; put focus on its Undo.
  React.useEffect(() => { if (toast.action) ref.current?.querySelector("button")?.focus() }, [toast.action])
  const dismiss = React.useRef(onDismiss)
  React.useEffect(() => { dismiss.current = onDismiss })
  React.useEffect(() => {
    if (held) return
    const timer = setTimeout(() => dismiss.current(), duration)
    return () => clearTimeout(timer)
  }, [held, duration])
  // Hold the timer while the pointer or focus is inside, so Undo stays reachable.
  return (
    <Toast
      ref={ref}
      toast={toast}
      onDismiss={onDismiss}
      disabled={disabled}
      onPointerEnter={() => setHeld(true)}
      onPointerLeave={() => setHeld(false)}
      onFocus={() => setHeld(true)}
      onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) setHeld(false) }}
    />
  )
}

/** Keep it mounted: a live region announces only text inserted after it exists. */
function Toaster({
  toast,
  onDismiss,
  duration = 6000,
  disabled,
  className,
}: {
  toast: ToastMessage | null
  onDismiss: () => void
  duration?: number
  disabled?: boolean
  className?: string
}) {
  return (
    <div
      role="status"
      aria-label="Notifications"
      data-slot="toaster"
      className={cn(
        "pointer-events-none absolute inset-x-4 bottom-4 z-50 flex justify-center md:inset-x-auto md:right-6 md:bottom-6",
        className
      )}
    >
      {toast && <TimedToast key={toast.id} toast={toast} onDismiss={onDismiss} duration={duration} disabled={disabled} />}
    </div>
  )
}

export { Toast, Toaster }
