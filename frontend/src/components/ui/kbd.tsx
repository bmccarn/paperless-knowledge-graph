import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";

export function Kbd({ className, ...props }: ComponentProps<"kbd">) {
  return <kbd className={cn("label-12 inline-flex items-center rounded-sm border border-border-strong bg-raised px-1.5 py-0.5 font-sans text-fg-muted", className)} {...props} />;
}
