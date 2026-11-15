import { BadgeCheck, CircleDashed, CircleMinus, Clock, Contrast, Lock, RefreshCw, TriangleAlert, type LucideIcon } from "lucide-react";
import type { AnswerState, ClaimResult, EvidenceLevel as Level } from "@/lib/answer-state";
import { cn } from "@/lib/utils";

/** Word, icon, text token and rule token for each answer state and claim result. */
export const STATUSES: Record<AnswerState | ClaimResult, { word: string; icon: LucideIcon; color: string; line: string }> = {
  verified: { word: "Verified", icon: BadgeCheck, color: "var(--verified)", line: "var(--verified-line)" },
  documented: { word: "Documented", icon: Clock, color: "var(--qualified)", line: "var(--qualified-line)" },
  partial: { word: "Partial", icon: Contrast, color: "var(--partial)", line: "var(--partial-line)" },
  withheld: { word: "Withheld", icon: Lock, color: "var(--withheld)", line: "var(--withheld-line)" },
  not_checked: { word: "Not checked", icon: CircleDashed, color: "var(--withheld)", line: "var(--partial-line)" },
  retry: { word: "Retry needed", icon: RefreshCw, color: "var(--partial)", line: "var(--partial-line)" },
  failed: { word: "Failed", icon: TriangleAlert, color: "var(--error)", line: "var(--error-line)" },
  supported: { word: "Supported", icon: BadgeCheck, color: "var(--verified)", line: "var(--verified-line)" },
  unsupported: { word: "Unsupported", icon: Lock, color: "var(--withheld)", line: "var(--withheld-line)" },
  conflicting: { word: "Conflicting", icon: TriangleAlert, color: "var(--error)", line: "var(--error-line)" },
  left_out: { word: "Left out", icon: CircleMinus, color: "var(--partial)", line: "var(--partial-line)" },
};

export function StatusLabel({ status, size = "md", className }: { status: AnswerState | ClaimResult; size?: "sm" | "md"; className?: string }) {
  const { word, icon: Icon, color } = STATUSES[status];
  return (
    <span data-status={status} style={{ color }}
      className={cn("inline-flex items-center font-semibold", size === "sm" ? "gap-1 label-12" : "gap-1.5 label-13", className)}>
      <Icon aria-hidden className={size === "sm" ? "size-3.5" : "size-4"} strokeWidth={1.75} />
      {word}
    </span>
  );
}

/** The level word plus a three-segment meter; never a percentage. */
export function EvidenceLevel({ level, className }: { level: Level; className?: string }) {
  const filled = { low: 1, medium: 2, high: 3 }[level];
  return (
    <span role="img" aria-label={`Evidence ${level}`} data-level={level}
      className={cn("inline-flex items-center gap-2 label-13 text-fg-muted", className)}>
      <span aria-hidden className="flex gap-0.5">
        {[1, 2, 3].map(segment => (
          <span key={segment} className={cn("h-2 w-2.5 rounded-[1px] border border-border-strong", segment <= filled && "border-fg-muted bg-fg-muted")} />
        ))}
      </span>
      Evidence {level}
    </span>
  );
}
