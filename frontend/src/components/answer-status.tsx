import { EvidenceLevel, StatusLabel } from "@/components/status-label";
import type { AnswerState, EvidenceLevel as Level } from "@/lib/answer-state";

/** The answer's state header: state, reason sentence, evidence level and elapsed time. */
export function VerdictLine({ state, reason, level, seconds }: { state: AnswerState; reason: string; level?: Level; seconds?: number }) {
  return (
    <div data-testid="verdict" className="flex flex-wrap items-center gap-x-[18px] gap-y-1.5 border-y border-border pt-2.5 pb-3 label-13 text-fg-muted">
      <StatusLabel status={state} />
      <span>{reason}</span>
      {level && <EvidenceLevel level={level} />}
      {seconds != null && <span className="tabular-nums">{seconds.toFixed(1)} s</span>}
    </div>
  );
}

/** The live line while a question runs; never part of the answer frame. Reduced motion shows "Working" instead of the dots. */
export function StreamStatus({ step, seconds }: { step: string; seconds: number }) {
  return (
    <div role="status" className="flex items-center gap-3 label-13 text-fg-muted">
      <span data-testid="typing-dots" aria-hidden className="flex gap-1 motion-reduce:hidden">
        {[0, 1, 2].map(dot => <span key={dot} className="typing-dot size-1.5 rounded-full bg-fg-muted" />)}
      </span>
      <span className="hidden font-medium text-fg motion-reduce:inline">Working</span>
      <span>{step}</span>
      {/* Hidden from the live region so screen readers hear each step, not every second. */}
      <span aria-hidden className="tabular-nums">{seconds} s</span>
    </div>
  );
}
