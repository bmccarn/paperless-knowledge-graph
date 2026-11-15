/** Answer states and evidence levels. The state comes from the backend disposition, never from the answer text. */
export type AnswerState = "verified" | "documented" | "partial" | "withheld" | "not_checked" | "retry" | "failed";
export type ClaimResult = "supported" | "unsupported" | "conflicting" | "left_out";
export type EvidenceLevel = "high" | "medium" | "low";

const STATES = new Map(Object.entries<AnswerState>({
  supported: "verified",
  qualified: "documented",
  partial: "partial",
  unsupported: "withheld",
  incomplete: "withheld",
  current_unresolved: "withheld",
  timeout: "withheld",
  audit_failed: "withheld",
  unaudited: "not_checked",
  stored_binding_unavailable: "not_checked",
  corpus_changed: "retry",
}));

// Reasons that need no counts, dates or missing-evidence text from the answer.
const REASONS = new Map(Object.entries({
  unaudited: "Fast mode does not check claims. Verify against the sources before relying on this.",
  stored_binding_unavailable: "This saved answer could not be re-verified. Ask again to check it.",
  corpus_changed: "The document index changed during the check. Ask again after indexing finishes.",
}));

/**
 * Map the final `complete` event, or the stored message, to its state.
 * Pass null when no complete event arrived: a stream error, an HTTP error or a dropped stream.
 * shortcut: a missing or unknown disposition is Not checked; #117 adds the verification.status fallback for older answers.
 */
export function answerState(complete: { finalization?: { disposition?: string } } | null | undefined): { state: AnswerState; reason?: string } {
  if (!complete) return { state: "failed", reason: "The answer failed. Your question was not answered." };
  const disposition = complete.finalization?.disposition ?? "";
  const state = STATES.get(disposition) ?? "not_checked";
  const reason = REASONS.get(disposition);
  return reason ? { state, reason } : { state };
}

/** Map `evidence.score` or the legacy `confidence` when no level was sent. */
export function scoreLevel(score: number): EvidenceLevel {
  return score >= 0.75 ? "high" : score >= 0.5 ? "medium" : "low";
}
