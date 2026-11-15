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
 * An older stored answer without a disposition uses its known `verification.status`; anything else is Not checked.
 */
export function answerState(complete: { finalization?: { disposition?: string }; verification?: { status?: string } } | null | undefined): { state: AnswerState; reason?: string } {
  if (!complete) return { state: "failed", reason: "The answer failed. Your question was not answered." };
  const status = complete.verification?.status;
  const disposition = complete.finalization?.disposition ?? (status === "verified" ? "supported" : status) ?? "";
  const state = STATES.get(disposition) ?? "not_checked";
  const reason = REASONS.get(disposition);
  return reason ? { state, reason } : { state };
}

/** Map `evidence.score` or the legacy `confidence` when no level was sent. */
export function scoreLevel(score: number): EvidenceLevel {
  return score >= 0.75 ? "high" : score >= 0.5 ? "medium" : "low";
}

/** The parts of a live or stored answer that decide what Ask shows. */
export interface AnswerMessage {
  content: string;
  /** Set by the page when no complete event arrived. */
  failed?: boolean;
  confidence?: number;
  evidence?: { score?: number; level?: string };
  finalization?: { disposition?: string };
  verification?: { status?: string; missing_evidence?: string[]; partial?: { omitted_count?: number; original_total?: number } };
  claim_ledger?: { candidate_text?: string; claims?: Array<{ references?: Array<{ document_id?: number }> }> };
  source_dates?: Record<string, string>;
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const LEVELS = new Set<string>(["high", "medium", "low"]);
const PROSE_STATES = new Set<AnswerState>(["verified", "documented", "partial", "not_checked"]);

/** "2026-02-28" becomes "28 Feb 2026"; read as a calendar date, never through a time zone. */
export function formatSourceDate(date: string): string {
  const [year, month, day] = date.split("-").map(Number);
  return `${day} ${MONTHS[month - 1]} ${year}`;
}

/** The stored answer without inline `[Document N]` links, the appended notices and the Fast prefix. */
export function plainAnswer(answer: string): string {
  return answer
    .replace(/^Unaudited quick answer — verify the source documents before relying on it\.\n\n/, "")
    // The backend appends the Partial notice first, then the Documented notice.
    .replace(/\n\nThese are documented facts\. Current status as of .+ is not established by the retrieved evidence\.$/, "")
    .replace(/\n\nPartial answer: some claims could not be verified and were omitted\. This does not answer every part of your question\.$/, "")
    .replace(/ ?\[Document\s+\d+\]\([^)]*\)/g, "");
}

function reasonFor(state: AnswerState, m: AnswerMessage): string {
  if (state === "verified") {
    const claims = m.claim_ledger?.claims ?? [];
    const docs = new Set(claims.flatMap(claim => (claim.references ?? []).map(ref => ref.document_id))).size;
    const where = `${docs} document${docs === 1 ? "" : "s"}`;
    return claims.length === 1 ? `The claim matches a passage in ${where}.` : `All ${claims.length} claims match passages in ${where}.`;
  }
  if (state === "documented") {
    const newest = Object.values(m.source_dates ?? {}).sort().pop();
    return "These facts are documented." + (newest ? ` The newest source is dated ${formatSourceDate(newest)}.` : "")
      + " The sources do not show whether they are still true today.";
  }
  if (state === "partial") {
    const left = m.verification?.partial?.omitted_count ?? 0, total = m.verification?.partial?.original_total ?? 0;
    return left === 1 ? `1 of ${total} claims was left out because it did not match a source passage.`
      : `${left} of ${total} claims were left out because they did not match a source passage.`;
  }
  if (state === "withheld") return m.verification?.missing_evidence?.[0] ?? "No answer is shown because the claims did not match the sources.";
  // Not checked without a fixed reason: an older answer saved before dispositions were recorded.
  return "This answer was saved before the current source checks. Ask again to check it.";
}

/** State, reason, evidence level and the prose to show (null when the state shows no answer text). */
export function answerView(m: AnswerMessage): { state: AnswerState; reason: string; level?: EvidenceLevel; prose: string | null } {
  const { state, reason } = answerState(m.failed ? null : m);
  const sent = m.evidence?.level;
  const level = state === "failed" ? undefined
    : sent && LEVELS.has(sent) ? sent as EvidenceLevel : scoreLevel(m.evidence?.score ?? m.confidence ?? 0);
  const candidate = m.claim_ledger?.candidate_text;
  const prose = !PROSE_STATES.has(state) ? null : typeof candidate === "string" && candidate ? candidate : plainAnswer(m.content);
  return { state, reason: reason ?? reasonFor(state, m), level, prose };
}
