import { TypeDot } from "@/components/kind-shape";
import { formatSourceDate } from "@/lib/answer-state";
import { getPaperlessDocUrl } from "@/lib/api";
import type { LedgerClaim } from "@/lib/claim-prose";

export interface AnswerSource { document_id?: number; title?: string; doc_type?: string; paperless_url?: string }

const link = "inline-flex min-h-11 items-center label-12 text-fg underline underline-offset-3 hover:text-fg-muted md:min-h-0";

/** "Open" in the app and "Open in Paperless"; in a list, the hidden title tells the links of different sources apart. */
export function SourceLinks({ source, title, paperlessBaseUrl }: { source: AnswerSource; title?: string; paperlessBaseUrl: string }) {
  const hidden = title && <span className="sr-only"> {title}</span>;
  if (!source.document_id) return null;
  return (
    <span className="flex flex-wrap gap-x-4">
      <a href={`/documents/${source.document_id}`} className={link}>Open{hidden}</a>
      <a href={source.paperless_url || getPaperlessDocUrl(source.document_id, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer" className={link}>
        Open in Paperless{hidden}
      </a>
    </span>
  );
}

const and = (numbers: number[]) => numbers.length < 2 ? `${numbers[0]}` : `${numbers.slice(0, -1).join(", ")} and ${numbers[numbers.length - 1]}`;

/**
 * Numbered sources, each with its type, its date when known and the claims it supports.
 * A source that supports the selected claim is marked. The title opens the source sheet.
 */
export function SourceList({ id, sources, claims = [], sourceDates, active, paperlessBaseUrl, onOpen }: {
  id: string; sources: AnswerSource[]; claims?: LedgerClaim[]; sourceDates?: Record<string, string>; active?: number | null;
  paperlessBaseUrl: string; onOpen: (source: AnswerSource) => void;
}) {
  return (
    <section id={id} tabIndex={-1} aria-label="Sources" className="scroll-mt-4">
      <h3 className="mb-2.5 label-13 font-semibold text-fg">Sources</h3>
      <ol className="border-t border-border">
        {sources.map((source, i) => {
          const title = source.title || `Document ${source.document_id}`;
          const date = sourceDates?.[String(source.document_id)];
          const supports = claims.flatMap((claim, j) =>
            claim.references?.some(ref => ref.document_id === source.document_id) ? [j + 1] : []);
          const current = active != null && supports.includes(active) || undefined;
          return (
            <li key={i} aria-current={current}
              className="group grid grid-cols-[28px_1fr] gap-x-2 gap-y-0.5 border-b border-border py-2.5 aria-[current=true]:shadow-[inset_2px_0_0_var(--mark-line)] md:grid-cols-[28px_1fr_auto]">
              <span className="pl-1 pt-px label-13 font-semibold text-fg-muted tabular-nums group-aria-[current=true]:text-fg">{i + 1}</span>
              <button type="button" onClick={() => onOpen(source)} className="doc-title min-h-11 text-left text-fg hover:underline hover:underline-offset-3 md:min-h-0">{title}</button>
              <span className="col-start-2 flex flex-wrap items-center gap-x-3 label-12 text-fg-subtle tabular-nums">
                {source.doc_type && <TypeDot type={source.doc_type} className="label-12" />}
                {date && <span>{formatSourceDate(date)}</span>}
                {supports.length > 0 && <span>Supports {supports.length === 1 ? "claim" : "claims"} {and(supports)}</span>}
              </span>
              <span className="col-start-2 md:col-start-3 md:row-start-1"><SourceLinks source={source} title={title} paperlessBaseUrl={paperlessBaseUrl} /></span>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
