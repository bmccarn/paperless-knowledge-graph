import { Clock } from "lucide-react";

export interface TimelineEvent {
  date: string;
  precision: string;
  date_text: string;
  claim: string;
  claim_id: string;
  candidate_digest: string;
  presentation: "observation" | "answer_context";
  references: Array<{ document_id: number; quote: string; source_title?: string }>;
}

export interface TimelineReceipt {
  version?: string;
  status?: string;
  candidate_digest?: string;
  event_count?: number;
}

export function TimelineDates({ events, receipt, answerId, onSource }: {
  events?: TimelineEvent[];
  receipt?: TimelineReceipt;
  answerId: string;
  onSource: (source: { document_id: number; title?: string; excerpt: string }) => void;
}) {
  if (!receipt && !events?.length) return null;
  const valid = receipt?.version === "verified-dates-v1" &&
    (receipt.status === "ready" || receipt.status === "no_dates") &&
    receipt.event_count === (events?.length || 0) &&
    (events || []).every(event => event.candidate_digest === receipt.candidate_digest &&
      (event.presentation === "observation" || event.presentation === "answer_context"));
  if (!valid) return <p className="text-xs text-muted-foreground">Timeline unavailable for this answer.</p>;
  if (receipt.status === "no_dates") return <p className="text-xs text-muted-foreground">No supported calendar dates were identified in this answer.</p>;
  return (
    <section aria-label="Dates in verified observations" className="rounded-lg border bg-card/70 px-3 py-2 text-xs space-y-3 min-w-0 break-words">
      <p className="font-medium flex items-center gap-1">
        <Clock className="h-3 w-3 shrink-0" /> Dates in verified observations ({events?.length})
      </p>
      <p className="text-muted-foreground">Dates mentioned in the answer, sorted chronologically. A date may describe a request, issue, effective term or expiration; it does not alone confirm that an action occurred.</p>
      {events?.map((event, index) => (
        <article key={index} className="border-t pt-2 space-y-2" aria-label={`Date mention ${index + 1}`}>
          <p className="font-medium"><time dateTime={event.date}>{event.date_text}</time> <span className="font-normal text-muted-foreground">({event.precision} precision)</span></p>
          {event.presentation === "observation" && <p className="whitespace-pre-wrap">{event.claim}</p>}
          <a className="text-primary underline inline-block py-1" href={`#${answerId}`}>Read in the full answer</a>
          <div className="flex flex-wrap gap-2">
            {Array.from(new Set(event.references.map(ref => ref.document_id))).map(documentId => {
              const refs = event.references.filter(ref => ref.document_id === documentId);
              return <button key={documentId} type="button" className="text-left text-primary underline py-1 break-words min-w-0"
                onClick={() => onSource({ document_id: documentId, title: refs[0].source_title,
                  excerpt: Array.from(new Set(refs.map(ref => ref.quote))).join("\n…\n") })}>
                {refs[0].source_title || `Document ${documentId}`}
              </button>;
            })}
          </div>
        </article>
      ))}
    </section>
  );
}
