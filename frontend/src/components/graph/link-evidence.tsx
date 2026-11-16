"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { ExternalLink, Flag } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { TypeDot } from "@/components/kind-shape";
import { getGraphLink, getPaperlessDocUrl, usePaperlessUrl, type LinkRecord } from "@/lib/api";
import { quoteParts, scoreText, shortDate } from "@/lib/graph-inspector";
import type { NodeSummary, SummaryLink } from "@/lib/graph-layout";
import { cn } from "@/lib/utils";

/**
 * One card per source document of a link, stated before inferred, then newest first: title, type, date, score,
 * the quote with both end names marked, the rationale of an inferred record and the document actions.
 * The summary draws the cards at once; quotes and rationale arrive from `GET /graph/link`.
 */
export function LinkEvidence({ link, documents, names }: { link: SummaryLink; documents: NodeSummary["documents"]; names: string[] }) {
  const paperless = usePaperlessUrl();
  const [attempt, setAttempt] = useState(0);
  const [loaded, setLoaded] = useState<{ key: string; attempt: number; records: Map<number, LinkRecord> | null } | null>(null);
  useEffect(() => {
    let active = true;
    getGraphLink(link.key).then(
      data => { if (active) setLoaded({ key: link.key, attempt, records: new Map(data.records.map(record => [record.doc_id, record])) }); },
      () => { if (active) setLoaded({ key: link.key, attempt, records: null }); });
    return () => { active = false; };
  }, [link.key, attempt]);
  const loading = loaded?.key !== link.key || loaded.attempt !== attempt;
  const records = loading ? null : loaded.records;
  const cards = link.records.map(record => ({ ...record, doc: documents[String(record.doc_id)] }))
    .sort((a, b) => Number(a.inferred) - Number(b.inferred) || (b.doc?.date || "").localeCompare(a.doc?.date || "") || a.doc_id - b.doc_id);
  return (
    <>
      {!loading && !records && (
        <Alert className="mt-4">
          <AlertDescription className="flex flex-wrap items-center justify-between gap-2">
            The quotes for this link did not load.
            <Button size="xs" variant="outline" onClick={() => setAttempt(value => value + 1)}>Retry</Button>
          </AlertDescription>
        </Alert>
      )}
      {cards.map(card => {
        const title = card.doc?.title || "Untitled document";
        const evidence = records?.get(card.doc_id);
        const hidden = <span className="sr-only"> {title}</span>;
        return (
          <article key={card.doc_id} aria-label={title}
            className={cn("mt-4 border-l-2 pl-3.5", card.inferred ? "border-dashed border-fg-subtle" : "border-fg-muted")}>
            <header className="flex items-baseline justify-between gap-2">
              <h3 className="doc-title text-fg">{title}</h3>
              <span className="label-12 font-semibold text-fg-muted">{card.inferred ? "Inferred" : "Stated"}</span>
            </header>
            <p className="mt-0.5 flex flex-wrap items-center gap-x-3 label-12 tabular-nums text-fg-subtle">
              {card.doc?.doc_type && <TypeDot type={card.doc.doc_type} className="label-12" />}
              {shortDate(card.doc?.date) && <span>{shortDate(card.doc?.date)}</span>}
              <span>{scoreText("Confidence", card.confidence)}</span>
            </p>
            {loading ? <Skeleton aria-hidden className="mt-2 h-[42px]" />
              : evidence && (evidence.quotes.length ? evidence.quotes.map((quote, index) => (
                <blockquote key={index} className="quote mt-2 text-fg">
                  “{quoteParts(quote, names).map((part, at) => part.match
                    ? <mark key={at} className="rounded-[2px] bg-mark px-px text-on-mark">{part.text}</mark> : part.text)}”
                </blockquote>
              )) : <p className="mt-2 label-12 text-fg-muted">No quote stored for this document.</p>)}
            {card.inferred && evidence?.rationale && (
              <p className="mt-2 label-13 leading-5 text-fg-muted"><span className="font-semibold text-fg">Rationale</span> {evidence.rationale}</p>
            )}
            <div className="mt-2.5 flex flex-wrap items-center gap-2">
              <Button variant="outline" size="sm" asChild><Link href={`/documents/${card.doc_id}`}>Open document{hidden}</Link></Button>
              <Button variant="outline" size="sm" asChild>
                <a href={getPaperlessDocUrl(card.doc_id, paperless)} target="_blank" rel="noopener noreferrer"><ExternalLink />Open in Paperless{hidden}</a>
              </Button>
              <Link href={`/documents/${card.doc_id}#review`}
                className="ml-1 inline-flex items-center gap-1 label-12 text-fg underline decoration-border-strong underline-offset-3 max-md:min-h-11">
                <Flag aria-hidden className="size-3.5" />Wrong link?{hidden}
              </Link>
            </div>
          </article>
        );
      })}
    </>
  );
}
