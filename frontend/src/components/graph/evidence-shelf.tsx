"use client";

import { useEffect, useRef } from "react";
import Link from "next/link";
import { documentType } from "@/lib/categories";
import { plural, shortDate, type SummaryDocument } from "@/lib/graph-inspector";
import { cn } from "@/lib/utils";

/** Chips before "N more"; the rest open on the documents page, filtered to the entity. */
const SHELF_CHIPS = 12;

/**
 * The strip under the canvas: the focus entity's documents, newest first. Hovering or focusing a chip
 * marks the links it supports; chips of the selected link's documents are marked.
 */
export function EvidenceShelf({ documents, entityId, marked, onMark, selectedDocument = null, className }: {
  documents: SummaryDocument[];
  entityId: string;
  marked: ReadonlySet<number>;
  onMark: (docId: number | null) => void;
  selectedDocument?: number | null;
  className?: string;
}) {
  const more = documents.length - SHELF_CHIPS;
  const list = useRef<HTMLUListElement>(null);
  const shown = documents.slice(0, SHELF_CHIPS);
  const chosen = documents.find(doc => doc.id === selectedDocument);
  // Keep the selected Timeline source visible even when it is outside the newest twelve.
  if (chosen && !shown.some(doc => doc.id === chosen.id)) shown[shown.length - 1] = chosen;
  useEffect(() => {
    if (selectedDocument !== null) list.current?.querySelector<HTMLElement>(`[data-document-id="${selectedDocument}"]`)
      ?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [selectedDocument]);
  return (
    <section aria-labelledby="evidence-shelf-title"
      className={cn("flex h-16 shrink-0 items-center gap-4 border-t border-border bg-background pl-4 md:pl-6", className)}>
      <h2 id="evidence-shelf-title" className="shrink-0 label-13 font-semibold text-fg">
        Evidence · <span className="tabular-nums">{plural(documents.length, "document")}</span>
      </h2>
      <ul ref={list} className="flex min-w-0 flex-1 items-center gap-2 overflow-x-auto py-2 pr-4 [scrollbar-width:none]">
        {shown.map(doc => {
          const date = shortDate(doc.date);
          return (
            <li key={doc.id} className="shrink-0">
              <Link href={`/documents/${doc.id}`} data-document-id={doc.id} data-marked={marked.has(doc.id) || undefined}
                onMouseEnter={() => onMark(doc.id)} onMouseLeave={() => onMark(null)}
                onFocus={() => onMark(doc.id)} onBlur={() => onMark(null)}
                className="group flex h-9 items-center gap-2 whitespace-nowrap rounded-[4px] border border-border-strong px-3 text-fg transition-colors duration-(--dur-2) hover:bg-raised data-marked:border-mark-line data-marked:bg-mark data-marked:text-on-mark">
                <span aria-hidden className="size-2 shrink-0 rounded-full" style={{ background: documentType(doc.doc_type || "").color }} />
                <span className="font-[family-name:var(--font-source-serif)] text-sm leading-5">{doc.title}</span>
                {date && <span className="label-12 tabular-nums text-fg-subtle group-data-marked:text-on-mark">{date}</span>}
              </Link>
            </li>
          );
        })}
        {more > 0 && (
          <li className="shrink-0">
            <Link href={`/documents?entity=${encodeURIComponent(entityId)}`}
              className="flex h-9 items-center rounded-[4px] border border-border-strong px-3 label-13 font-medium text-fg-muted hover:bg-raised">
              <span className="tabular-nums">{more}</span>&nbsp;more
            </Link>
          </li>
        )}
      </ul>
    </section>
  );
}
