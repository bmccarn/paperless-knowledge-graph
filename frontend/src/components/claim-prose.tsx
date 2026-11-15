"use client";

import { useState, type CSSProperties, type ReactNode } from "react";
import { StatusLabel, STATUSES } from "@/components/status-label";
import { Button } from "@/components/ui/button";
import { formatAnswerInline } from "@/lib/answer-format";
import { formatSourceDate, type AnswerState, type ClaimResult } from "@/lib/answer-state";
import { claimRanges, emphasize, proseBlocks, quoteWindow, type Block, type LedgerClaim } from "@/lib/claim-prose";

const UNDERLINED = new Set<AnswerState>(["verified", "documented", "partial"]);
const RESULTS = new Set(["supported", "unsupported", "conflicting"]);

/** A ledger status as a claim result; "missing" and unknown statuses read as Unsupported. */
export const claimResult = (status?: string) => (RESULTS.has(status ?? "") ? status : "unsupported") as ClaimResult;

/**
 * A reference's quote, three lines from just before the values the claim states, with those values emphasized.
 * Expanded, it shows the whole passage.
 */
export function Quote({ claim = "", text = "", expanded, className = "" }: { claim?: string; text: string; expanded: boolean; className?: string }) {
  const { start, values } = quoteWindow(claim, text);
  const cut = !expanded && start > 0;
  return (
    <q className={`quote text-fg [quotes:none] ${expanded ? "line-clamp-none" : "line-clamp-3"} ${className}`}>
      {cut && "… "}
      {emphasize(cut ? text.slice(start) : text, values).map((piece, i) => piece.match
        ? <strong key={i} className="font-semibold">{piece.text}</strong> : piece.text)}
    </q>
  );
}

/** Notes show this many references; the rest open on request. */
const NOTE_REFERENCES = 2;

/**
 * One claim's proof: number, result, quote, and the document title with its date when known.
 * Beside the claim in the margin when the answer column is wide, under it when narrower,
 * and on a phone only while it is the active claim.
 */
export function MarginNote({ id, n, claim, sourceDates, active, onSelect }: {
  id: string; n: number; claim: LedgerClaim; sourceDates?: Record<string, string>; active: boolean; onSelect: () => void;
}) {
  const result = claimResult(claim.status);
  const [showAll, setShowAll] = useState(false);
  // References that hold the claim's amount come first, then other values; the sort is stable.
  const rank = (quote = "") => quoteWindow(claim.claim ?? "", quote).rank;
  const references = [...(claim.references ?? [])].sort((a, b) => rank(a.quote) - rank(b.quote));
  const hidden = references.length - NOTE_REFERENCES;
  return (
    <span id={id} role="note" aria-label={`Claim ${n} source`} aria-current={active || undefined} tabIndex={0} onFocus={onSelect} onClick={onSelect}
      className="mt-1.5 mb-3.5 block border-l-2 border-(--st-line) pl-3 font-sans label-12 leading-[18px] text-fg-muted not-italic font-normal aria-[current=true]:border-mark-line max-md:not-aria-[current=true]:hidden @min-[58rem]:float-right @min-[58rem]:clear-right @min-[58rem]:mt-0.5 @min-[58rem]:-mr-[356px] @min-[58rem]:w-[300px]">
      <span className="mb-0.5 flex items-center gap-1.5">
        <span className="mr-0.5 font-semibold tabular-nums">{n}</span>
        <StatusLabel status={result} size="sm" />
      </span>
      {references.slice(0, showAll ? undefined : NOTE_REFERENCES).map((ref, i) => {
        const date = sourceDates?.[String(ref.document_id)];
        return (
          <span key={i} className="block">
            <Quote claim={claim.claim} text={ref.quote} expanded={active} className="my-1" />
            <span className="block">{ref.source_title || `Document ${ref.document_id}`}{date && <> · <span className="tabular-nums">{formatSourceDate(date)}</span></>}</span>
          </span>
        );
      })}
      {hidden > 0 && (
        // No focus on press: focus would select the note, expand its quotes and move the button before the click lands.
        <Button variant="link" size="xs" aria-expanded={showAll} onMouseDown={event => event.preventDefault()} onClick={() => setShowAll(!showAll)}
          className="mt-1 h-auto px-0 text-fg-muted underline-offset-2 hover:text-fg">
          {showAll ? "Show fewer" : `and ${hidden} more`}
        </Button>
      )}
    </span>
  );
}

/**
 * Answer prose with each claim underlined in its state color and numbered in ledger order.
 * Not checked answers and saved answers without candidate text render as plain Markdown.
 * The selected claim is shared with the claim table, so the caller holds it.
 */
export function ClaimProse({ id, text, state, claims = [], sourceDates, active, onSelect: setActive }: {
  id: string; text: string; state: AnswerState; claims?: LedgerClaim[]; sourceDates?: Record<string, string>;
  active: number | null; onSelect: (n: number) => void;
}) {
  const ranges = UNDERLINED.has(state) ? claimRanges(text, claims) : [];
  const noteId = (n: number) => `note-${id}-${n}`;
  const openNote = (n: number) => {
    setActive(n);
    // After the render that shows the note on a phone; also when the claim was already selected.
    requestAnimationFrame(() => document.getElementById(noteId(n))?.scrollIntoView({ block: "nearest" }));
  };

  const inline = (block: Block, key: number) => block.pieces.map((piece, i) => {
    const html = { __html: formatAnswerInline(piece.text) };
    const n = piece.claim;
    if (!n) return <span key={`${key}-${i}`} dangerouslySetInnerHTML={html} />;
    const current = active === n || undefined;
    return (
      <span key={`${key}-${i}`}>
        <span data-claim={n} aria-current={current} onClick={() => setActive(n)}
          className="cursor-pointer underline decoration-(--st-line) decoration-2 underline-offset-[5px] [text-decoration-skip-ink:none] aria-[current=true]:rounded-[1px] aria-[current=true]:bg-mark aria-[current=true]:text-on-mark aria-[current=true]:shadow-[0_0_0_2px_var(--mark)]"
          dangerouslySetInnerHTML={html} />
        {piece.last && <>
          <button type="button" aria-label={`Claim ${n}`} aria-controls={noteId(n)} aria-current={current}
            onClick={() => openNote(n)}
            className="cite relative ml-0.5 leading-none font-sans text-fg-muted hover:text-fg aria-[current=true]:text-fg after:absolute after:-inset-x-2 after:-inset-y-3 after:content-['']">{n}</button>
          <MarginNote id={noteId(n)} n={n} claim={claims[n - 1]} sourceDates={sourceDates} active={active === n} onSelect={() => setActive(n)} />
        </>}
      </span>
    );
  });

  const elements: ReactNode[] = [];
  const blocks = proseBlocks(text, ranges);
  for (let i = 0; i < blocks.length; i++) {
    const block = blocks[i];
    if (block.kind === "ul" || block.kind === "ol") {
      // A blank line between items of the same list keeps the list open.
      const items: ReactNode[] = [];
      let j = i;
      for (; j < blocks.length; j++) {
        if (blocks[j].kind === block.kind) items.push(<li key={j}>{inline(blocks[j], j)}</li>);
        else if (!(blocks[j].kind === "blank" && blocks[j + 1]?.kind === block.kind)) break;
      }
      elements.push(block.kind === "ol"
        ? <ol key={i} className="ml-6 list-decimal">{items}</ol>
        : <ul key={i} className="ml-6 list-disc">{items}</ul>);
      i = j - 1;
    } else if (block.kind === "code") {
      elements.push(<pre key={i} className="bg-muted rounded-md p-3 text-xs overflow-x-auto my-2 font-mono">{block.pieces[0].text}</pre>);
    } else if (block.kind === "blank") {
      elements.push(<div key={i} className="h-2" />);
    } else if (block.kind === "h3") {
      elements.push(<h3 key={i} className="font-semibold mt-3 mb-1">{inline(block, i)}</h3>);
    } else if (block.kind === "h2") {
      elements.push(<h2 key={i} className="font-semibold mt-3 mb-1">{inline(block, i)}</h2>);
    } else {
      elements.push(<p key={i}>{inline(block, i)}</p>);
    }
  }
  return (
    <div id={id} tabIndex={-1} style={{ "--st-line": STATUSES[state].line } as CSSProperties}
      className="prose space-y-2 scroll-mt-4">{elements}</div>
  );
}
