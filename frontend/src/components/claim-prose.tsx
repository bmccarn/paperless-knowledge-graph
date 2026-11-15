"use client";

import { useEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { StatusLabel, STATUSES } from "@/components/status-label";
import { formatAnswerInline } from "@/lib/answer-format";
import { formatSourceDate, type AnswerState, type ClaimResult } from "@/lib/answer-state";
import { claimRanges, proseBlocks, type Block, type LedgerClaim } from "@/lib/claim-prose";

const UNDERLINED = new Set<AnswerState>(["verified", "documented", "partial"]);
const RESULTS = new Set(["supported", "unsupported", "conflicting"]);

/**
 * One claim's proof: number, result, quote, and the document title with its date when known.
 * Beside the claim in the margin when the answer column is wide, under it when narrower,
 * and on a phone only while it is the active claim.
 */
export function MarginNote({ id, n, claim, sourceDates, active, onSelect }: {
  id: string; n: number; claim: LedgerClaim; sourceDates?: Record<string, string>; active: boolean; onSelect: () => void;
}) {
  const result = (RESULTS.has(claim.status ?? "") ? claim.status : "unsupported") as ClaimResult;
  return (
    <span id={id} role="note" aria-label={`Claim ${n} source`} aria-current={active || undefined} tabIndex={0} onFocus={onSelect} onClick={onSelect}
      className="group/note mt-1.5 mb-3.5 block border-l-2 border-(--st-line) pl-3 font-sans label-12 leading-[18px] text-fg-muted not-italic font-normal aria-[current=true]:border-mark-line max-md:not-aria-[current=true]:hidden @min-[58rem]:float-right @min-[58rem]:clear-right @min-[58rem]:mt-0.5 @min-[58rem]:-mr-[356px] @min-[58rem]:w-[300px]">
      <span className="mb-0.5 flex items-center gap-1.5">
        <span className="mr-0.5 font-semibold tabular-nums">{n}</span>
        <StatusLabel status={result} size="sm" />
      </span>
      {(claim.references ?? []).map((ref, i) => {
        const date = sourceDates?.[String(ref.document_id)];
        return (
          <span key={i} className="block">
            <q className="quote my-1 line-clamp-3 block text-fg [quotes:none] group-focus-within/note:line-clamp-none group-aria-[current=true]/note:line-clamp-none">{ref.quote}</q>
            <span className="block">{ref.source_title || `Document ${ref.document_id}`}{date && <> · <span className="tabular-nums">{formatSourceDate(date)}</span></>}</span>
          </span>
        );
      })}
    </span>
  );
}

/**
 * Answer prose with each claim underlined in its state color and numbered in ledger order.
 * Not checked answers and saved answers without candidate text render as plain Markdown.
 */
export function ClaimProse({ id, text, state, claims = [], sourceDates }: {
  id: string; text: string; state: AnswerState; claims?: LedgerClaim[]; sourceDates?: Record<string, string>;
}) {
  const [active, setActive] = useState<number | null>(null);
  const scrollTo = useRef<number | null>(null);
  const ranges = UNDERLINED.has(state) ? claimRanges(text, claims) : [];
  const noteId = (n: number) => `note-${id}-${n}`;
  useEffect(() => {
    if (scrollTo.current === null) return;
    document.getElementById(noteId(scrollTo.current))?.scrollIntoView({ block: "nearest" });
    scrollTo.current = null;
  });

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
            onClick={() => { scrollTo.current = n; setActive(n); }}
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
