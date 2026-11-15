"use client";

import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { StatusLabel } from "@/components/status-label";
import { claimResult } from "@/components/claim-prose";
import { Collapsible, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatAnswerInline } from "@/lib/answer-format";
import { formatSourceDate } from "@/lib/answer-state";
import type { LedgerClaim } from "@/lib/claim-prose";

/** Older saved ledgers name one source on the claim itself instead of in `references`. */
export type TableClaim = LedgerClaim & { claim?: string; document_id?: number; source_title?: string; evidence_quote?: string; evidence_excerpt?: string };
/** An entry of `verification.partial.omitted_units`. */
export interface OmittedUnit { id?: string; claim?: string; omission_reason?: string }

const OMISSIONS: Record<string, string> = {
  source_rejection: "No source passage matched it.",
  dependency_omitted: "It depends on a claim that was left out.",
  dependency_unknown: "Its link to the other claims could not be checked.",
};

const cell = "h-auto whitespace-normal py-3 align-top max-md:p-0";
const claimCell = `${cell} [font-family:var(--font-source-serif)] text-[15px] leading-[22px] text-fg`;

/** A claim is a slice of Markdown: drop heading and list markers, join its lines and format it like the prose (escaped). */
const claimHtml = (text = "") => ({ __html: formatAnswerInline(text.replace(/^(?:#{1,3} |[-*] |\d+\.\s)/gm, "").replace(/\s*\n+\s*/g, " ")) });

/**
 * The claim, result and source of each claim in ledger order, then each left-out unit of a Partial answer.
 * Closed by default; the rows stay in the page while closed so the selected claim is marked here too.
 */
export function ClaimTable({ id, claims, omitted = [], sourceDates, active, onSelect, defaultOpen = false }: {
  id: string; claims: TableClaim[]; omitted?: OmittedUnit[]; sourceDates?: Record<string, string>;
  active: number | null; onSelect: (n: number) => void; defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const count = `${claims.length} ${claims.length === 1 ? "claim" : "claims"}${omitted.length ? `, ${omitted.length} left out` : ""}`;
  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger aria-controls={id}
        className="group inline-flex min-h-11 items-center gap-1.5 label-14 text-fg md:min-h-8">
        <ChevronRight aria-hidden className="size-4 text-fg-muted transition-transform duration-(--dur-2) group-aria-expanded:rotate-90" strokeWidth={1.75} />
        Claim table <span className="font-normal text-fg-muted tabular-nums">{count}</span>
      </CollapsibleTrigger>
      <div id={id} hidden={!open} className="pt-2">
        <Table aria-label="Claims" className="label-13">
          <TableHeader className="max-md:sr-only">
            <TableRow className="hover:bg-transparent">
              <TableHead className="w-10"><span className="sr-only">Number</span></TableHead>
              <TableHead className="md:w-[42%]">Claim</TableHead>
              <TableHead>Result</TableHead>
              <TableHead>Source</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {claims.map((claim, i) => {
              const n = i + 1;
              const current = active === n || undefined;
              const refs = claim.references?.length ? claim.references
                : claim.source_title || claim.evidence_quote || claim.evidence_excerpt
                  ? [{ document_id: claim.document_id ?? 0, quote: claim.evidence_quote || claim.evidence_excerpt || "", source_title: claim.source_title }] : [];
              return (
                <TableRow key={n} aria-current={current} onClick={() => onSelect(n)}
                  className="cursor-pointer aria-[current=true]:bg-raised aria-[current=true]:shadow-[inset_2px_0_0_var(--mark-line)] max-md:grid max-md:grid-cols-[40px_1fr] max-md:gap-x-2 max-md:gap-y-1.5 max-md:py-3">
                  <TableCell className={`${cell} max-md:row-span-3`}>
                    <button type="button" aria-label={`Select claim ${n}`} aria-current={current}
                      onClick={event => { event.stopPropagation(); onSelect(n); }}
                      className="inline-flex size-11 items-center justify-center rounded-[4px] font-semibold tabular-nums text-fg-muted hover:text-fg md:size-8 aria-[current=true]:bg-mark aria-[current=true]:text-on-mark">{n}</button>
                  </TableCell>
                  <TableCell className={claimCell} dangerouslySetInnerHTML={claimHtml(claim.claim)} />
                  <TableCell className={`${cell} md:whitespace-nowrap`}><StatusLabel status={claimResult(claim.status)} size="sm" /></TableCell>
                  <TableCell className={`${cell} label-12 text-fg-muted`}>
                    {refs.map((ref, j) => {
                      const date = sourceDates?.[String(ref.document_id)];
                      return (
                        <span key={j} className="block">
                          {ref.quote && <q className="quote mb-1 line-clamp-3 text-fg [quotes:none]">{ref.quote}</q>}
                          <span className="block">{ref.source_title || `Document ${ref.document_id}`}{date && <> · <span className="tabular-nums">{formatSourceDate(date)}</span></>}</span>
                        </span>
                      );
                    })}
                  </TableCell>
                </TableRow>
              );
            })}
            {omitted.map((unit, i) => (
              <TableRow key={`left-${unit.id ?? i}`} className="hover:bg-transparent max-md:grid max-md:grid-cols-[40px_1fr] max-md:gap-x-2 max-md:gap-y-1.5 max-md:py-3">
                <TableCell className={`${cell} max-md:row-span-3`} />
                <TableCell className={claimCell} dangerouslySetInnerHTML={claimHtml(unit.claim)} />
                <TableCell className={`${cell} md:whitespace-nowrap`}><StatusLabel status="left_out" size="sm" /></TableCell>
                <TableCell className={`${cell} label-12 text-fg-muted`}>{OMISSIONS[unit.omission_reason ?? ""] ?? OMISSIONS.source_rejection}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </Collapsible>
  );
}
