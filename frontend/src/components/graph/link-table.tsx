"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { KindShape } from "@/components/kind-shape";
import { Pager } from "@/components/pager";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { entityName, shortDate } from "@/lib/graph-inspector";
import type { NeighborhoodLayout, NodeSummary, SummaryLink } from "@/lib/graph-layout";
import { TABLE_COLUMNS, TABLE_PAGE_SIZE, sortTableRows, tableRows, type TableColumn } from "@/lib/graph-table";

/** Native table with one Tab stop across headers and rows. Arrow Up from the first row reaches the headers. */
export function LinkTable({ summary, layout, selection, onSelectLink, unfilteredTotal = 0, onResetFilters }: {
  unfilteredTotal?: number; onResetFilters?: () => void;
  summary: NodeSummary; layout: NeighborhoodLayout; selection: string | null; onSelectLink: (key: string) => void;
}) {
  const isSelected = (link: SummaryLink) => link.key === selection || (selection !== summary.focus.id && link.neighbor.id === selection);
  const [sort, setSort] = useState<{ column: TableColumn; descending: boolean }>({ column: "documents", descending: true });
  const rows = useMemo(() => sortTableRows(tableRows(summary, layout), sort.column, sort.descending), [summary, layout, sort]);
  const [page, setPage] = useState(() => Math.max(0, Math.floor(rows.findIndex(row => isSelected(row.link)) / TABLE_PAGE_SIZE)));
  const [active, setActive] = useState<string | null>(() => rows.find(row => isSelected(row.link))?.link.key ?? null);
  const [previousSelection, setPreviousSelection] = useState(selection);
  if (previousSelection !== selection) {
    setPreviousSelection(selection);
    const index = rows.findIndex(row => isSelected(row.link));
    if (index >= 0) { setPage(Math.floor(index / TABLE_PAGE_SIZE)); setActive(rows[index].link.key); }
  }
  const table = useRef<HTMLDivElement>(null);
  useEffect(() => {
    table.current?.querySelector('[data-state="selected"]')?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [selection]);
  const pageCount = Math.max(1, Math.ceil(rows.length / TABLE_PAGE_SIZE));
  const currentPage = Math.min(page, pageCount - 1);
  const visible = rows.slice(currentPage * TABLE_PAGE_SIZE, (currentPage + 1) * TABLE_PAGE_SIZE);
  const headers = TABLE_COLUMNS.map(([key]) => `header:${key}`);
  const tabStop = visible.some(row => row.link.key === active) || headers.includes(active ?? "")
    ? active : visible[0]?.link.key ?? headers[0];
  const move = (event: KeyboardEvent, key: string) => {
    const header = headers.indexOf(key), row = visible.findIndex(row => row.link.key === key);
    let next: string | undefined;
    if (header >= 0) {
      if (event.key === "ArrowRight") next = headers[(header + 1) % headers.length];
      if (event.key === "ArrowLeft") next = headers[(header - 1 + headers.length) % headers.length];
      if (event.key === "Home") next = headers[0];
      if (event.key === "End") next = headers.at(-1);
      if (event.key === "ArrowDown") next = visible[0]?.link.key;
    } else {
      if (event.key === "ArrowDown") next = visible[Math.min(row + 1, visible.length - 1)]?.link.key;
      if (event.key === "ArrowUp") next = row > 0 ? visible[row - 1].link.key : `header:${sort.column}`;
      if (event.key === "Home") next = visible[0]?.link.key;
      if (event.key === "End") next = visible.at(-1)?.link.key;
      if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelectLink(key); }
    }
    if (next) {
      event.preventDefault();
      setActive(next);
      Array.from(table.current?.querySelectorAll<HTMLElement>("[data-table-key]") ?? []).find(element => element.dataset.tableKey === next)?.focus();
    }
  };
  return (
    <section aria-label={`Links of ${entityName(summary.focus)}`} className="flex min-h-0 min-w-0 flex-1 flex-col">
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 px-4 pb-3 pt-5 md:px-8">
        <h2 className="font-(family-name:--font-geist-serif-regular) text-lg leading-6">Links of {entityName(summary.focus)}</h2>
        <p role="status" className="label-13 tabular-nums text-fg-muted">{rows.length} links, {rows.filter(row => row.link.support_state === "inferred").length} inferred</p>
        <p id="link-table-keys" className="sr-only">Arrow keys move. Enter opens evidence. Arrow Up from the first row reaches sortable headers.</p>
      </div>
      <div ref={table} className="min-h-0 flex-1 overflow-auto px-4 md:px-8 [&>[data-slot=table-container]]:overflow-visible">
        <Table aria-label={`Links of ${entityName(summary.focus)}`} aria-describedby="link-table-keys" className="min-w-[1180px] label-13">
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              {TABLE_COLUMNS.map(([column, label]) => (
                <TableHead key={column} scope="col" aria-sort={sort.column === column ? sort.descending ? "descending" : "ascending" : undefined}
                  className={`aria-[sort]:text-fg ${column === "documents" || column === "confidence" ? "text-right" : ""}`}>
                  <button type="button" data-table-key={`header:${column}`} tabIndex={tabStop === `header:${column}` ? 0 : -1}
                    onFocus={() => setActive(`header:${column}`)} onKeyDown={event => move(event, `header:${column}`)}
                    onClick={() => { setSort({ column, descending: sort.column === column ? !sort.descending : column === "documents" || column === "confidence" || column === "latest" }); setPage(0); }}
                    className="inline-flex min-h-11 items-center gap-1 label-12 md:min-h-9">
                    {label}{sort.column === column && (sort.descending ? <ArrowDown aria-hidden className="size-3.5" /> : <ArrowUp aria-hidden className="size-3.5" />)}
                  </button>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {visible.map(row => (
              <TableRow key={row.link.key} data-table-key={row.link.key} data-link-key={row.link.key} data-on-canvas={row.onCanvas}
                data-state={isSelected(row.link) ? "selected" : undefined}
                aria-current={isSelected(row.link) || undefined}
                tabIndex={row.link.key === tabStop ? 0 : -1} onFocus={() => setActive(row.link.key)}
                onKeyDown={event => move(event, row.link.key)} onClick={() => onSelectLink(row.link.key)} className="cursor-pointer [&>td]:h-12 md:[&>td]:h-9 data-[state=selected]:[&>td]:text-on-mark data-[state=selected]:[&>td:first-child]:shadow-[inset_2px_0_0_var(--mark-line)]">
                <TableCell>{row.onCanvas ? <><Check aria-hidden className="size-3.5" /><span className="sr-only">On canvas</span></> : <span className="sr-only">Not on canvas</span>}</TableCell>
                <TableCell className="text-fg-muted">{row.family}</TableCell>
                <TableCell>{row.relationship}</TableCell>
                <TableCell className="text-fg-muted"><span className="inline-flex items-center gap-1">{row.direction === "In" ? <ArrowLeft aria-hidden className="size-3.5" /> : <ArrowRight aria-hidden className="size-3.5" />}{row.direction}</span></TableCell>
                <TableCell><span className="inline-flex max-w-64 items-center gap-1.5"><KindShape bare label={row.link.neighbor.label} /><span className="truncate" title={row.name}>{row.name}</span></span></TableCell>
                <TableCell className="text-fg-muted">{row.type}</TableCell>
                <TableCell className={row.link.support_state === "supported" ? "text-fg-muted" : undefined}>{row.support}</TableCell>
                <TableCell className="text-right">{row.documents}</TableCell>
                <TableCell className="text-right text-fg-muted">{row.confidence === null ? "No score" : row.confidence.toFixed(2)}</TableCell>
                <TableCell className="text-fg-muted">{shortDate(row.latest) || "No date"}</TableCell>
              </TableRow>
            ))}
            {!rows.length && <TableRow><TableCell colSpan={TABLE_COLUMNS.length} className="py-8 text-center text-fg-muted">{unfilteredTotal ? <><p>Your filters hide all {unfilteredTotal} links.</p><Button variant="outline" className="mt-3" onClick={onResetFilters}>Reset filters</Button></> : "No direct links extracted. Open Focus to see this entity's documents."}</TableCell></TableRow>}
          </TableBody>
        </Table>
      </div>
      <Pager page={currentPage} pageCount={pageCount} pageSize={TABLE_PAGE_SIZE} total={rows.length} onPage={value => { setPage(value); setActive(null); table.current?.scrollTo({ top: 0 }); }} />
    </section>
  );
}
