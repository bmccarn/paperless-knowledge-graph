"use client";

import { SlidersHorizontal, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetClose, SheetContent, SheetDescription, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { TypeDot } from "@/components/kind-shape";
import { DOCUMENT_TYPE_KEYS, documentType } from "@/lib/categories";
import { defaultFilters, filterLinks, ownerHidden, type GraphFilters } from "@/lib/graph-filters";
import { drawableLinks, FAMILIES, type NodeSummary } from "@/lib/graph-layout";
import type { GraphView } from "@/lib/graph-url-state";

export function filterChips(filters: GraphFilters): { label: string; clear: Partial<GraphFilters> }[] {
  const chips: { label: string; clear: Partial<GraphFilters> }[] = [];
  if (filters.families.length !== FAMILIES.length) chips.push({ label: filters.families.length ? FAMILIES.filter(([key]) => filters.families.includes(key)).map(([, name]) => name).join(", ") : "No families", clear: { families: defaultFilters().families } });
  if (filters.support !== "all") chips.push({ label: filters.support === "supported" ? "Supported only" : "Inferred only", clear: { support: "all" } });
  if (filters.minConfidence) chips.push({ label: `Confidence ${filters.minConfidence.toFixed(2)} or more`, clear: { minConfidence: 0 } });
  if (!filters.noScore) chips.push({ label: "No score excluded", clear: { noScore: true } });
  if (filters.minDocs > 1) chips.push({ label: `${filters.minDocs}+ documents`, clear: { minDocs: 1 } });
  if (filters.from) chips.push({ label: `From ${filters.from}`, clear: { from: "" } });
  if (filters.to) chips.push({ label: `To ${filters.to}`, clear: { to: "" } });
  if (filters.types !== null) chips.push({ label: filters.types.length ? filters.types.map(type => documentType(type).label).join(", ") : "No document types", clear: { types: null } });
  if (filters.owner) chips.push({ label: filters.owner === "hide" ? "Owner hidden" : "Owner shown", clear: { owner: null } });
  return chips;
}

type Props = { summary: NodeSummary; filters: GraphFilters; view: GraphView; onChange: (filters: GraphFilters) => void };
const row = "flex min-h-11 cursor-pointer items-center gap-3 label-13 md:min-h-8";
const countClass = "ml-auto tabular-nums text-fg-muted";
const field = "space-y-2 border-b border-border pb-5 disabled:opacity-50 [&>legend]:mb-3 [&>legend]:label-13 [&>legend]:font-semibold";

/** The same controls serve every view; Overview retains the disabled groups' URL values. */
export function GraphFilterControls({ summary, filters, view, onChange }: Props) {
  const change = (next: Partial<GraphFilters>) => onChange({ ...filters, ...next });
  const links = view === "table" ? summary.links : drawableLinks(summary);
  const count = (next: Partial<GraphFilters> = {}) => filterLinks(links, summary.documents, { ...filters, ...next }, undefined, view).length;
  const overview = view === "overview";
  const types = [...new Set([...DOCUMENT_TYPE_KEYS, ...Object.values(summary.documents).flatMap(doc => doc.doc_type ? [doc.doc_type] : [])])];
  const toggle = (values: string[], key: string) => values.includes(key) ? values.filter(value => value !== key) : [...values, key];
  return <div className="space-y-5">
    {overview && <p className="copy-14 text-fg-muted">The map does not use document dates, types or confidence, and leaves out the owner.</p>}
    <fieldset className={field}><legend>Relationship families</legend>
      {FAMILIES.map(([key, name]) => <label key={key} className={row}>
        <input type="checkbox" checked={filters.families.includes(key)} onChange={() => change({ families: toggle(filters.families, key) })} className="size-4 accent-(--accent-solid)" />
        <span>{name}</span><span className={countClass}>{count({ families: [key] })}</span>
      </label>)}
    </fieldset>
    <fieldset className={field}><legend>Support</legend>
      {([['all', 'All'], ['supported', 'Supported only'], ['inferred', 'Inferred only']] as const).map(([key, label]) => <label key={key} className={row}>
        <input type="radio" name="graph-support" checked={filters.support === key} onChange={() => change({ support: key })} className="size-4 accent-(--accent-solid)" />
        <span>{label}</span><span className={countClass}>{count({ support: key })}</span>
      </label>)}
    </fieldset>
    <fieldset disabled={overview} className={field}><legend>Confidence</legend>
      <label className={row} htmlFor="graph-confidence">Minimum <output className="ml-auto tabular-nums">{filters.minConfidence.toFixed(2)}</output><span className="tabular-nums text-fg-muted">{count()} links</span></label>
      <input id="graph-confidence" type="range" min="0" max="1" step="0.05" value={filters.minConfidence} onChange={event => change({ minConfidence: Number(event.target.value) })} className="h-11 w-full accent-(--accent-solid) md:h-6" />
      <label className={row}><input type="checkbox" checked={filters.noScore} onChange={event => change({ noScore: event.target.checked })} className="size-4 accent-(--accent-solid)" />Include links with no score<span className={countClass}>{filterLinks(links, summary.documents, { ...filters, noScore: true }, undefined, view).filter(link => link.records.some(record => record.confidence === null)).length}</span></label>
    </fieldset>
    <fieldset className={field}><legend>Evidence documents per link</legend>
      <div className="flex gap-1">{[1, 2, 3, 5, 10].map(value => <Button key={value} variant={filters.minDocs === value ? "default" : "outline"} aria-pressed={filters.minDocs === value}
        aria-label={`At least ${value} ${value === 1 ? "document" : "documents"}, ${count({ minDocs: value })} links`} onClick={() => change({ minDocs: value })} className="h-11 flex-1 flex-col gap-0 px-1 tabular-nums"><span>{value}</span><span className="label-12">{count({ minDocs: value })}</span></Button>)}</div>
    </fieldset>
    <fieldset disabled={overview} className={field}><legend>Document date</legend>
      <div className="grid grid-cols-2 gap-3">{([['from', 'From'], ['to', 'To']] as const).map(([key, label]) => <label key={key} className="min-w-0 space-y-2 label-13"><span className="flex justify-between">{label}<span className="tabular-nums text-fg-muted">{count()}</span></span>
        <input aria-label={label} type="month" value={filters[key]} max={key === "from" ? filters.to || undefined : undefined} min={key === "to" ? filters.from || undefined : undefined}
          onChange={event => change({ [key]: event.target.value })} className="h-11 w-full min-w-0 rounded-md border border-border-strong bg-surface px-2 text-base text-fg md:h-9 md:text-sm" /></label>)}</div>
    </fieldset>
    <fieldset disabled={overview} className={field}><legend>Document type</legend>
      <div className="flex flex-wrap gap-2">{types.map(key => <Button key={key} variant="outline" aria-pressed={filters.types === null || filters.types.includes(key)}
        onClick={() => { const next = toggle(filters.types ?? types, key); change({ types: next.length === types.length ? null : next }); }}
        className="h-11 gap-2 px-2.5 aria-pressed:border-accent-line aria-pressed:bg-accent-wash md:h-8"><TypeDot type={key} /><span className="tabular-nums text-fg-muted">{count({ types: [key] })}</span></Button>)}</div>
    </fieldset>
    <fieldset disabled={overview} className="space-y-2 [&>legend]:mb-3 [&>legend]:label-13 [&>legend]:font-semibold"><legend>Owner</legend>
      <label className={row}><input type="checkbox" checked={ownerHidden(filters, view)} onChange={event => change({ owner: event.target.checked ? "hide" : "show" })} className="size-4 accent-(--accent-solid)" />Hide the owner entity<span className={countClass}>{count({ owner: "hide" })}</span></label>
    </fieldset>
  </div>;
}

export function GraphFiltersSheet(props: Props) {
  const chips = filterChips(props.filters);
  const total = filterLinks(props.view === "table" ? props.summary.links : drawableLinks(props.summary), props.summary.documents, props.filters, undefined, props.view).length;
  return <Sheet>
    <SheetTrigger asChild><Button variant="outline" className="h-11 gap-2 md:h-9"><SlidersHorizontal className="size-4" />Filters{chips.length > 0 && <span className="tabular-nums">{chips.length}</span>}</Button></SheetTrigger>
    <SheetContent side="left" className="gap-0 max-md:max-w-none md:w-[380px]" aria-describedby="graph-filter-description">
      <div className="border-b px-5 pb-4 pt-5"><SheetTitle className="heading-16">Filters</SheetTitle><SheetDescription id="graph-filter-description" className="mt-1">{props.summary.focus.name} · <span role="status" className="tabular-nums">{total} links match</span></SheetDescription></div>
      <div className="min-h-0 flex-1 overflow-y-auto p-5"><GraphFilterControls {...props} /></div>
      <div className="flex shrink-0 items-center justify-between gap-3 border-t p-4 pb-[calc(16px+env(safe-area-inset-bottom))]">
        <Button variant="ghost" className="h-11" onClick={() => props.onChange(defaultFilters())}>Reset filters</Button>
        <SheetClose asChild><Button className="h-11 tabular-nums">Show {total} links</Button></SheetClose>
      </div>
    </SheetContent>
  </Sheet>;
}

export function GraphFilterChips({ filters, onChange }: Pick<Props, "filters" | "onChange">) {
  const chips = filterChips(filters);
  if (!chips.length) return null;
  return <div aria-label="Active filters" role="group" className="flex shrink-0 items-center gap-2 overflow-x-auto border-b px-4 py-2 md:flex-wrap md:px-5">
    {chips.map(chip => <Button key={chip.label} variant="outline" aria-label={`Remove ${chip.label}`} onClick={() => onChange({ ...filters, ...chip.clear })}
      className="h-auto min-h-11 shrink-0 md:max-w-full md:whitespace-normal border-accent-line bg-accent-wash px-2.5 text-left label-12 md:min-h-8">{chip.label}<X className="size-3.5 shrink-0" /></Button>)}
    <Button variant="link" className="h-11 shrink-0 px-1 label-12 md:h-8" onClick={() => onChange(defaultFilters())}>Reset filters</Button>
  </div>;
}
