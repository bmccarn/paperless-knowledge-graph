"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { KindShape } from "@/components/kind-shape";
import { documentType, kindColor, nodeKind } from "@/lib/categories";
import { linkWidth } from "@/lib/graph-encoding";
import { drawableLinks, type NodeSummary } from "@/lib/graph-layout";
import { documentsByDate, entityName, plural, shortDate, typeWords } from "@/lib/graph-inspector";
import { documentMonth, inRange, monthLabel, timelineAxis, timelineLabels, timelineLanes, timelineTracks, timelineRowName } from "@/lib/graph-timeline";
import type { GraphFilters } from "@/lib/graph-filters";
import { cn } from "@/lib/utils";

export function TimelineView({ summary, unfilteredTotal, filters, phone, selection, onRange, onSelect, onMark, onReset }: {
  summary: NodeSummary; unfilteredTotal: number; filters: GraphFilters; phone: boolean; selection: string | null;
  onRange: (from: string, to: string) => void; onSelect: (id: string, docId?: number) => void;
  onMark: (id: number | null) => void; onReset: () => void;
}) {
  const axisRef = useRef<SVGSVGElement>(null);
  const [trackWidth, setTrackWidth] = useState(500);
  useEffect(() => {
    if (!axisRef.current) return;
    const observer = new ResizeObserver(([entry]) => setTrackWidth(entry.contentRect.width));
    observer.observe(axisRef.current);
    return () => observer.disconnect();
  }, [phone]);
  const [extra, setExtra] = useState<Record<string, number>>({});
  const [brush, setBrush] = useState<[number, number] | null>(null);
  const brushStart = useRef<number | null>(null);
  const months = useMemo(() => timelineAxis(summary), [summary]);
  const lanes = useMemo(() => timelineLanes(summary, extra), [summary, extra]);
  const documents = useMemo(() => documentsByDate(summary), [summary]);
  const links = useMemo(() => drawableLinks(summary), [summary]);
  const groups = useMemo(() => {
    const result = new Map<string, typeof documents>();
    for (const doc of documents) {
      const month = documentMonth(doc.date) ?? "No date";
      result.set(month, [...(result.get(month) ?? []), doc]);
    }
    return [...result].sort(([a], [b]) => a === "No date" ? 1 : b === "No date" ? -1 : b.localeCompare(a));
  }, [documents]);
  const types = [...new Set(documents.map(doc => doc.doc_type ?? ""))];
  const dated = documents.filter(doc => documentMonth(doc.date));
  const noLinks = links.length === 0;
  const x = (month: string | null) => month === null ? trackWidth - 28 : 16 + Math.max(0, months.indexOf(month)) / Math.max(1, months.length - 1) * (trackWidth - 100);
  const monthAt = (event: React.PointerEvent<SVGSVGElement>) => {
    const bounds = event.currentTarget.getBoundingClientRect();
    return Math.max(0, Math.min(months.length - 1, Math.round(((event.clientX - bounds.left) - 16) / (trackWidth - 100) * Math.max(1, months.length - 1))));
  };
  const from = brush ? months[Math.min(...brush)] : filters.from;
  const to = brush ? months[Math.max(...brush)] : filters.to;
  const rangeX = (month: string, fallback: number) => month ? month < months[0] ? 0 : month > months[months.length - 1] ? trackWidth - 84 : x(month) : fallback;
  const range = Boolean(from || to);
  const markTitle = (ids: number[], inferred: boolean, type: string) => ids.map(id => {
    const doc = summary.documents[String(id)];
    return `${doc?.title || "Untitled document"}, ${documentType(doc?.doc_type ?? "").label || "No type"}, ${documentMonth(doc?.date) ? shortDate(doc.date) || monthLabel(documentMonth(doc?.date)!, true) : "No date"}, ${inferred ? "Inferred" : "Stated"}, ${typeWords(type)}`;
  }).join("; ");

  const legend = (
    <div aria-label="Timeline legend" className={cn("flex shrink-0 flex-wrap items-center gap-x-5 gap-y-2 border-t border-border px-4 py-3 label-12 text-fg-muted", phone && "max-h-28 overflow-y-auto")}>
        <span className="flex items-center gap-2"><span className="size-2 rounded-full bg-fg-muted" />Stated document</span>
        <span className="flex items-center gap-2"><span className="size-2 rounded-full border-2 border-fg-muted" />Inferred document</span>
        <span className="flex items-center gap-2"><span className="w-6 border-t-2 border-graph-edge" />Supported link</span>
        <span className="flex items-center gap-2"><span className="w-6 border-t-2 border-dashed border-graph-edge" />Inferred link</span>
        <span>Marks take the color of the linked entity&apos;s kind.</span>
        {types.map(type => <span key={type} className="flex items-center gap-2"><span className="size-2 rounded-full" style={{ background: documentType(type).color }} />{documentType(type).label || "No type"}</span>)}
      </div>
  );

  return (
    <section aria-label={`Timeline of ${entityName(summary.focus)}`} className="flex min-h-0 flex-1 flex-col">
      <div className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3 md:px-6">
        <p role="status" className="label-13 text-fg-muted">Timeline of {entityName(summary.focus)}: {plural(links.length, "link")}, {plural(documents.length, "document")}.
          {(filters.from || filters.to) && ` ${documents.filter(doc => inRange(documentMonth(doc.date), filters.from, filters.to)).length} documents fall in the date range.`}</p>
        <div role="group" aria-label="Timeline date range" className="flex items-center gap-2 label-12 text-fg-muted">
          <label htmlFor="timeline-from">From</label><Input id="timeline-from" type="month" value={filters.from} max={filters.to || undefined} onChange={event => onRange(event.target.value, filters.to)} className="h-11 w-36 text-xs md:h-9 md:text-sm" />
          <label htmlFor="timeline-to">To</label><Input id="timeline-to" type="month" value={filters.to} min={filters.from || undefined} onChange={event => onRange(filters.from, event.target.value)} className="h-11 w-36 text-xs md:h-9 md:text-sm" />
        </div>
      </div>
      {!dated.length && <p className="border-b px-4 py-3 copy-14 text-fg-muted">None of the {documents.length} documents has a date.</p>}
      {noLinks && <div className="border-b px-4 py-4 copy-14 text-fg-muted">
        <p>{unfilteredTotal ? `Your filters hide all ${unfilteredTotal} links.` : `No direct links extracted. This entity appears in ${plural(documents.length, "document")}.`}</p>
        {unfilteredTotal > 0 && <Button variant="link" className="px-0" onClick={onReset}>Reset filters</Button>}
      </div>}
      {phone ? (
        <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-4">
          {groups.map(([month, docs], groupIndex) => <section key={month} aria-label={month === "No date" ? month : monthLabel(month, true)}>
            {month !== "No date" && (groupIndex === 0 || groups[groupIndex - 1][0].slice(0, 4) !== month.slice(0, 4)) && <h2 className="py-4 font-[family-name:var(--font-geist-serif-regular)] text-xl text-fg">{month.slice(0, 4)}</h2>}
            <h3 aria-label={month === "No date" ? month : monthLabel(month, true)} className="sticky top-0 z-10 border-b bg-background py-3 label-13 font-semibold text-fg-muted">{month === "No date" ? month : monthLabel(month, true).split(" ")[0]}</h3>
            <ul>{docs.map(doc => {
              const supported = links.filter(link => link.records.some(record => record.doc_id === doc.id));
              const content = <>
                  <span aria-hidden className="mt-2 size-2 shrink-0 rounded-full" style={{ background: inRange(documentMonth(doc.date), from, to) ? documentType(doc.doc_type ?? "").color : "var(--graph-faded)" }} />
                  <span className="min-w-0"><span className="block font-[family-name:var(--font-source-serif)] text-base text-fg">{doc.title}</span>
                    <span className="label-12 text-fg-muted">{documentMonth(doc.date) ? shortDate(doc.date) || monthLabel(documentMonth(doc.date)!, true) : "No date"} · {documentType(doc.doc_type ?? "").label || "No type"}{!inRange(documentMonth(doc.date), from, to) && " · Outside date range"}</span></span>
              </>;
              return <li key={doc.id} className="min-h-12 border-b border-border py-3" data-outside-range={!inRange(documentMonth(doc.date), from, to) || undefined}>
                {supported.length ? <button type="button" className="flex min-h-12 w-full items-start gap-3 text-left" onClick={() => onSelect(supported[0].key, doc.id)}>{content}</button>
                  : <Link href={`/documents/${doc.id}`} className="flex min-h-12 w-full items-start gap-3 text-left">{content}</Link>}
                {supported.length > 0 && <ul className="ml-5 space-y-1">{supported.map(link => <li key={link.key}><button type="button" className="min-h-11 text-left label-13 text-fg-muted underline decoration-border-strong underline-offset-4" onClick={() => { onSelect(link.key, doc.id); }}>
                  {link.direction === "in" ? `${entityName(link.neighbor)} ${typeWords(link.type).toLowerCase()} ${entityName(summary.focus)}` : `${typeWords(link.type)} ${entityName(link.neighbor)}`}{link.records.find(record => record.doc_id === doc.id)?.inferred ? " · Inferred" : " · Stated"}
                </button></li>)}</ul>}
              </li>;
            })}</ul>
          </section>)}
        </div>
      ) : (
        <div className="min-h-0 flex-1 overflow-auto px-6 pb-6" data-timeline-desktop>
          <div className="min-w-[820px]">
            <div className="sticky top-0 z-10 grid grid-cols-[176px_220px_minmax(0,1fr)] bg-background pt-3">
              <div className="col-span-2" />
              <svg role="img" aria-label="Drag across the axis to set the document date range" ref={axisRef} viewBox={`0 0 ${trackWidth} 54`} preserveAspectRatio="none" className="h-14 w-full touch-none select-none cursor-crosshair"
                onPointerDown={event => { if (!months.length) return; event.currentTarget.setPointerCapture(event.pointerId); brushStart.current = monthAt(event); setBrush([brushStart.current, brushStart.current]); }}
                onPointerMove={event => { if (brushStart.current !== null) setBrush([brushStart.current, monthAt(event)]); }}
                onPointerUp={event => { if (brushStart.current === null) return; const end = monthAt(event); onRange(months[Math.min(brushStart.current, end)], months[Math.max(brushStart.current, end)]); brushStart.current = null; setBrush(null); }}
                onPointerCancel={() => { brushStart.current = null; setBrush(null); }}>
                {months.map(month => <line key={month} x1={x(month)} x2={x(month)} y1="36" y2={month.endsWith("-01") ? 46 : 40} stroke="var(--border-strong)" />)}
                {timelineLabels(months, trackWidth).map(label => <text key={label.index} x={x(months[label.index])} y="24" textAnchor={label.index === 0 ? "start" : label.index === months.length - 1 ? "end" : "middle"} fill="var(--fg-muted)" fontSize="12">{label.text}</text>)}
                <text x={trackWidth - 28} y="24" textAnchor="middle" fill="var(--fg-muted)" fontSize="12">No date</text>
                {range && months.length > 0 && <rect x={rangeX(from, 0)} y="32" width={Math.max(2, rangeX(to, trackWidth - 84) - rangeX(from, 0))} height="20" fill="var(--mark)" stroke="var(--mark-line)" />}
              </svg>
            </div>
            <div className="grid grid-cols-[176px_220px_minmax(0,1fr)] border-b border-border-strong">
              <div className="col-span-2 flex flex-col justify-end pb-2 label-12 text-fg-muted"><strong>Documents</strong><span>{plural(documents.length, "evidence document")}</span></div>
              <div className="relative h-24">
                {groups.map(([month, docs]) => <div key={month} className="absolute bottom-2 flex -translate-x-1/2 flex-col-reverse items-center gap-0.5" style={{ left: `${x(month === "No date" ? null : month) / trackWidth * 100}%` }}>
                  {docs.slice(0, 6).map(doc => <span key={doc.id} role="img" aria-label={`${doc.title}, ${documentMonth(doc.date) ? shortDate(doc.date) || monthLabel(documentMonth(doc.date)!, true) : "No date"}`} title={doc.title}
                    className="size-2 rounded-full" style={{ background: inRange(documentMonth(doc.date), from, to) ? documentType(doc.doc_type ?? "").color : "var(--graph-faded)" }}
                    onMouseEnter={() => onMark(doc.id)} onMouseLeave={() => onMark(null)} />)}
                  {docs.length > 6 && <span className="label-12 text-fg-muted">+{docs.length - 6}</span>}
                </div>)}
              </div>
            </div>
            {lanes.map(lane => <div key={lane.family} data-timeline-family={lane.family} className="grid grid-cols-[176px_220px_minmax(0,1fr)] border-b border-border py-2">
              <h2 className="row-span-full pr-4 pt-3 label-12 font-semibold text-fg-muted" style={{ gridRow: `span ${lane.rows.length + (lane.hidden ? 1 : 0)}` }}>{lane.name}</h2>
              {lane.rows.map(row => {
                const { tracks, height } = timelineTracks(row.links, summary.documents, (trackWidth - 100) / Math.max(1, months.length - 1));
                return <div key={row.id} className="contents" data-timeline-row={row.id}>
                <button type="button" aria-label={timelineRowName(row, row.links, summary.documents)} aria-pressed={selection === row.id} onClick={() => onSelect(row.id)}
                  className="flex min-h-11 min-w-0 items-center gap-2 pr-3 text-left label-13 text-fg hover:bg-raised aria-pressed:bg-raised" title={entityName(row)}>
                  <KindShape label={row.label} bare /><span className="truncate">{entityName(row)}</span>
                </button>
                <svg aria-label={`Document marks for ${entityName(row)}`} role="group" viewBox={`0 0 ${trackWidth} ${height}`} preserveAspectRatio="none" className="w-full overflow-visible" style={{ height }}>
                  {range && months.length > 0 && <rect x={rangeX(from, 0)} width={Math.max(2, rangeX(to, trackWidth - 84) - rangeX(from, 0))} height="100%" fill="var(--raised)" />}
                  <line x1={trackWidth - 60} x2={trackWidth - 60} y1="0" y2="100%" stroke="var(--border)" strokeDasharray="3 4" />
                  {/* Keep visible marks above every neighboring hit area, including other tracks. */}
                  {tracks.flatMap(({ link, marks, y }) => marks.map((mark, index) => <circle key={`${link.key}:${index}`} aria-hidden cx={x(mark.month) + mark.dx} cy={y + mark.dy} r="12" fill="transparent" className="cursor-pointer" onClick={() => onSelect(link.key, mark.ids[0])} />))}
                  {tracks.map(({ link, marks, y }) => {
                    const dates = marks.filter(mark => mark.month !== null);
                    return <g key={link.key} data-timeline-link={link.key}>
                      {dates.length > 0 && <line x1={x(dates[0].month)} x2={x(dates[dates.length - 1].month)} y1={y} y2={y} pointerEvents="none" stroke="var(--graph-edge)" strokeWidth={linkWidth(link.records.length)} strokeDasharray={link.support_state === "inferred" ? "6 4" : undefined} />}
                      {marks.map((mark, index) => {
                        const title = mark.ids.length > 1
                          ? `${plural(mark.ids.length, "document")}, ${mark.month ? monthLabel(mark.month, true) : "No date"}, ${mark.inferred ? "Inferred" : link.records.some(record => mark.ids.includes(record.doc_id) && record.inferred) ? "Stated and inferred" : "Stated"}, ${typeWords(link.type)}`
                          : markTitle(mark.ids, mark.inferred, link.type);
                        return <g key={index} role="button" tabIndex={0} className="group" aria-pressed={selection === link.key} aria-label={title} data-month={mark.month ?? "no-date"} data-inferred={mark.inferred} data-document-ids={mark.ids.join(",")}
                          onClick={() => { onSelect(link.key, mark.ids[0]); }} onKeyDown={event => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelect(link.key, mark.ids[0]); } }}
                          onMouseEnter={() => onMark(mark.ids[0])} onMouseLeave={() => onMark(null)} onFocus={() => onMark(mark.ids[0])} onBlur={() => onMark(null)}
                          style={{ cursor: "pointer" }}>
                          <title>{title}</title><circle cx={x(mark.month) + mark.dx} cy={y + mark.dy} r="10" fill="none" pointerEvents="none" stroke="var(--fg)" strokeWidth="2" className={selection === link.key ? undefined : "opacity-0 group-focus-visible:opacity-100"} /><circle cx={x(mark.month) + mark.dx} cy={y + mark.dy} r="12" fill="transparent" pointerEvents="none" />
                          <circle cx={x(mark.month) + mark.dx} cy={y + mark.dy} r="6" fill={mark.inferred ? "var(--bg)" : (inRange(mark.month, from, to) ? kindColor(nodeKind(row.label).kind) : "var(--graph-faded)")} stroke={(inRange(mark.month, from, to) ? kindColor(nodeKind(row.label).kind) : "var(--graph-faded)")} strokeWidth="2" />
                          {mark.ids.length > 1 && <text x={x(mark.month) + mark.dx + 10} y={y + mark.dy + 4} fontSize="12" fill="var(--fg-muted)">{mark.ids.length}</text>}
                        </g>;
                      })}
                    </g>;
                  })}
                </svg>
              </div>;
              })}
              {lane.hidden > 0 && <><Button variant="link" className="col-span-2 h-11 justify-start px-0 label-13 text-fg-muted" onClick={() => setExtra({ ...extra, [lane.family]: (extra[lane.family] ?? 0) + 25 })}>+{lane.hidden} more {lane.name.toLowerCase()}</Button></>}
            </div>)}
          </div>
        </div>
      )}
      {phone ? <details className="shrink-0 border-t border-border">
        <summary className="flex min-h-11 cursor-pointer items-center px-4 label-13 text-fg-muted">Timeline legend</summary>
        {legend}
      </details> : legend}
    </section>
  );
}
