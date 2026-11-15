"use client";

import { useState } from 'react';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { KindShape } from '@/components/kind-shape';
import type { LegendItems } from '@/lib/graph-inspector';
import { cn } from '@/lib/utils';

const STYLES = {
  supported: { text: 'Supported', stroke: 'var(--graph-edge)', width: 2, dash: undefined },
  inferred: { text: 'Inferred', stroke: 'var(--graph-edge)', width: 2, dash: '6 4' },
  aggregate: { text: 'Hidden group', stroke: 'var(--border-strong)', width: 1, dash: '3 3' },
};

/** The legend's rows: kinds in view, link styles in view, and the size rule; `inline` puts them on wrapping lines. */
export function LegendContent({ items, inline = false }: { items: LegendItems; inline?: boolean }) {
  return (
    <>
      <ul aria-label="Kinds" className={inline ? "flex flex-wrap gap-x-4 gap-y-1" : "grid grid-cols-2 gap-x-3 gap-y-1 border-b border-border pb-2"}>
        {items.labels.map(label => <li key={label}><KindShape label={label} className="whitespace-nowrap label-12 text-fg-muted" /></li>)}
      </ul>
      <ul aria-label="Links" className={cn("flex flex-wrap gap-x-4 gap-y-1", !inline && "py-2")}>
        {items.styles.map(style => (
          <li key={style} data-style={style} className="flex items-center gap-2 label-12 text-fg-muted">
            <svg aria-hidden width="24" height="10">
              <line x1="1" y1="5" x2="23" y2="5" stroke={STYLES[style].stroke} strokeWidth={STYLES[style].width} strokeDasharray={STYLES[style].dash} />
            </svg>
            {STYLES[style].text}
          </li>
        ))}
      </ul>
      <p className="flex items-center gap-2 label-12 text-fg-muted">
        <svg aria-hidden width="46" height="20" className="fill-none stroke-fg-subtle" strokeWidth={1.5}>
          <circle cx="7" cy="12" r="4" /><circle cx="20" cy="12" r="6" /><circle cx="36" cy="10" r="9" />
        </svg>
        Size: evidence documents
      </p>
    </>
  );
}

const OPEN_KEY = "graph-legend-open";

/**
 * Desktop legend, placed under the canvas. Collapsed (the default) it is a button at the canvas's bottom left;
 * open, a strip above the evidence shelf, so it never covers a node. The open state is kept in browser storage,
 * unless `defaultOpen` sets it (the gallery).
 */
export function GraphLegend({ items, className, defaultOpen }: { items: LegendItems; className?: string; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(() => {
    if (defaultOpen !== undefined) return defaultOpen;
    try { return localStorage.getItem(OPEN_KEY) === "1"; } catch { return false; }
  });
  const toggle = () => {
    setOpen(!open);
    if (defaultOpen !== undefined) return;
    try { localStorage.setItem(OPEN_KEY, open ? "0" : "1"); } catch { /* storage unavailable: keep it for this view */ }
  };
  // One element tree in both states keeps keyboard focus on the toggle.
  return (
    <section aria-labelledby={open ? "graph-legend-title" : undefined} className={cn(open
      ? "flex shrink-0 items-start gap-x-6 border-t border-border bg-background py-2.5 pl-4 pr-4 md:pl-6"
      : "relative h-0", className)}>
      <div className={open ? "flex h-6 shrink-0 items-center gap-1" : "absolute bottom-4 left-4"}>
        {open && <h2 id="graph-legend-title" className="label-13 font-semibold text-fg">Legend</h2>}
        <button type="button" aria-expanded={open} aria-controls="graph-legend-content" onClick={toggle}
          className={cn("inline-flex items-center gap-1.5 rounded-md text-fg", open
            ? "size-6 justify-center text-fg-muted hover:bg-raised"
            : "h-8 border border-border-strong bg-surface px-3 label-12 font-medium hover:bg-raised")}>
          {open ? <ChevronDown aria-hidden className="size-3.5" /> : <ChevronUp aria-hidden className="size-3.5" />}
          <span className={open ? "sr-only" : undefined}>Legend</span>
        </button>
      </div>
      {open && (
        <div id="graph-legend-content" className="flex min-w-0 flex-1 flex-wrap items-center gap-x-6 gap-y-1 py-0.5">
          <LegendContent items={items} inline />
        </div>
      )}
    </section>
  );
}
