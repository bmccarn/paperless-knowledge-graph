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

/** The legend's rows: kinds in view, link styles in view, and the size rule. */
export function LegendContent({ items }: { items: LegendItems }) {
  return (
    <>
      <ul aria-label="Kinds" className="grid grid-cols-2 gap-x-3 gap-y-1 border-b border-border pb-2">
        {items.labels.map(label => <li key={label}><KindShape label={label} className="whitespace-nowrap label-12 text-fg-muted" /></li>)}
      </ul>
      <ul aria-label="Links" className="flex flex-wrap gap-x-4 gap-y-1 py-2">
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

/** Bottom left of the canvas, open by default and collapsible. */
export function GraphLegend({ items, className }: { items: LegendItems; className?: string }) {
  const [open, setOpen] = useState(true);
  const toggle = (
    <button type="button" aria-expanded={open} aria-controls="graph-legend-content" onClick={() => setOpen(value => !value)}
      className={cn("inline-flex items-center gap-1.5 rounded-md text-fg", open
        ? "size-6 justify-center text-fg-muted hover:bg-raised"
        : "h-8 border border-border-strong bg-surface px-3 label-12 font-medium hover:bg-raised")}>
      {open ? <ChevronDown aria-hidden className="size-3.5" /> : <ChevronUp aria-hidden className="size-3.5" />}
      <span className={open ? "sr-only" : undefined}>Legend</span>
    </button>
  );
  if (!open) return <div className={className}>{toggle}</div>;
  return (
    <section aria-labelledby="graph-legend-title" className={cn("w-66 rounded-[10px] border border-border bg-surface px-3 pb-3 pt-2.5", className)}>
      <div className="mb-1.5 flex items-center justify-between">
        <h2 id="graph-legend-title" className="label-12 font-semibold text-fg">Legend</h2>
        {toggle}
      </div>
      <div id="graph-legend-content"><LegendContent items={items} /></div>
    </section>
  );
}
