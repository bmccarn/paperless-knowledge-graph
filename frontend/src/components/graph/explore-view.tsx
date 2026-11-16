"use client";

import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import dynamic from "next/dynamic";
import { ChevronUp, Info, Loader2, Maximize, Minus, Plus, RotateCw, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { KindShape } from "@/components/kind-shape";
import { ApiError, getGraphNodeSummary, getGraphSubgraph } from "@/lib/api";
import type { ExplorerGraph, ExplorerLink, ExplorerNode } from "@/lib/graph-data";
import {
  collapse,
  exploreCrossLinks,
  exploreLayout,
  exploreLimits,
  exploreName,
  hiddenNeighbors,
  MAX_HOP,
  type ExploreLink,
  type ExploreNode,
} from "@/lib/graph-explore";
import { defaultFilters, filterLinks, ownerHidden, type GraphFilters } from "@/lib/graph-filters";
import { documentsByDate, entityName, legendItems, linksOfDocument, plural } from "@/lib/graph-inspector";
import { crossCap, drawableLinks, type NodeSummary, type Subgraph, type SubgraphLink, type SummaryLink } from "@/lib/graph-layout";
import { EXPANSION_LIMIT, parseLinkKey, type GraphUrlState } from "@/lib/graph-url-state";
import { cn } from "@/lib/utils";
import type { Camera } from "./force-graph-client";
import { EvidenceShelf } from "./evidence-shelf";
import { ExploreInspector, FocusInspector, LinkInspector } from "./entity-inspector";
import { GraphLegend, LegendContent } from "./graph-legend";

const ForceGraphClient = dynamic(() => import("./force-graph-client").then((module) => module.ForceGraphClient), { ssr: false });

/** The trail panel's width plus its 16 px margin, kept clear of nodes on desktop. */
const TRAIL_INSET = 288;

// Summaries of expanded entities, cached for the session; a failed request is dropped so Retry asks again.
const summaries = new Map<string, NodeSummary>();
const requests = new Map<string, Promise<NodeSummary>>();
function loadSummary(id: string): Promise<NodeSummary> {
  let request = requests.get(id);
  if (!request) {
    request = getGraphNodeSummary(id).then(data => { summaries.set(id, data); return data; });
    request.catch(() => requests.delete(id));
    requests.set(id, request);
  }
  return request;
}

/** Loads the summaries of `wanted`, once each; `arrived` and `setFailed` report back. */
function useLoad(wanted: readonly string[], failed: ReadonlySet<string>, arrived: () => void, setFailed: (update: (previous: ReadonlySet<string>) => ReadonlySet<string>) => void) {
  const missing = [...new Set(wanted)].filter(id => !summaries.has(id) && !failed.has(id)).sort().join(",");
  useEffect(() => {
    if (!missing) return;
    let active = true;
    for (const id of missing.split(",")) loadSummary(id).then(
      () => { if (active) arrived(); },
      error => { if (active && !(error instanceof ApiError && error.status === 404)) setFailed(previous => new Set(previous).add(id)); },
    );
    return () => { active = false; };
  }, [missing, arrived, setFailed]);
}

interface ExploreViewProps {
  root: NodeSummary;
  filters: GraphFilters;
  url: GraphUrlState;
  phone: boolean;
  navigate: (change: Partial<GraphUrlState>) => void;
  onResetFilters: (filters: GraphFilters) => void;
}

/** `view=explore`: a network that grows in place from the focus entity, up to three hops. */
export function ExploreView({ root: rawRoot, filters, url, phone, navigate, onResetFilters }: ExploreViewProps) {
  const xKey = url.expanded.join(",");
  const x = useMemo(() => (xKey ? xKey.split(",") : []), [xKey]);
  const limits = useMemo(() => exploreLimits(phone), [phone]);
  const [hovered, setHovered] = useState<string | null>(null);
  const [keyboardNodeId, setKeyboardNodeId] = useState<string | null>(null);
  const [markedDoc, setMarkedDoc] = useState<number | null>(null);
  const [notice, setNotice] = useState<{ x: string; text: string } | null>(null);
  const [trailOpen, setTrailOpen] = useState(false);
  const cameraRef = useRef<Camera | null>(null);
  const rootId = rawRoot.focus.id;

  const selectedLinkId = url.selection && parseLinkKey(url.selection) ? url.selection : null;
  const selectedNodeId = selectedLinkId ? null : url.selection;
  const [version, arrived] = useReducer((value: number) => value + 1, 0);
  const [failed, setFailed] = useState<ReadonlySet<string>>(new Set());
  const retry = (id: string) => setFailed(previous => {
    const next = new Set(previous);
    next.delete(id);
    return next;
  });
  const filtered = useCallback((summary: NodeSummary): NodeSummary =>
    ({ ...summary, links: filterLinks(summary.links, summary.documents, filters, undefined, "explore") }), [filters]);
  const root = useMemo(() => filtered(rawRoot), [filtered, rawRoot]);
  const known = useMemo(() => {
    void version; // the cache is module state; a new version means a new summary arrived
    return new Map([...summaries].map(([id, summary]) => [id, filtered(summary)])).set(rootId, root);
  }, [filtered, version, rootId, root]);
  const layout = useMemo(() => exploreLayout(root, known, x, limits), [root, known, x, limits]);
  const pendingId = layout.pending === null ? null : x[layout.pending];
  const pendingFailed = pendingId !== null && failed.has(pendingId);
  // A reload with `x` draws once every expansion has arrived, so the first fit sees the whole picture.
  const [drawn, setDrawn] = useState(false);
  if (!drawn && (layout.pending === null || pendingFailed)) setDrawn(true);
  const loadingId = drawn && pendingId !== null && !pendingFailed ? pendingId : null;
  const [slow, setSlow] = useState<string | null>(null);
  useEffect(() => {
    if (!loadingId) return;
    const timer = setTimeout(() => setSlow(loadingId), 1000);
    return () => clearTimeout(timer);
  }, [loadingId]);

  const nodes = useMemo(() => new Map(layout.nodes.map(node => [node.id, node])), [layout]);
  // Expansions of entities in view, and the selected and hovered entities, whose summaries give their handle's count.
  useLoad([...x, selectedNodeId, hovered].filter((id): id is string => id !== null && id !== rootId && nodes.has(id)), failed, arrived, setFailed);
  // Links among everything in view that no summary holds, such as two branches that meet.
  const ids = useMemo(() => layout.nodes.map(node => node.id).sort().join(","), [layout]);
  const wantCross = drawn && !loadingId && layout.nodes.length >= 2;
  const [cross, setCross] = useState<{ ids: string; data: Subgraph | null } | null>(null);
  const [crossAttempt, setCrossAttempt] = useState(0);
  useEffect(() => {
    if (!wantCross) return;
    let active = true;
    getGraphSubgraph(ids.split(",")).then(data => { if (active) setCross({ ids, data }); }, () => { if (active) setCross({ ids, data: null }); });
    return () => { active = false; };
  }, [wantCross, ids, crossAttempt]);
  const crossFailed = cross?.ids === ids && !cross.data;
  const extra = useMemo(() => cross?.data
    ? exploreCrossLinks(layout, { ...cross.data, links: filterLinks(cross.data.links, cross.data.documents, filters, undefined, "explore") }, crossCap(phone))
    : null, [cross, layout, filters, phone]);
  const links: ExploreLink[] = useMemo(() => [...(extra?.links ?? []), ...layout.links], [extra, layout]);

  const graph: ExplorerGraph = useMemo(() => ({
    nodes: layout.nodes.map(({ id, name, label, evidence_doc_count, link_count, is_owner }) =>
      ({ id, name, label, props: { evidence_doc_count, link_count, is_owner } })),
    // Array order is drawing order: links that no expansion drew go under the others.
    links: links.map(({ key, source, target, type, link }) => ({ id: key, source, target, type,
      props: { implied: link.support_state === "inferred", source_doc_ids: link.records.map(record => record.doc_id) } })),
    discarded: 0,
  }), [layout, links]);
  const positions = useMemo(() => new Map(layout.nodes.map(({ id, x, y }) => [id, { x, y }])), [layout]);
  const inView = useMemo(() => new Set(nodes.keys()), [nodes]);
  /** The entities a node could still add, or null before its summary arrives. */
  const hidden = useCallback((id: string) => {
    const summary = known.get(id);
    return summary ? hiddenNeighbors(summary, inView) : null;
  }, [known, inView]);
  const handles = useMemo(() => new Map(layout.nodes.flatMap(node => {
    if (node.hop >= MAX_HOP) return [[node.id, "Open in Focus"] as const];
    const count = hidden(node.id);
    return count === null ? [] : [[node.id, count ? `+${count}` : "All links shown"] as const];
  })), [layout, hidden]);

  const openFocus = useCallback((id: string) => navigate({ focus: id, view: "focus" }), [navigate]);
  const expand = useCallback((id: string) => {
    const node = nodes.get(id);
    if (!node) return;
    if (node.hop >= MAX_HOP) return openFocus(id);
    if (hidden(id) === 0) return;
    const key = x.join();
    if (x.length >= EXPANSION_LIMIT) setNotice({ x: key, text: `This view holds at most ${EXPANSION_LIMIT} expansions. Collapse a branch or open one entity in Focus.` });
    else if (layout.nodes.length >= limits.entities) setNotice({ x: key, text: `This view holds at most ${limits.entities} entities. Collapse a branch or open one entity in Focus.` });
    else navigate({ expanded: [...x, id] });
  }, [nodes, hidden, x, layout, limits.entities, navigate, openFocus]);
  const collapseAt = useCallback((indices: number[]) => {
    const next = collapse(layout, x, indices);
    // A selected entity that the collapse removes is no longer selected.
    const removed = url.selection && !parseLinkKey(url.selection) && !exploreLayout(root, known, next, limits).nodes.some(node => node.id === url.selection);
    navigate({ expanded: next, ...(removed ? { selection: null } : {}) });
  }, [layout, x, url.selection, root, known, limits, navigate]);
  const expansionsOf = useCallback((id: string) => layout.expansions.filter(item => item.id === id).map(item => item.index), [layout]);

  // + grows the selected node and - collapses it, as the handle and the inspector buttons do.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (!selectedNodeId || !nodes.has(selectedNodeId) || event.defaultPrevented || event.ctrlKey || event.metaKey || event.altKey
        || target?.closest("input, textarea, select, [contenteditable]")) return;
      if (event.key === "+" || event.key === "=") expand(selectedNodeId);
      else if (event.key === "-" && expansionsOf(selectedNodeId).length) collapseAt(expansionsOf(selectedNodeId));
      else return;
      event.preventDefault();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [selectedNodeId, nodes, expand, collapseAt, expansionsOf]);

  const selectNode = useCallback((node: ExplorerNode) => { navigate({ selection: node.id }); setKeyboardNodeId(null); }, [navigate]);
  const selectLinkKey = useCallback((key: string) => { if (parseLinkKey(key)) navigate({ selection: key }); setKeyboardNodeId(null); }, [navigate]);
  const selectLink = useCallback((link: ExplorerLink) => selectLinkKey(link.id), [selectLinkKey]);
  const clearSelection = useCallback(() => { navigate({ selection: null }); setKeyboardNodeId(null); }, [navigate]);
  const onReady = useCallback((camera: Camera | null) => { cameraRef.current = camera; }, []);

  const documents = useMemo(() => documentsByDate(root), [root]);
  const allDocuments = useMemo(() => Object.assign({}, cross?.data?.documents, ...[...known.values()].map(summary => summary.documents)) as NodeSummary["documents"], [known, cross]);
  const markedLinkIds = useMemo(() => (markedDoc !== null ? linksOfDocument(root, markedDoc) : undefined), [root, markedDoc]);
  const legend = useMemo(() => legendItems({ nodes: layout.nodes.map(node => ({ ...node, kind: "neighbor" as const, family: null })), links }), [layout, links]);
  /** A drawn link as a link of `from`, for the inspector's rows and link evidence. */
  const asLink = useCallback(({ source, target, ...link }: SubgraphLink, from: string): SummaryLink => {
    const other = nodes.get(source === from ? target : source)!;
    const { id, label, name, evidence_doc_count, link_count, is_owner } = other;
    return { ...link, direction: source === from ? "out" : "in", neighbor: { id, label, name, evidence_doc_count, link_count, is_owner } };
  }, [nodes]);
  const rootName = entityName(root.focus);
  const leaf = drawableLinks(rawRoot).length === 0;
  const hiddenAll = !leaf && drawableLinks(root).length === 0;

  const selectedLink = links.find(link => link.key === selectedLinkId);
  const selectedNode = selectedNodeId ? nodes.get(selectedNodeId) : undefined;
  const selected = Boolean(selectedLink || selectedNode);
  const rowEvents = { onSelectLink: selectLinkKey, onFocusRow: setKeyboardNodeId };
  let inspector = null;
  if (selectedLink) inspector = (
    <LinkInspector summary={{ ...root, documents: allDocuments }} link={asLink(selectedLink.link, selectedLink.source)}
      end={nodes.get(selectedLink.source)!} onClose={clearSelection} />);
  else if (selectedNode?.hop === 0) inspector = <FocusInspector summary={root} onClose={clearSelection} onOpenTable={() => navigate({ view: "table" })} {...rowEvents} />;
  else if (selectedNode) {
    const count = hidden(selectedNode.id);
    const step = Math.min(limits.step, count ?? limits.step);
    const grow = selectedNode.hop >= MAX_HOP ? { label: "Open in Focus", disabled: false }
      : count === 0 ? { label: "All links shown", disabled: true }
      : { label: phone ? "Show more links" : `Show ${step} more ${step === 1 ? "link" : "links"} from here`, disabled: count === null };
    const own = expansionsOf(selectedNode.id);
    inspector = (
      <ExploreInspector key={selectedNode.id} entity={selectedNode} hop={selectedNode.hop} rootName={rootName} {...rowEvents}
        links={links.filter(link => link.source === selectedNode.id || link.target === selectedNode.id).map(link => asLink(link.link, selectedNode.id))}
        grow={{ ...grow, onClick: () => expand(selectedNode.id) }} onCollapse={own.length ? () => collapseAt(own) : undefined}
        onFocusEntity={openFocus} onClose={clearSelection} />);
  }

  const name = exploreName(root.focus, layout.nodes.length, links.length, layout.hops);
  const limitText = notice?.x === x.join() ? notice.text
    : layout.clipped ? `This view holds at most ${limits.entities} entities. Collapse a branch or open one entity in Focus.` : null;
  const ownerSkipped = ownerHidden(filters, "explore") && [rawRoot, ...layout.expansions.map(item => summaries.get(item.id))]
    .some(summary => summary && drawableLinks(summary).some(link => link.neighbor.is_owner));
  const crossNote = extra && extra.total > extra.links.length ? ` Showing the ${extra.links.length} strongest of ${extra.total} links between branches.` : "";
  const trail = (
    <Trail rootName={rootName} rootLabel={root.focus.label} rootLinks={layout.links.filter(link => link.source === root.focus.id || link.target === root.focus.id).length}
      expansions={layout.expansions.map(item => ({ ...item, node: nodes.get(item.id)! }))} pending={pendingId ? { name: entityName(nodes.get(pendingId) ?? { id: pendingId, name: "", label: "Entity" }), failed: pendingFailed } : null}
      onCollapse={index => collapseAt([index])} onRetry={() => pendingId && retry(pendingId)} />
  );

  return (
    <div className="relative flex min-h-0 flex-1 flex-col md:flex-row">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <section className="relative min-h-0 flex-1" aria-label="Graph workspace">
          {hiddenAll ? (
            <div className="flex h-full flex-col items-center justify-center gap-3 p-8 text-center">
              <p className="copy-14 text-fg-muted">Your filters hide all {drawableLinks(rawRoot).length} links.</p>
              <Button variant="outline" onClick={() => onResetFilters(defaultFilters())}>Reset filters</Button>
            </div>
          ) : leaf ? (
            <div className="flex h-full items-center justify-center p-8">
              <p className="max-w-sm text-balance text-center copy-14 text-fg-muted">
                No direct links extracted. This entity appears in {plural(documents.length, "document")}.
              </p>
            </div>
          ) : drawn ? (
            <ForceGraphClient graph={graph} positions={positions} focusId={root.focus.id} showLabels
              keyboardNodeId={keyboardNodeId ?? loadingId} markedLinkIds={markedLinkIds}
              selectedNodeId={selectedNodeId} selectedLinkId={selectedLinkId}
              onSelectNode={selectNode} onSelectLink={selectLink} onClear={clearSelection} onReady={onReady}
              name={name} phone={phone} handles={handles} onHandle={expand} onHover={setHovered}
              inset={phone ? 0 : TRAIL_INSET} panToNew maxZoom={0.9} skipOverlaps />
          ) : (
            <div role="status" className="flex h-full items-center justify-center gap-2 text-sm">
              <Loader2 className="h-5 w-5 animate-spin" />Loading links…
            </div>
          )}
          {drawn && !leaf && !hiddenAll && (
            <>
              {!phone && <div className="absolute left-4 top-4 w-[272px]">{trail}</div>}
              <p role="status" className={cn("absolute top-4 label-13 text-fg-muted", phone ? "left-4 right-16" : "left-[304px] right-16 max-w-[640px]")}>
                <span className={phone && !limitText ? "sr-only" : undefined}>
                  {name}{ownerSkipped && " The owner is hidden by the Owner filter."}{crossNote}
                </span>
                {crossFailed && <> Links between branches did not load.{" "}
                  <Button variant="link" size="xs" className="h-auto p-0 label-13" onClick={() => { setCross(null); setCrossAttempt(value => value + 1); }}>Retry</Button></>}
                {loadingId && slow === loadingId && <span className="block">Loading links of {entityName(nodes.get(loadingId)!)}</span>}
                {limitText && <span className="block text-fg">{limitText}</span>}
              </p>
              <div role="group" aria-label="Zoom" className="absolute bottom-3 right-3 flex flex-col overflow-hidden rounded-(--r-ctl) border border-border-strong bg-surface md:bottom-4 md:right-4">
                {([["Zoom in", Plus, 1.5], ["Zoom out", Minus, 1 / 1.5], ["Fit view", Maximize, 0]] as const).map(([label, Icon, factor]) => (
                  <Button key={label} variant="ghost" size="icon-lg" aria-label={label} title={label}
                    onClick={() => (factor ? cameraRef.current?.zoomBy(factor) : cameraRef.current?.fit())}
                    className="rounded-none text-fg not-first:border-t not-first:border-border md:size-8"><Icon /></Button>
                ))}
              </div>
              <Sheet>
                <SheetTrigger asChild>
                  <Button variant="outline" size="icon-lg" aria-label="Legend" className="absolute right-3 top-3 md:hidden"><Info /></Button>
                </SheetTrigger>
                <SheetContent side="bottom" className="rounded-t-xl px-4 pb-[calc(16px+env(safe-area-inset-bottom))] pt-4">
                  <SheetTitle className="heading-16">Legend</SheetTitle>
                  <div><LegendContent items={legend} /></div>
                </SheetContent>
              </Sheet>
            </>
          )}
        </section>
        {drawn && !leaf && !hiddenAll && <GraphLegend items={legend} className="max-md:hidden" />}
        {phone && drawn && !leaf && !hiddenAll && !selected && (
          <Sheet open={trailOpen} onOpenChange={setTrailOpen}>
            <SheetTrigger asChild>
              <button type="button" className="flex h-14 shrink-0 items-center justify-between gap-2 rounded-t-xl border-t border-border bg-overlay pl-4 pr-2 text-left shadow-(--shadow-overlay)">
                <span className="truncate label-14 font-medium text-fg">Explored from {rootName}{" "}
                  <span className="font-normal text-fg-muted">· {plural(layout.expansions.length, "expansion")}</span></span>
                <span className="flex size-11 items-center justify-center"><ChevronUp aria-hidden className="size-5" /></span>
                <span className="sr-only">Open the trail</span>
              </button>
            </SheetTrigger>
            <SheetContent side="bottom" aria-describedby={undefined} className="rounded-t-xl px-2 pb-[calc(16px+env(safe-area-inset-bottom))] pt-3">
              <SheetTitle className="sr-only">Trail</SheetTitle>
              {trail}
            </SheetContent>
          </Sheet>
        )}
        <EvidenceShelf documents={documents} entityId={root.focus.id} marked={new Set(selectedLink?.link.records.map(record => record.doc_id))}
          onMark={setMarkedDoc} className="max-md:hidden" />
      </div>
      {inspector && (
        <aside aria-label="Evidence inspector"
          className="h-[300px] shrink-0 overflow-y-auto border-t border-border bg-background max-md:rounded-t-xl max-md:shadow-(--shadow-overlay) md:h-auto md:w-[380px] md:border-l md:border-t-0">
          {inspector}
        </aside>
      )}
    </div>
  );
}

/** "Explored from <root>": the root and each expansion in order, with the entities it added and Collapse. */
function Trail({ rootName, rootLabel, rootLinks, expansions, pending, onCollapse, onRetry }: {
  rootName: string;
  rootLabel: string;
  rootLinks: number;
  expansions: { index: number; added: string[]; node: ExploreNode }[];
  pending: { name: string; failed: boolean } | null;
  onCollapse: (index: number) => void;
  onRetry: () => void;
}) {
  const row = "grid h-8 grid-cols-[16px_minmax(0,1fr)_auto_28px] items-center gap-2 rounded-(--r-ctl) pl-2 max-md:h-11";
  return (
    <section aria-label={`Explored from ${rootName}`} className="rounded-(--r-panel) border border-border bg-surface px-2 pb-2 pt-3 max-md:border-0 max-md:bg-transparent">
      <h2 className="truncate px-2 label-12 font-semibold text-fg">Explored from {rootName}</h2>
      <p className="mb-1 border-b border-border px-2 pb-2 label-12 text-fg-muted">
        {expansions.length ? `${plural(expansions.length, "expansion")}. ` : ""}Select a node, then press +.
      </p>
      <ol className="max-h-[50vh] overflow-y-auto">
        <li className={cn(row, "grid-cols-[16px_minmax(0,1fr)_auto] pr-2")}>
          <KindShape label={rootLabel} bare />
          <span className="truncate label-13 text-fg">{rootName}</span>
          <span className="label-12 tabular-nums text-fg-muted">{plural(rootLinks, "link")}</span>
        </li>
        {expansions.map(({ index, added, node }) => (
          <li key={index} className={row}>
            <KindShape label={node.label} bare />
            <span className="truncate label-13 text-fg">{entityName(node)}</span>
            <span className="label-12 tabular-nums text-fg-muted">+{added.length}</span>
            <Button variant="ghost" size="icon-xs" aria-label={`Collapse ${entityName(node)}`} title="Collapse" onClick={() => onCollapse(index)}>
              <X />
            </Button>
          </li>
        ))}
        {pending && (
          <li className={cn(row, "grid-cols-[16px_minmax(0,1fr)_auto]", pending.failed && "h-auto py-1.5")}>
            {pending.failed ? <span aria-hidden /> : <Loader2 aria-hidden className="size-3.5 animate-spin text-fg-muted" />}
            <span role={pending.failed ? "alert" : "status"} className="label-13 text-fg-muted">
              {pending.failed ? `Links of ${pending.name} did not load.` : `Loading links of ${pending.name}`}
            </span>
            {pending.failed && <Button variant="outline" size="xs" onClick={onRetry}><RotateCw />Retry</Button>}
          </li>
        )}
      </ol>
    </section>
  );
}
