"use client";

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import dynamic from "next/dynamic";
import { useRouter, useSearchParams } from "next/navigation";
import { Info, Loader2, Maximize, Minus, Plus, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Sheet, SheetContent, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { cn, errMsg } from "@/lib/utils";
import { ApiError, getGraphNodeSummary, getGraphSubgraph } from "@/lib/api";
import { graphNavigation, listedViews, parseGraphState, parseLinkKey, shownView } from "@/lib/graph-url-state";
import type { ExplorerGraph, ExplorerLink, ExplorerNode } from "@/lib/graph-data";
import {
  crossCap,
  crossLinks,
  drawableLinks,
  familyCap,
  neighborhoodLayout,
  PHONE_PILL_RADIUS,
  PILL_RADIUS,
  showMore,
  VIEW_LIMIT,
  type NodeSummary,
  type Subgraph,
  type SubgraphLink,
  type SummaryLink,
} from "@/lib/graph-layout";
import { canvasName, documentsByDate, legendItems, linksByFamily, linksOfDocument, plural } from "@/lib/graph-inspector";
import type { Camera } from "./force-graph-client";
import { EvidenceShelf } from "./evidence-shelf";
import { FocusInspector, GroupInspector, LinkInspector, NeighborInspector } from "./entity-inspector";
import { GraphLegend, LegendContent } from "./graph-legend";
import { rememberRecent } from "./graph-landing";
import { LinkTable } from "./link-table";
import { PathView } from "./path-view";
import { ViewSheet, ViewSwitcher } from "./view-switcher";

const ForceGraphClient = dynamic(
  () => import("./force-graph-client").then((module) => module.ForceGraphClient),
  { ssr: false },
);

const PHONE = "(max-width: 767px)";
// The switcher appears once a second view ships.
const VIEWS = listedViews();
const subscribePhone = (change: () => void) => {
  const query = window.matchMedia(PHONE);
  query.addEventListener("change", change);
  return () => query.removeEventListener("change", change);
};

/** `/graph?focus=<id>`: the focus entity and its capped neighbors in fixed radial sectors. */
export function NeighborhoodView({ focusId }: { focusId: string }) {
  const router = useRouter();
  const params = useSearchParams();
  const urlState = parseGraphState(new URLSearchParams(params));
  const [summary, setSummary] = useState<NodeSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [missing, setMissing] = useState(false);
  const [query, setQuery] = useState("");
  const [attempt, setAttempt] = useState(0);
  const [extra, setExtra] = useState<Record<string, number>>({});
  const [atLimit, setAtLimit] = useState(false);
  const isTable = shownView(urlState.view) === "table";
  const isPath = shownView(urlState.view) === "path";
  const selectedLinkId = urlState.selection && parseLinkKey(urlState.selection) ? urlState.selection : null;
  const selectedNodeId = selectedLinkId ? null : urlState.selection;
  const [keyboardNodeId, setKeyboardNodeId] = useState<string | null>(null);
  const [markedDoc, setMarkedDoc] = useState<number | null>(null);
  const phone = useSyncExternalStore(subscribePhone, () => window.matchMedia(PHONE).matches, () => false);
  const cameraRef = useRef<Camera | null>(null);
  const [expandedSheet, setExpandedSheet] = useState<string | null>(null);
  // The links between neighbors, for the drawn neighbor ids they were requested with.
  const [cross, setCross] = useState<{ ids: string; data: Subgraph | null } | null>(null);
  const [crossAttempt, setCrossAttempt] = useState(0);
  const [crossSlow, setCrossSlow] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    getGraphNodeSummary(focusId)
      .then((data) => {
        if (!active) return;
        setSummary(data);
        const { id, name, label, evidence_doc_count } = data.focus;
        rememberRecent({ id, name, label, documents: evidence_doc_count });
      })
      .catch((error) => {
        if (!active) return;
        if (error instanceof ApiError && error.status === 404) setMissing(true);
        else setError(errMsg(error, "Request failed."));
      });
    return () => { active = false; };
  }, [focusId, attempt]);

  const cap = summary ? familyCap(summary.focus.is_hub, phone) : 0;
  // The phone View button and inspector peek leave a shorter canvas; give its pills more room.
  const pillRadius = phone ? PHONE_PILL_RADIUS + (urlState.selection ? 100 : 0) : PILL_RADIUS;
  const layout = useMemo(() => summary && neighborhoodLayout(summary, cap, extra, pillRadius),
    [summary, cap, extra, pillRadius]);
  // The route needs 2 to 150 ids; pills and their members are not sent. "+N more" requests again.
  const crossIds = useMemo(() => layout?.nodes.filter(node => node.kind === "neighbor").map(node => node.id).sort().join(",") ?? "", [layout]);
  const wantCross = !isTable && !isPath && urlState.cross && crossIds.includes(",");
  useEffect(() => {
    if (!wantCross) return;
    let active = true;
    const slow = setTimeout(() => setCrossSlow(crossIds), 1000);
    getGraphSubgraph(crossIds.split(","))
      .then(data => { if (active) setCross({ ids: crossIds, data }); }, () => { if (active) setCross({ ids: crossIds, data: null }); })
      .finally(() => clearTimeout(slow));
    return () => { active = false; clearTimeout(slow); };
  }, [wantCross, crossIds, crossAttempt]);
  const crossLoading = wantCross && cross?.ids !== crossIds;
  const crossFailed = wantCross && !crossLoading && !cross?.data;
  // Until a new request returns, the last response still draws its links among the neighbors that remain.
  const crossData = wantCross ? cross?.data : null;
  const shownCross = useMemo(() => layout && crossData ? crossLinks(layout, crossData, crossCap(phone)) : null, [layout, crossData, phone]);
  const { graph, positions } = useMemo(() => {
    // The canvas sizes and rings nodes from these fields of the summary.
    const fields = new Map<string, Record<string, unknown>>(summary ? [
      ...summary.links.map(({ neighbor }) => [neighbor.id, neighbor] as const),
      [summary.focus.id, summary.focus] as const,
    ] : []);
    const graph: ExplorerGraph = {
      nodes: (layout?.nodes ?? []).map(({ id, name, label }) => {
        const { evidence_doc_count, link_count, is_hub, is_owner } = fields.get(id) ?? {};
        return { id, name, label, props: { evidence_doc_count, link_count, is_hub, is_owner } };
      }),
      // Array order is drawing order: links between neighbors go under the focus links.
      links: [...(shownCross?.links ?? []).map(({ key, source, target, type, link, curvature }) => ({
        id: key, source, target, type,
        props: { implied: link.support_state === "inferred", source_doc_ids: link.records.map((record) => record.doc_id), curvature },
      })), ...(layout?.links ?? []).map(({ key, source, target, type, link }) => ({
        id: key, source, target, type,
        props: link
          ? { implied: link.support_state === "inferred", source_doc_ids: link.records.map((record) => record.doc_id) }
          : { aggregate: true },
      }))],
      discarded: 0,
    };
    return { graph, positions: new Map(layout?.nodes.map(({ id, x, y }) => [id, { x, y }])) };
  }, [layout, summary, shownCross]);

  const navigate = useCallback((change: Parameters<typeof graphNavigation>[1]) => {
    const { href, method } = graphNavigation(new URLSearchParams(params), change);
    router[method](href, { scroll: false });
  }, [params, router]);
  /** "+N more": add up to 25 neighbors of the family to its sector. */
  const addMore = useCallback((family: string) => {
    if (!summary) return;
    const next = showMore(summary, cap, extra, family);
    if (next !== extra) setExtra(next);
    else if (layout && layout.entities >= VIEW_LIMIT) setAtLimit(true);
  }, [summary, cap, extra, layout]);
  // A pill click selects it and lists its members; Shift+click adds them, as "+N more" does.
  const selectNode = useCallback((node: ExplorerNode, event?: MouseEvent) => {
    if (node.label === "Aggregate" && event?.shiftKey) return addMore(node.id.slice("pill:".length));
    navigate({ selection: node.id });
    setKeyboardNodeId(null);
  }, [addMore, navigate]);
  const selectLinkKey = useCallback((key: string) => {
    if (parseLinkKey(key)) navigate({ selection: key });
    setKeyboardNodeId(null);
  }, [navigate]);
  const selectLink = useCallback((link: ExplorerLink) => {
    // A pill's dashed link stands for the pill.
    if (link.props.aggregate === true) navigate({ selection: link.target });
    else selectLinkKey(link.id);
  }, [navigate, selectLinkKey]);
  const openFocus = useCallback((id: string) => navigate({ focus: id }), [navigate]);
  const clearSelection = useCallback(() => {
    navigate({ selection: null });
    setKeyboardNodeId(null);
    setExpandedSheet(null);
  }, [navigate, setExpandedSheet]);
  const onReady = useCallback((camera: Camera | null) => { cameraRef.current = camera; }, []);

  const linkTotal = layout?.sectors.reduce((sum, sector) => sum + sector.total, 0) ?? 0;
  const documents = useMemo(() => (summary ? documentsByDate(summary) : []), [summary]);
  const markedLinkIds = useMemo(() => (summary && markedDoc !== null ? linksOfDocument(summary, markedDoc) : undefined), [summary, markedDoc]);
  const legend = useMemo(() => layout && legendItems({ nodes: layout.nodes, links: [...layout.links, ...(shownCross?.links ?? [])] }), [layout, shownCross]);
  const leaf = summary !== null && drawableLinks(summary).length === 0;
  /** A link between neighbors as a link of `from`, so the inspector rows and link summary can show it. */
  const asLink = useCallback(({ source, target, ...link }: SubgraphLink, from: string): SummaryLink => {
    const other = source === from ? target : source;
    return { ...link, direction: source === from ? "out" : "in", neighbor: summary!.links.find(item => item.neighbor.id === other)!.neighbor };
  }, [summary]);
  const selectedCross = shownCross?.links.find(link => link.key === selectedLinkId);
  const selectedLink = summary?.links.find(link => link.key === selectedLinkId)
    ?? (selectedCross && asLink(selectedCross.link, selectedCross.source));
  const markedDocs = useMemo(() => new Set(selectedLink?.records.map(record => record.doc_id)), [selectedLink]);
  const pill = !isTable && selectedNodeId?.startsWith("pill:") ? layout?.sectors.find(sector => `pill:${sector.family}` === selectedNodeId) : undefined;
  const adds = useMemo(() => {
    if (!summary || !pill) return 0;
    const next = showMore(summary, cap, extra, pill.family);
    return next === extra ? 0 : neighborhoodLayout(summary, cap, next).sectors.find(sector => sector.family === pill.family)!.drawn - pill.drawn;
  }, [summary, cap, extra, pill]);
  const selected = Boolean(selectedLink || pill || (selectedNodeId && (positions.has(selectedNodeId) || (isTable && summary?.links.some(link => link.neighbor.id === selectedNodeId)))));
  const rowEvents = { onSelectLink: selectLinkKey, onFocusRow: setKeyboardNodeId };
  const crossCount = !urlState.cross ? null : !wantCross ? 0 : shownCross && !crossLoading ? shownCross.links.length : null;
  let crossCaption = null;
  // On phone the plain count and "none" are for screen readers only, so the canvas keeps its height.
  let crossQuiet = false;
  if (crossLoading) crossCaption = crossSlow === crossIds ? "Loading links between neighbors" : null;
  else if (crossFailed) crossCaption = (
    <>Links between neighbors did not load.{" "}
      <Button variant="link" size="xs" className="h-auto p-0 text-sm" onClick={() => { setCross(null); setCrossAttempt(value => value + 1); }}>Retry</Button>
    </>);
  else if (crossCount === 0 && !leaf) [crossCaption, crossQuiet] = ["No links between these neighbors.", true];
  else if (shownCross) [crossCaption, crossQuiet] = shownCross.total > shownCross.links.length
    ? [`Showing the ${shownCross.links.length} strongest of ${shownCross.total} links between neighbors.`, false]
    : [`${plural(shownCross.total, "link")} between neighbors.`, true];
  const switcher = { views: VIEWS, current: shownView(urlState.view), focusName: summary?.focus.name ?? null, onSwitch: (view: typeof urlState.view) => navigate({ view }) };

  const sheetKey = selectedLinkId ?? selectedNodeId ?? "focus";
  // On phone the inspector is a bottom sheet: a 300 px peek, or expanded over all but 120 px of the canvas.
  const expanded = phone && !isTable && expandedSheet === sheetKey;
  let inspector = null;
  if (summary && layout && !isPath) {
    if (selectedCross && selectedLink) inspector = (
      <LinkInspector summary={{ ...summary, documents: { ...cross?.data?.documents, ...summary.documents } }} link={selectedLink}
        end={summary.links.find(link => link.neighbor.id === selectedCross.source)!.neighbor} onClose={clearSelection} />);
    else if (selectedLink) inspector = <LinkInspector summary={summary} link={selectedLink} onClose={clearSelection} />;
    else if (pill) {
      const drawn = new Set(layout.links.map(link => link.key));
      inspector = (
        <GroupInspector sector={pill} adds={adds} onAdd={() => addMore(pill.family)} onClose={clearSelection} atLimit={layout.entities >= VIEW_LIMIT} onOpenTable={() => navigate({ view: "table" })} {...rowEvents}
          links={linksByFamily(summary).find(group => group.family === pill.family)!.links.filter(link => !drawn.has(link.key))} />);
    }
    else if (selectedNodeId && selectedNodeId !== summary.focus.id && ((isTable && summary.links.some(link => link.neighbor.id === selectedNodeId)) || (!isTable && positions.has(selectedNodeId))))
      inspector = <NeighborInspector summary={summary} neighborId={selectedNodeId} onClose={clearSelection} onFocusEntity={openFocus} {...rowEvents}
        crossLinks={shownCross?.links.filter(link => link.source === selectedNodeId || link.target === selectedNodeId).map(link => asLink(link.link, selectedNodeId))} />;
    else if (!isTable) inspector = <FocusInspector onOpenTable={() => navigate({ view: "table" })} summary={summary} onClose={selected ? clearSelection : undefined} {...rowEvents} />;
  }

  if (missing) return (
    <div className="flex h-full flex-col bg-background px-4 py-6 md:px-12 md:py-10">
      <h1 className="page-title">Graph</h1>
      <div className="mx-auto flex w-full max-w-lg flex-1 flex-col justify-center gap-6 pb-16">
        <p role="status" className="text-base text-foreground">
          This entity no longer exists. It may have been merged.
        </p>
        <form className="space-y-2" onSubmit={(event) => {
          event.preventDefault();
          if (query.trim()) router.push(`/graph?${new URLSearchParams({ q: query.trim() })}`);
        }}>
          <label htmlFor="missing-entity-search" className="text-sm font-medium">Find an entity</label>
          <div className="flex gap-2">
            <Input id="missing-entity-search" placeholder="Search by name or alias…" value={query}
              onChange={(event) => setQuery(event.target.value)} className="h-11 text-base md:h-9 md:text-sm" />
            <Button type="submit" className="h-11 md:h-9" disabled={!query.trim()}>
              <Search className="size-4" />Search
            </Button>
          </div>
        </form>
      </div>
    </div>
  );

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3 md:px-5">
        <div>
          <h1 className="page-title">Graph</h1>
          {!isTable && !isPath && summary && layout && (
            <p className="mt-1 text-sm text-muted-foreground">
              <span>
                {summary.focus.name}: {linkTotal} links in {layout.sectors.length} families.
                {summary.focus.is_hub && ` Large hub: the top ${cap} of each family are shown.`}
              </span>
              {" "}<span role="status" className={crossQuiet ? "max-md:sr-only" : undefined}>{crossCaption}</span>
            </p>
          )}
        </div>
        {VIEWS.length > 1 && <ViewSwitcher {...switcher} className="max-md:hidden" />}
      </header>
      {VIEWS.length > 1 && (
        <div className="flex h-14 shrink-0 items-center border-b px-4 md:hidden"><ViewSheet {...switcher} /></div>
      )}
      {error && (
        <div role="alert" className="flex items-center justify-between gap-2 border-b border-destructive/30 bg-destructive/10 px-4 py-2 text-sm">
          <span>Could not load the neighborhood: {error}</span>
          <Button size="sm" variant="outline" onClick={() => { setError(null); setAttempt((value) => value + 1); }}>
            Retry
          </Button>
        </div>
      )}
      {atLimit && !isTable && !isPath && (
        <p role="status" className="border-b px-4 py-2 text-sm">
          The canvas shows at most {VIEW_LIMIT} entities. No more neighbors can be added to this view.
        </p>
      )}
      {isPath && <PathView focus={summary?.focus ?? null} phone={phone} />}
      {!isPath && <div className="relative flex min-h-0 flex-1 flex-col md:flex-row">
        <div className={`flex min-h-0 min-w-0 flex-1 flex-col ${expanded ? "max-md:h-[120px] max-md:flex-none" : ""}`}>
          {isTable && summary && layout ? (
            <LinkTable summary={summary} layout={layout} selection={urlState.selection} onSelectLink={selectLinkKey} />
          ) : <>
          <section className="relative min-h-0 flex-1" aria-label="Graph workspace">
            {leaf ? (
              <div className="flex h-full items-center justify-center p-8">
                <p className="max-w-sm text-balance text-center copy-14 text-fg-muted">
                  No direct links extracted. This entity appears in {plural(documents.length, "document")}.
                </p>
              </div>
            ) : layout ? (
              <ForceGraphClient
                graph={graph}
                positions={positions}
                focusId={focusId}
                keyboardNodeId={keyboardNodeId}
                markedLinkIds={markedLinkIds}
                showLabels
                selectedNodeId={selectedNodeId}
                selectedLinkId={selectedLinkId}
                onSelectNode={selectNode}
                onSelectLink={selectLink}
                onClear={clearSelection}
                onReady={onReady}
                name={canvasName(summary!, layout, crossCount)}
                phone={phone}
              />
            ) : !error && (
              <div role="status" className="flex h-full items-center justify-center gap-2 text-sm">
                <Loader2 className="h-5 w-5 animate-spin" />
                Loading neighbors…
              </div>
            )}
            {layout && !leaf && !expanded && (
              <div role="group" aria-label="Zoom" className="absolute bottom-3 right-3 flex flex-col overflow-hidden rounded-(--r-ctl) border border-border-strong bg-surface md:bottom-4 md:right-4">
                {([["Zoom in", Plus, 1.5], ["Zoom out", Minus, 1 / 1.5], ["Fit view", Maximize, 0]] as const).map(([label, Icon, factor]) => (
                  <Button key={label} variant="ghost" size="icon-lg" aria-label={label} title={label}
                    onClick={() => (factor ? cameraRef.current?.zoomBy(factor) : cameraRef.current?.fit())}
                    className="rounded-none text-fg not-first:border-t not-first:border-border md:size-8"><Icon /></Button>
                ))}
              </div>
            )}
            {legend && !leaf && !expanded && (
              <div className="absolute right-3 top-3 flex gap-2 md:right-4">
                {/* A switch: on phone only its track shows, at the 44 px touch size. */}
                <button type="button" aria-pressed={urlState.cross} onClick={() => { if (urlState.cross) setCross(null); navigate({ cross: !urlState.cross }); }}
                  className="inline-flex h-8 items-center gap-2 rounded-md border border-border-strong bg-surface pl-2.5 pr-3 label-12 font-medium text-fg hover:bg-raised max-md:size-11 max-md:justify-center max-md:p-0">
                  <span aria-hidden className={cn("relative h-[18px] w-8 shrink-0 rounded-full border", urlState.cross ? "border-fg bg-fg" : "border-border-strong")}>
                    <span className={cn("absolute top-[2px] size-3 rounded-full", urlState.cross ? "left-4 bg-bg" : "left-[2px] bg-fg-muted")} />
                  </span>
                  <span className="max-md:sr-only">Links between neighbors</span>
                </button>
                <Sheet>
                  <SheetTrigger asChild>
                    <Button variant="outline" size="icon-lg" aria-label="Legend" className="md:hidden"><Info /></Button>
                  </SheetTrigger>
                  <SheetContent side="bottom" className="rounded-t-xl px-4 pb-[calc(16px+env(safe-area-inset-bottom))] pt-4">
                    <SheetTitle className="heading-16">Legend</SheetTitle>
                    <div><LegendContent items={legend} /></div>
                  </SheetContent>
                </Sheet>
              </div>
            )}
          </section>
          {legend && !leaf && <GraphLegend items={legend} className="max-md:hidden" />}
          {summary && (
            <EvidenceShelf documents={documents} entityId={summary.focus.id} marked={markedDocs} onMark={setMarkedDoc}
              className={selected ? "max-md:hidden" : undefined} />
          )}
          </>}
        </div>
        {inspector && (
          <aside
            key={sheetKey}
            aria-label="Evidence inspector"
            className={`shrink-0 overflow-y-auto border-t border-border bg-background max-md:rounded-t-xl max-md:shadow-(--shadow-overlay) md:h-auto md:w-[380px] md:border-l md:border-t-0 ${expanded ? "max-md:min-h-0 max-md:flex-1" : "h-[300px]"} ${selected || leaf ? "" : "max-md:hidden"}`}
          >
            {phone && selected && !isTable && (
              <button type="button" aria-expanded={expanded} onClick={() => setExpandedSheet(expanded ? null : sheetKey)}
                className="sticky top-0 z-10 flex h-8 w-full items-center justify-center bg-background">
                <span aria-hidden className="h-1 w-9 rounded-full bg-border-strong" />
                <span className="sr-only">{expanded ? "Collapse inspector" : "Expand inspector"}</span>
              </button>
            )}
            {inspector}
          </aside>
        )}
      </div>}
    </div>
  );
}
