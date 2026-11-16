"use client";

import { startTransition, useCallback, useEffect, useMemo, useOptimistic, useRef, useState, useSyncExternalStore, type ReactNode } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { ArrowDown, ArrowUp, CircleX, LayoutGrid, Loader2, Maximize, Minus, Plus, Search, X } from "lucide-react";
import { KindShape } from "@/components/kind-shape";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetTitle } from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getGraphOverview } from "@/lib/api";
import { kindColor, nodeKind, nodeLabelText, NODE_LABELS, type Kind } from "@/lib/categories";
import { displayName } from "@/lib/graph-encoding";
import { defaultFilters, filterUrl, parseFilters, type GraphFilters } from "@/lib/graph-filters";
import { plural } from "@/lib/graph-inspector";
import { mainKinds, overviewLinks, readOverview, type KindCount, type OverviewMap, type OverviewResponse } from "@/lib/graph-overview";
import { GRAPH_VIEWS, graphNavigation, listedViews, parseGraphState, type GraphView } from "@/lib/graph-url-state";
import { cn, errMsg } from "@/lib/utils";
import type { ArrangeRequest } from "@/lib/overview.worker";
import { filterChips, GraphFilterChips, GraphFiltersSheet } from "./graph-filters";
import { GraphHeader } from "./graph-landing";
import { GraphLegend } from "./graph-legend";
import type { OverviewCamera } from "./overview-canvas";
import { Toggle } from "./path-view";
import { ViewSheet, ViewSwitcher } from "./view-switcher";

const OverviewCanvas = dynamic(() => import("./overview-canvas").then(module => module.OverviewCanvas), { ssr: false });

const PHONE = "(max-width: 767px)";
const subscribePhone = (change: () => void) => {
  const query = window.matchMedia(PHONE);
  query.addEventListener("change", change);
  return () => query.removeEventListener("change", change);
};
const TOP_ENTITIES = 10;
const count = (value: number) => value.toLocaleString("en-US");
const words = (label: string) => nodeLabelText(label).toLowerCase();
const nameOf = (map: OverviewMap, node: number) => displayName(map.nodes[node].name, map.nodes[node].id, map.nodes[node].label);
const groupName = (map: OverviewMap, group: number) => nameOf(map, map.groups[group].lead);
// On this page the switcher also shows Overview; other pages list it once the owner accepts it (#204).
const OVERVIEW = GRAPH_VIEWS.find(view => view.id === "overview")!;
const SWITCHER_VIEWS = [...listedViews(), { ...OVERVIEW, listed: true }];
const exploreListed = GRAPH_VIEWS.some(view => view.id === "explore" && view.listed);

/**
 * Arranged maps, per response and the filters that shape the groups. Keyed by the response object, not its version:
 * the version covers node ids and pairs only, so a new fetch with new names or counts must arrange again.
 */
const arranged = new WeakMap<OverviewResponse, Map<string, OverviewMap>>();
const filterKey = ({ families, support, minDocs }: GraphFilters) => JSON.stringify([families, support, minDocs]);

type Load = { attempt: number; response?: OverviewResponse; error?: string };
type Arrange = { key: string; map?: OverviewMap; error?: string };

/** The response, then its groups and layout from a Web Worker; each step reports its own failure. */
function useOverviewMap(filters: GraphFilters) {
  const [attempt, setAttempt] = useState(0);
  const [load, setLoad] = useState<Load | null>(null);
  const [arrange, setArrange] = useState<Arrange | null>(null);
  useEffect(() => {
    let active = true;
    getGraphOverview().then(
      response => { if (active) setLoad({ attempt, response }); },
      error => { if (active) setLoad({ attempt, error: errMsg(error, "Request failed.") }); });
    return () => { active = false; };
  }, [attempt]);
  const response = load?.attempt === attempt ? load.response : undefined;
  // Only the filters that shape the groups start a new arrangement; a selection or another filter does not.
  const shape = filterKey(filters);
  const key = response ? `${response.version}|${shape}|${attempt}` : null;
  useEffect(() => {
    if (!response || !key) return;
    if (!arranged.has(response)) arranged.set(response, new Map());
    const cached = arranged.get(response)!.get(shape);
    if (cached) {
      queueMicrotask(() => setArrange({ key, map: cached }));
      return;
    }
    const worker = new Worker(new URL("../../lib/overview.worker.ts", import.meta.url));
    const start = performance.now();
    worker.onmessage = ({ data }: MessageEvent<{ map?: OverviewMap; error?: string }>) => {
      if (data.map) {
        // The real-archive trial reads this measure: worker round trip, with the worker's own group and layout times.
        performance.measure("overview-arrange", { start, end: performance.now(), detail: { entities: data.map.nodes.length, ...data.map.timings } });
        arranged.get(response)?.set(shape, data.map);
      }
      setArrange({ key, ...data });
      worker.terminate();
    };
    worker.onerror = event => { setArrange({ key, error: event.message || "The worker stopped." }); worker.terminate(); };
    const [families, support, minDocs] = JSON.parse(shape);
    worker.postMessage({ response, filters: { families, support, minDocs } } satisfies ArrangeRequest);
    return () => worker.terminate();
  }, [response, key, shape]);
  const current = arrange?.key === key ? arrange : null;
  return {
    response, map: current?.map ?? null,
    error: load?.attempt === attempt && load.error ? { step: "load" as const, message: load.error } : current?.error ? { step: "arrange" as const, message: current.error } : null,
    retry: () => setAttempt(value => value + 1),
  };
}

/** A group's kinds as one bar of kind colors; the text beside it names them, so color is never the only signal. */
export function KindMix({ kinds, className }: { kinds: KindCount[]; className?: string }) {
  const total = kinds.reduce((sum, kind) => sum + kind.count, 0) || 1;
  return (
    <div aria-hidden className={cn("flex h-1.5 w-full gap-0.5 overflow-hidden rounded-full", className)}>
      {kinds.map(kind => <span key={kind.label} className="h-full rounded-full" style={{ width: `${(100 * kind.count) / total}%`, background: kindColor(nodeKind(kind.label).kind) }} />)}
    </div>
  );
}

const KIND_NAMES: Record<Kind, string> = { people: "People", org: "Organizations", place: "Places", money: "Money", agree: "Agreements",
  thing: "Things", health: "Health", time: "Time", docs: "Documents", unknown: "Other" };

/** Only what is drawn: kind colors at map zoom, shapes and line styles at detail zoom. Collapsed until opened, as on Focus. */
function OverviewLegend({ map, detail }: { map: OverviewMap; detail: boolean }) {
  const { kinds, labels, inferred, between } = useMemo(() => {
    const seen = new Set(map.nodes.filter((_, at) => !Number.isNaN(map.positions[at * 2])).map(node => node.label));
    return { kinds: (Object.keys(KIND_NAMES) as Kind[]).filter(kind => [...seen].some(label => nodeKind(label).kind === kind)),
      labels: [...NODE_LABELS.filter(label => seen.has(label)), ...([...seen].some(label => nodeKind(label).kind === "unknown") ? ["Other"] : [])],
      inferred: map.links.some(link => link.support === 2), between: map.links.some(link => map.groupOf[link.source] !== map.groupOf[link.target]) };
  }, [map]);
  const line = (dash: string | undefined, text: string, stroke = "var(--graph-edge)") => (
    <li className="flex items-center gap-2 label-12 text-fg-muted"><svg aria-hidden width="24" height="10"><line x1="1" y1="5" x2="23" y2="5" stroke={stroke} strokeWidth={detail ? 2 : 1} strokeDasharray={dash} /></svg>{text}</li>);
  return (
    <GraphLegend className="max-md:hidden">
      <ul aria-label="Kinds" className="flex flex-wrap gap-x-4 gap-y-1">
        {detail
          ? labels.map(label => <li key={label}><KindShape label={label} className="whitespace-nowrap label-12 text-fg-muted" /></li>)
          : kinds.map(kind => (
            <li key={kind} className="flex items-center gap-2 label-12 text-fg-muted">
              <span aria-hidden className="size-2 shrink-0 rounded-full" style={{ background: kindColor(kind) }} />{KIND_NAMES[kind]}
            </li>))}
      </ul>
      <ul aria-label="Links" className="flex flex-wrap gap-x-4 gap-y-1">
        {detail && line(undefined, "Supported")}
        {detail && inferred && line("6 4", "Inferred")}
        {!detail && between && line("2 3", "Strongest links between groups", "var(--border)")}
      </ul>
      <p className="label-12 text-fg-muted">
        {detail ? "Size: evidence documents. Zoom out for the map of groups." : "Dots show kind by color. Zoom in for shapes, sizes and names."}
      </p>
    </GraphLegend>
  );
}

/** "Map of the archive: …", with what the map leaves out. */
function caption(map: OverviewMap, response: OverviewResponse) {
  const drawn = map.groups.reduce((sum, group) => sum + group.members.length, 0) + map.small.reduce((sum, set) => sum + set.length, 0);
  const parts = [`Map of the archive: ${count(drawn)} ${drawn === 1 ? "entity" : "entities"} in ${plural(map.groups.length, "group")} and ${plural(map.small.length, "small group")}.`];
  if (response.truncated) parts.push(`The map shows the ${count(response.nodes.length)} most linked entities.`);
  const only = [response.unlinked && `${count(response.unlinked)} ${response.unlinked === 1 ? "entity links" : "entities link"} only to documents`,
    map.ownerOnly && `${count(map.ownerOnly)} only to the owner`].filter(Boolean);
  if (only.length) parts.push(`${only.join(" and ")}.`);
  if (map.hidden) parts.push(`Your filters hide the links of ${plural(map.hidden, "entity", "entities")}.`);
  if (response.owner !== null) parts.push("The owner is hidden.");
  return { drawn, text: parts.join(" "), name: parts[0] };
}

function GroupPanel({ map, group, onClose, onSelectNode, onOpen }: {
  map: OverviewMap; group: number; onClose?: () => void; onSelectNode: (node: number) => void; onOpen: (node: number, view: GraphView) => void;
}) {
  const { members, links, kinds, lead } = map.groups[group];
  const leadName = nameOf(map, lead);
  return (
    <div className="relative px-4 pb-8 pt-5 md:px-6 md:pt-6" role="region" aria-label="Inspector: group">
      <p className="flex items-center gap-2 label-13 text-fg-muted"><LayoutGrid aria-hidden className="size-4" strokeWidth={1.75} />Group</p>
      {onClose && <Button variant="ghost" size="icon-sm" aria-label="Clear selection" onClick={onClose} className="absolute right-2 top-2 md:right-4 md:top-4"><X /></Button>}
      <h2 className="mt-3 font-(family-name:--font-geist-serif-regular) text-[22px] leading-7 text-fg">{leadName}</h2>
      <p className="mt-2 copy-14 text-fg-muted">
        {count(members.length)} entities and {plural(links, "link")}, named after the entity with the most evidence documents.
      </p>
      <div className="mt-4 flex flex-wrap gap-2">
        <Button onClick={() => onOpen(lead, "focus")}>Open {leadName} in Focus</Button>
        {exploreListed && <Button variant="outline" onClick={() => onOpen(lead, "explore")}>Explore from {leadName}</Button>}
      </div>
      <h3 className="mt-6 mb-2 label-13 font-semibold text-fg">Kinds</h3>
      <KindMix kinds={kinds} />
      <ul aria-label="Kinds in this group" className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5">
        {kinds.map(kind => (
          <li key={kind.label} className="flex items-center gap-2 label-12 text-fg-muted">
            <KindShape label={kind.label} className="min-w-0 truncate label-12 text-fg-muted" /><b className="ml-auto font-normal tabular-nums text-fg">{count(kind.count)}</b>
          </li>))}
      </ul>
      <h3 className="mt-6 mb-2 label-13 font-semibold text-fg">Most documented</h3>
      <ol aria-label={`The ${Math.min(TOP_ENTITIES, members.length)} entities with the most evidence documents`}>
        {members.slice(0, TOP_ENTITIES).map(node => (
          <li key={node}>
            <button type="button" onClick={() => onSelectNode(node)}
              className="grid w-full grid-cols-[16px_minmax(0,1fr)_auto] items-center gap-x-2.5 rounded-md px-2 py-1.5 text-left hover:bg-raised">
              <KindShape label={map.nodes[node].label} bare />
              <span className="truncate label-13 text-fg">{nameOf(map, node)}</span>
              <span className="label-12 tabular-nums text-fg-muted">{plural(map.nodes[node].documents, "doc")}</span>
              <span className="col-start-2 label-12 text-fg-subtle">{nodeLabelText(map.nodes[node].label)}</span>
            </button>
          </li>))}
      </ol>
    </div>
  );
}

function EntityPanel({ map, node, onClose, onSelectGroup, onOpen }: {
  map: OverviewMap; node: number; onClose: () => void; onSelectGroup: (group: number) => void; onOpen: (node: number, view: GraphView) => void;
}) {
  const entity = map.nodes[node];
  const group = map.groupOf[node];
  return (
    <div className="relative px-4 pb-8 pt-5 md:px-6 md:pt-6" role="region" aria-label="Inspector: entity">
      <KindShape label={entity.label} className="label-13 text-fg-muted" />
      <Button variant="ghost" size="icon-sm" aria-label="Clear selection" onClick={onClose} className="absolute right-2 top-2 md:right-4 md:top-4"><X /></Button>
      <h2 className="mt-3 font-(family-name:--font-geist-serif-regular) text-[22px] leading-7 text-fg">{nameOf(map, node)}</h2>
      <p className="mt-2 copy-14 tabular-nums text-fg-muted">{plural(entity.documents, "evidence document")} · {plural(entity.links, "link")}</p>
      <p className="mt-1 copy-14 text-fg-muted">
        {group >= 0
          ? <>In the group <Button variant="link" className="h-auto p-0 copy-14" onClick={() => onSelectGroup(group)}>{groupName(map, group)}</Button>.</>
          : "In the small groups."}
      </p>
      <div className="mt-4 flex flex-wrap gap-2">
        <Button onClick={() => onOpen(node, "focus")}>Open in Focus</Button>
        {exploreListed && <Button variant="outline" onClick={() => onOpen(node, "explore")}>Explore from here</Button>}
      </div>
    </div>
  );
}

/** "Show as list": the groups as a table, sortable by size; each row opens its group in the inspector. */
function GroupTable({ map, selected, onSelectGroup }: { map: OverviewMap; selected: number | null; onSelectGroup: (group: number) => void }) {
  const [descending, setDescending] = useState(true);
  const rows = useMemo(() => {
    const order = map.groups.map((_, index) => index);
    return descending ? order : order.reverse();
  }, [map, descending]);
  return (
    <section aria-label="Groups" className="absolute inset-0 overflow-auto px-4 pb-8 pt-16 md:px-8">
      <Table aria-label="Groups of the archive" className="label-13">
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead scope="col" className="label-12">Group</TableHead>
            <TableHead scope="col" aria-sort={descending ? "descending" : "ascending"} className="text-right text-fg">
              <button type="button" onClick={() => setDescending(!descending)} className="inline-flex min-h-11 items-center gap-1 label-12 md:min-h-9">
                Entities{descending ? <ArrowDown aria-hidden className="size-3.5" /> : <ArrowUp aria-hidden className="size-3.5" />}
              </button>
            </TableHead>
            <TableHead scope="col" className="text-right label-12">Links</TableHead>
            <TableHead scope="col" className="label-12">Kinds</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map(index => {
            const group = map.groups[index];
            return (
              <TableRow key={group.id} data-state={index === selected ? "selected" : undefined} className="[&>td]:h-12 md:[&>td]:h-10 data-[state=selected]:[&>td]:text-on-mark data-[state=selected]:[&>td:first-child]:shadow-[inset_2px_0_0_var(--mark-line)]">
                <TableCell>
                  <button type="button" onClick={() => onSelectGroup(index)} aria-pressed={index === selected} className="inline-flex max-w-72 items-center gap-2 text-left hover:underline hover:underline-offset-4">
                    <KindShape label={map.nodes[group.lead].label} bare /><span className="truncate">{groupName(map, index)}</span>
                  </button>
                </TableCell>
                <TableCell className="text-right tabular-nums">{count(group.members.length)}</TableCell>
                <TableCell className="text-right tabular-nums text-fg-muted">{count(group.links)}</TableCell>
                <TableCell className="text-fg-muted">{group.kinds.map(kind => `${nodeLabelText(kind.label)} ${count(kind.count)}`).join(" · ")}</TableCell>
              </TableRow>);
          })}
        </TableBody>
      </Table>
      {map.small.length > 0 && <p className="mt-4 label-13 text-fg-muted">And {plural(map.small.length, "small group")} of fewer than 4 entities.</p>}
    </section>
  );
}

/** Phone: no canvas. The groups as 56 px rows; a row opens a sheet with its top entities, and a row there opens Focus. */
function PhoneGroups({ map, intro, selected, onSelectGroup, onOpen }: {
  map: OverviewMap; intro: string; selected: number | null; onSelectGroup: (group: number | null) => void; onOpen: (node: number, view: GraphView) => void;
}) {
  const group = selected === null ? null : map.groups[selected];
  return (
    <div className="min-h-0 flex-1 overflow-y-auto">
      <p className="border-b px-4 py-3 copy-14 text-fg-muted">{intro}</p>
      <ul aria-label="Groups">
        {map.groups.map((item, index) => (
          <li key={item.id} className="border-b border-border">
            <button type="button" onClick={() => onSelectGroup(index)} className="grid min-h-14 w-full gap-1.5 px-4 py-2.5 text-left">
              <span className="flex items-baseline justify-between gap-3">
                <span className="truncate text-[15px] leading-5 text-fg">{groupName(map, index)}</span>
                <span className="shrink-0 label-12 tabular-nums text-fg-muted">{count(item.members.length)} entities</span>
              </span>
              <KindMix kinds={item.kinds} />
              <span className="label-12 text-fg-muted">Mostly {mainKinds(item.kinds, words)}</span>
            </button>
          </li>))}
      </ul>
      {map.small.length > 0 && <p className="px-4 py-4 label-13 text-fg-muted">And {plural(map.small.length, "small group")} of fewer than 4 entities.</p>}
      <Sheet open={group !== null} onOpenChange={open => { if (!open) onSelectGroup(null); }}>
        <SheetContent side="bottom" className="max-h-[85vh] gap-0 overflow-y-auto rounded-t-xl px-4 pb-[calc(16px+env(safe-area-inset-bottom))] pt-4">
          {group && selected !== null && <>
            <SheetTitle className="font-(family-name:--font-geist-serif-regular) text-lg font-normal">{groupName(map, selected)}</SheetTitle>
            <SheetDescription className="mt-1">{count(group.members.length)} entities and {plural(group.links, "link")}. Open one in Focus.</SheetDescription>
            <ul aria-label="Most documented" className="mt-3 border-t border-border">
              {group.members.slice(0, TOP_ENTITIES).map(node => (
                <li key={node} className="border-b border-border">
                  <button type="button" onClick={() => onOpen(node, "focus")} className="grid min-h-14 w-full grid-cols-[16px_minmax(0,1fr)_auto] items-center gap-x-3 py-2 text-left">
                    <KindShape label={map.nodes[node].label} bare />
                    <span className="grid min-w-0"><span className="truncate text-[15px] leading-5 text-fg">{nameOf(map, node)}</span>
                      <span className="label-12 text-fg-muted">{nodeLabelText(map.nodes[node].label)}</span></span>
                    <span className="label-12 tabular-nums text-fg-muted">{plural(map.nodes[node].documents, "doc")}</span>
                  </button>
                </li>))}
            </ul>
          </>}
        </SheetContent>
      </Sheet>
    </div>
  );
}

/** A centered message for the states in which the map has nothing to draw. */
function Message({ children }: { children: ReactNode }) {
  return <div className="flex min-h-0 flex-1 flex-col items-center justify-center gap-4 p-8 text-center">{children}</div>;
}

/** `/graph?view=overview`: the whole archive in groups of entities that link to each other. Reached by URL until #204. */
export function OverviewPage() {
  const router = useRouter();
  const params = useSearchParams();
  const state = parseGraphState(new URLSearchParams(params));
  const phone = useSyncExternalStore(subscribePhone, () => window.matchMedia(PHONE).matches, () => false);
  const parsed = useMemo(() => parseFilters(new URLSearchParams(params)), [params]);
  const [filters, setOptimisticFilters] = useOptimistic(parsed);
  const changeFilters = (next: GraphFilters) => startTransition(() => {
    setOptimisticFilters(next);
    router.replace(filterUrl(new URLSearchParams(params), next), { scroll: false });
  });
  const { response, map, error, retry } = useOverviewMap(filters);
  const [list, setList] = useState(false);
  const [detail, setDetail] = useState(false);
  const camera = useRef<OverviewCamera | null>(null);
  const onReady = useCallback((value: OverviewCamera | null) => { camera.current = value; }, []);

  const navigate = useCallback((change: Parameters<typeof graphNavigation>[1]) => {
    const { href, method } = graphNavigation(new URLSearchParams(params), change);
    router[method](href, { scroll: false });
  }, [params, router]);
  // A selection the map does not hold (an old group key, an entity not drawn) is ignored.
  const index = useMemo(() => map ? new Map(map.nodes.map((node, at) => [node.id, at])) : null, [map]);
  const selectedGroup = map && state.selection?.startsWith("grp:") ? map.groups.findIndex(group => `grp:${group.id}` === state.selection) : -1;
  const selectedNodeAt = map && index && state.selection && !state.selection.startsWith("grp:") ? index.get(state.selection) ?? -1 : -1;
  const selectedNode = map && selectedNodeAt >= 0 && !Number.isNaN(map.positions[selectedNodeAt * 2]) ? selectedNodeAt : -1;
  const selectGroup = useCallback((group: number | null) => {
    if (!map) return;
    navigate({ selection: group === null ? null : `grp:${map.groups[group].id}` });
  }, [map, navigate]);
  const selectNode = useCallback((node: number) => { if (map) navigate({ selection: map.nodes[node].id }); }, [map, navigate]);
  const open = useCallback((node: number, view: GraphView) => { if (map) navigate({ focus: map.nodes[node].id, view }); }, [map, navigate]);

  const focusName = state.focus ? (index?.has(state.focus) ? nameOf(map!, index.get(state.focus)!) : state.focus) : null;
  const switcher = { views: SWITCHER_VIEWS, current: "overview" as const, focusName, onSwitch: (view: GraphView) => navigate({ view }) };
  // Only after the worker arranged the response, so a response it rejects shows the error instead of throwing here.
  const pairs = useMemo(() => response && map ? readOverview(response).links : [], [response, map]);
  const total = response ? overviewLinks(pairs, response.owner, defaultFilters()).length : 0;
  const filterCount = useCallback((next: GraphFilters) => overviewLinks(pairs, response?.owner ?? null, next).length, [pairs, response]);
  const filterSheet = response && <GraphFiltersSheet filters={filters} view="overview" onChange={changeFilters} count={filterCount} />;
  const text = map && response ? caption(map, response) : null;
  const filtering = filterChips(filters).length > 0;

  let body;
  if (error) body = (
    <Message>
      <Alert variant="destructive" className="max-w-md text-left">
        <CircleX aria-hidden strokeWidth={1.75} />
        <AlertTitle>{error.step === "load" ? "The archive map did not load." : "The archive map could not be arranged."}</AlertTitle>
        <AlertDescription>
          <p>{error.message}</p>
          <Button size="sm" variant="outline" className="mt-1" onClick={retry}>Retry</Button>
        </AlertDescription>
      </Alert>
    </Message>);
  else if (!response || !map) body = (
    <Message>
      <p role="status" className="flex items-center gap-2 copy-14 text-fg-muted">
        <Loader2 className="size-4 animate-spin" />
        {response ? `Arranging ${count(response.nodes.length - (response.owner === null ? 0 : 1))} entities` : "Loading the archive map"}
      </p>
    </Message>);
  else if (!text!.drawn && filtering && total) body = (
    <Message>
      <p className="copy-14 text-fg-muted">Your filters hide every link on the map.</p>
      <Button variant="outline" onClick={() => changeFilters(defaultFilters())}>Reset filters</Button>
    </Message>);
  else if (!text!.drawn) body = (
    <Message>
      <p className="max-w-sm text-balance font-(family-name:--font-geist-serif-regular) text-xl text-fg">
        {response.nodes.length ? "Every link between entities runs through the owner." : "No links between entities yet."}
      </p>
      <p className="max-w-sm text-balance copy-14 text-fg-muted">
        {response.nodes.length
          ? "The map leaves out the owner, so it has nothing to draw. Open the owner in Focus to see those links."
          : `Run a sync, then come back.${response.unlinked ? ` ${count(response.unlinked)} ${response.unlinked === 1 ? "entity links" : "entities link"} only to documents.` : ""}`}
      </p>
      <Button variant="outline" asChild><Link href="/graph"><Search />Search entities</Link></Button>
    </Message>);
  else if (phone) body = (
    <PhoneGroups map={map} selected={selectedGroup >= 0 ? selectedGroup : null} onSelectGroup={selectGroup} onOpen={open}
      intro={map.groups.length
        ? `The archive in ${plural(map.groups.length, "group")} and ${plural(map.small.length, "small group")}, largest first. The bar shows the kinds in each group.`
        : `No group has 4 or more entities yet. The archive has ${plural(map.small.length, "small group")} of 2 or 3 entities.`} />);
  else {
    const inspector = selectedGroup >= 0
      ? <GroupPanel map={map} group={selectedGroup} onClose={() => selectGroup(null)} onOpen={open} onSelectNode={node => { selectNode(node); camera.current?.showNode(node); }} />
      : selectedNode >= 0
        ? <EntityPanel map={map} node={selectedNode} onClose={() => navigate({ selection: null })} onOpen={open}
            onSelectGroup={group => { selectGroup(group); camera.current?.showGroup(group); }} />
        : null;
    body = (
      <div className="flex min-h-0 flex-1">
        <div className="flex min-h-0 min-w-0 flex-1 flex-col">
          <section aria-label="Graph workspace" className="relative min-h-0 min-w-0 flex-1">
            {list
              ? <GroupTable map={map} selected={selectedGroup >= 0 ? selectedGroup : null} onSelectGroup={selectGroup} />
              : <OverviewCanvas map={map} name={text!.name} selectedGroup={selectedGroup >= 0 ? selectedGroup : null} selectedNode={selectedNode >= 0 ? selectedNode : null}
                  onSelectGroup={group => (group === null ? navigate({ selection: null }) : selectGroup(group))} onSelectNode={selectNode} onReady={onReady}
                  detail={detail} onDetail={setDetail} />}
            <div className="absolute right-4 top-3 rounded-md border border-border-strong bg-surface pl-2.5 pr-1">
              <Toggle on={list} label="Show as list" onClick={() => setList(!list)} />
            </div>
            {!list && (
              <div role="group" aria-label="Zoom" className="absolute bottom-4 right-4 flex flex-col overflow-hidden rounded-(--r-ctl) border border-border-strong bg-surface">
                {([["Zoom in", Plus, 1.5], ["Zoom out", Minus, 1 / 1.5], ["Fit view", Maximize, 0]] as const).map(([label, Icon, factor]) => (
                  <Button key={label} variant="ghost" size="icon-lg" aria-label={label} title={label}
                    onClick={() => (factor ? camera.current?.zoomBy(factor) : camera.current?.fit())}
                    className="rounded-none text-fg not-first:border-t not-first:border-border md:size-8"><Icon /></Button>
                ))}
              </div>
            )}
          </section>
          {!list && <OverviewLegend map={map} detail={detail} />}
        </div>
        {inspector && <aside aria-label="Evidence inspector" className="w-[380px] shrink-0 overflow-y-auto border-l border-border bg-background">{inspector}</aside>}
      </div>);
  }

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <GraphHeader controls={<><ViewSwitcher {...switcher} />{filterSheet}</>}>
        {text && <p role="status" className="mt-1 max-w-3xl text-sm text-muted-foreground max-md:sr-only">{text.text}</p>}
      </GraphHeader>
      <div className="flex h-14 shrink-0 items-center justify-between border-b px-4 md:hidden"><ViewSheet {...switcher} />{filterSheet}</div>
      <GraphFilterChips filters={filters} onChange={changeFilters} />
      {body}
    </div>
  );
}
