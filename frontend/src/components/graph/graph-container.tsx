"use client";

import {
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import Link from "next/link";
import {
  ArrowRight,
  FileText,
  Focus,
  List,
  Loader2,
  Network,
  RefreshCw,
  Search,
  X,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { NodeDetailPanel } from "@/components/node-detail-panel";
import { getGraphInitial, getGraphNeighbors, graphSearch } from "@/lib/api";
import {
  emptyGraph,
  mergeGraph,
  nodeFromPayload,
  sourceDocumentIds,
  type ExplorerGraph,
  type ExplorerNode,
  type ExplorerLink,
} from "@/lib/graph-data";
import { getNodeColor } from "./graph-legend";
import type { GraphHandle } from "./force-graph-client";

const ForceGraphClient = dynamic(
  () =>
    import("./force-graph-client").then((module) => module.ForceGraphClient),
  { ssr: false },
);

function message(error: unknown) {
  return error instanceof Error ? error.message : "Request failed.";
}

function GraphContent() {
  const params = useSearchParams();
  const initialQuery = params.get("q") || "";
  const [graph, setGraph] = useState<ExplorerGraph>(emptyGraph);
  const [initialLoading, setInitialLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState(initialQuery);
  const [search, setSearch] = useState<{
    query: string;
    nodes: ExplorerNode[];
    total: number;
    offset: number;
    hasMore: boolean;
  } | null>(null);
  const [searching, setSearching] = useState(false);
  const [expanding, setExpanding] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedLinkId, setSelectedLinkId] = useState<string | null>(null);
  const [typeFilter, setTypeFilter] = useState("all");
  const [focused, setFocused] = useState(false);
  const [is3D, setIs3D] = useState(false);
  const [showLabels, setShowLabels] = useState(true);
  const [mobileList, setMobileList] = useState(false);
  const [seedLimit, setSeedLimit] = useState(100);
  const epoch = useRef(0);
  const searchVersion = useRef(0);
  const handleRef = useRef<GraphHandle | null>(null);

  const loadInitial = useCallback(async () => {
    const version = ++epoch.current;
    searchVersion.current++;
    setInitialLoading(true);
    setError(null);
    setSelectedNodeId(null);
    setSelectedLinkId(null);
    setSearch(null);
    setFocused(false);
    setExpanding(null);
    setSearching(false);
    setGraph(emptyGraph());
    try {
      const data = await getGraphInitial(seedLimit);
      if (version === epoch.current) setGraph(current => mergeGraph(current, data));
    } catch (error) {
      if (version === epoch.current) setError(message(error));
    } finally {
      if (version === epoch.current) setInitialLoading(false);
    }
  }, [seedLimit]);

  const invalidateRequests = useCallback(() => {
    epoch.current++;
    searchVersion.current++;
  }, []);
  useEffect(() => {
    void loadInitial();
    return invalidateRequests;
  }, [loadInitial, invalidateRequests]);

  const runSearch = useCallback(async (text: string, offset = 0) => {
    if (!text.trim()) {
      searchVersion.current++;
      setSearch(null);
      setSearching(false);
      return;
    }
    const version = ++searchVersion.current;
    setSearching(true);
    setError(null);
    try {
      const response = await graphSearch(text.trim(), undefined, 50, offset);
      const nodes = (response.results || [])
        .map(nodeFromPayload)
        .filter(
          (node: ExplorerNode | null): node is ExplorerNode => node !== null,
        );
      if (version === searchVersion.current)
        setSearch({ query: text.trim(), nodes, offset,
          total: response.total ?? nodes.length, hasMore: Boolean(response.has_more) });
    } catch (error) {
      if (version === searchVersion.current) setError(message(error));
    } finally {
      if (version === searchVersion.current) setSearching(false);
    }
  }, []);

  useEffect(() => {
    if (initialQuery) void runSearch(initialQuery);
  }, [initialQuery, runSearch]);

  const selectNode = useCallback((node: ExplorerNode) => {
    setGraph((current) =>
      current.nodes.some((item) => item.id === node.id)
        ? current
        : { ...current, nodes: [...current.nodes, node] },
    );
    setSelectedNodeId(node.id);
    setSelectedLinkId(null);
    setTypeFilter("all");
    setMobileList(false);
  }, []);

  const expandNode = useCallback(
    async (id: string) => {
      if (expanding) return;
      const version = epoch.current;
      setExpanding(id);
      setError(null);
      try {
        const data = await getGraphNeighbors(id, 1);
        if (version === epoch.current)
          setGraph((current) => mergeGraph(current, data));
      } catch (error) {
        if (version === epoch.current) setError(message(error));
      } finally {
        if (version === epoch.current) setExpanding(null);
      }
    },
    [expanding],
  );

  const clearSelection = useCallback(() => {
    setSelectedNodeId(null);
    setSelectedLinkId(null);
    setFocused(false);
  }, []);
  const selectLink = useCallback((link: ExplorerLink) => {
    setSelectedLinkId(link.id);
    setSelectedNodeId(null);
    setFocused(false);
  }, []);
  const onReady = useCallback((handle: GraphHandle | null) => {
    handleRef.current = handle;
  }, []);

  // Inspector selection does not change membership unless focus is enabled.
  // Keep the renderer's force data stable when only highlighting changes.
  const focusedNodeId = focused ? selectedNodeId : null;
  const visibleGraph = useMemo(() => {
    const neighborhood = new Set<string>();
    if (focusedNodeId) {
      neighborhood.add(focusedNodeId);
      for (const link of graph.links)
        if (link.source === focusedNodeId || link.target === focusedNodeId) {
          neighborhood.add(link.source);
          neighborhood.add(link.target);
        }
    }
    const nodes = graph.nodes.filter(
      (node) =>
        (typeFilter === "all" || node.label === typeFilter) &&
        (!focusedNodeId || neighborhood.has(node.id)),
    );
    const ids = new Set(nodes.map((node) => node.id));
    return {
      ...graph,
      nodes,
      links: graph.links.filter(
        (link) => ids.has(link.source) && ids.has(link.target),
      ),
    };
  }, [graph, typeFilter, focusedNodeId]);

  const selectedNode = graph.nodes.find((node) => node.id === selectedNodeId);
  const selectedLink = graph.links.find((link) => link.id === selectedLinkId);
  const listedNodes = (search?.nodes || visibleGraph.nodes)
    .slice()
    .sort((a, b) => a.name.localeCompare(b.name));
  const types = [...new Set(graph.nodes.map((node) => node.label))].sort();
  const labelFor = (id: string) =>
    graph.nodes.find((node) => node.id === id)?.name || id;

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3 md:px-5">
        <div>
          <div className="flex items-center gap-2">
            <Network className="h-4 w-4 text-blue-400" />
            <h1 className="font-semibold">Evidence graph</h1>
            <Badge variant="outline" className="text-[10px]">
              Loaded sample
            </Badge>
          </div>
          <p className="mt-1 text-xs text-muted-foreground">
            Explore connections, then inspect their recorded sources.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="md:hidden"
            onClick={() => {
              clearSelection();
              setMobileList((value) => !value);
            }}
            aria-expanded={mobileList}
          >
            <List className="h-4 w-4" />
            Browse
          </Button>
          <div
            className="flex rounded-md border p-0.5"
            aria-label="Graph dimensions"
          >
            <Button
              size="sm"
              variant={!is3D ? "secondary" : "ghost"}
              aria-pressed={!is3D}
              onClick={() => setIs3D(false)}
            >
              2D
            </Button>
            <Button
              size="sm"
              variant={is3D ? "secondary" : "ghost"}
              aria-pressed={is3D}
              onClick={() => setIs3D(true)}
            >
              3D
            </Button>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => handleRef.current?.zoomToFit(300, 55)}
          >
            <Focus className="h-4 w-4" />
            Fit view
          </Button>
          <Button
            variant="ghost"
            size="sm"
            aria-label="Reset sample"
            onClick={() => void loadInitial()}
            disabled={initialLoading}
          >
            <RefreshCw
              className={`h-4 w-4 ${initialLoading ? "animate-spin" : ""}`}
            />
            <span className="hidden sm:inline">Reset sample</span>
          </Button>
        </div>
      </header>
      {error && (
        <div
          role="alert"
          className="flex items-center justify-between gap-2 border-b border-destructive/30 bg-destructive/10 px-4 py-2 text-sm"
        >
          <span>Could not load graph data: {error}</span>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => setError(null)}
            aria-label="Dismiss error"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      )}
      <div className="relative flex min-h-0 flex-1">
        <aside
          aria-label="Graph browser"
          className={`${mobileList ? "absolute inset-y-0 left-0 z-30 flex w-[min(320px,90vw)] shadow-xl" : "hidden"} flex-col border-r bg-card md:static md:flex md:w-64 md:shrink-0`}
        >
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void runSearch(query);
            }}
            className="space-y-2 border-b p-3"
          >
            <label htmlFor="graph-search" className="text-xs font-medium">
              Find an entity or document
            </label>
            <div className="flex gap-1">
              <Input
                id="graph-search"
                placeholder="Search the graph…"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                className="min-w-0 text-sm"
              />
              <Button
                type="submit"
                size="icon"
                variant="secondary"
                aria-label="Search graph"
                disabled={searching}
              >
                {searching ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Search className="h-4 w-4" />
                )}
              </Button>
            </div>
            {search && (
              <div className="flex items-center justify-between gap-1 text-xs text-muted-foreground">
                <span>Results for “{search.query}”</span>
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  onClick={() => {
                    searchVersion.current++;
                    setSearch(null);
                    setQuery("");
                    setSearching(false);
                  }}
                  aria-label="Clear search"
                >
                  <X className="h-3 w-3" />
                </Button>
              </div>
            )}
          </form>
          <div className="space-y-2 border-b px-3 py-3">
            <div className="flex items-center justify-between gap-2">
              <label htmlFor="graph-type" className="text-xs">
                Show type
              </label>
              <select
                id="graph-type"
                value={typeFilter}
                onChange={(event) => setTypeFilter(event.target.value)}
                className="max-w-36 rounded border bg-background px-2 py-1 text-xs"
              >
                <option value="all">All types</option>
                {types.map((type) => (
                  <option key={type}>{type}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center justify-between gap-2">
              <label htmlFor="graph-seeds" className="text-xs">
                Sample seed size
              </label>
              <select
                id="graph-seeds"
                value={seedLimit}
                onChange={(event) => setSeedLimit(Number(event.target.value))}
                className="rounded border bg-background px-2 py-1 text-xs"
              >
                {[50, 100, 200, 300].map((limit) => (
                  <option key={limit}>{limit}</option>
                ))}
              </select>
            </div>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={showLabels}
                disabled={is3D}
                onChange={(event) => setShowLabels(event.target.checked)}
              />
              Show 2D labels
            </label>
          </div>
          <div className="px-3 py-2 text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
            {search
              ? `${listedNodes.length} on this page · ${search.total} matches`
              : `${listedNodes.length} loaded nodes`}
          </div>
          {search && (search.offset > 0 || search.hasMore) && (
            <div className="flex items-center justify-between gap-2 px-3 pb-2">
              <Button size="sm" variant="outline" disabled={searching || search.offset === 0}
                onClick={() => void runSearch(search.query, Math.max(0, search.offset - 50))}>
                Previous
              </Button>
              <span className="text-xs text-muted-foreground">Page {Math.floor(search.offset / 50) + 1}</span>
              <Button size="sm" variant="outline" disabled={searching || !search.hasMore}
                onClick={() => void runSearch(search.query, search.offset + 50)}>
                Next
              </Button>
            </div>
          )}
          <div className="min-h-0 flex-1 overflow-y-auto px-2 pb-3">
            {listedNodes.map((node) => (
              <button
                key={node.id}
                onClick={() => selectNode(node)}
                aria-pressed={selectedNodeId === node.id}
                className={`mb-1 flex w-full items-start gap-2 rounded-lg border p-2.5 text-left transition-colors hover:bg-accent ${selectedNodeId === node.id ? "border-blue-400/40 bg-blue-400/10" : "border-transparent"}`}
              >
                <span
                  className={`mt-1.5 h-2.5 w-2.5 shrink-0 ${node.label === "Document" ? "rounded-sm" : "rounded-full"}`}
                  style={{ backgroundColor: getNodeColor(node.label) }}
                />
                <span className="min-w-0">
                  <span className="line-clamp-2 block text-xs font-medium leading-relaxed">
                    {node.name}
                  </span>
                  <span className="text-[10px] text-muted-foreground">
                    {node.label}
                  </span>
                </span>
              </button>
            ))}
            {!initialLoading && !listedNodes.length && (
              <p className="p-3 text-xs text-muted-foreground">
                {search
                  ? "No matches returned. Try another name or document title."
                  : "No nodes in this view. Clear filters or search the graph."}
              </p>
            )}
          </div>
          <p className="border-t p-3 text-[11px] leading-relaxed text-muted-foreground">
            This is a partial view, not the complete archive. Search and expand
            nodes to explore more.
          </p>
        </aside>
        <section
          className="relative min-w-0 flex-1"
          aria-label="Graph workspace"
        >
          {initialLoading ? (
            <div
              role="status"
              className="flex h-full items-center justify-center gap-2 text-sm"
            >
              <Loader2 className="h-5 w-5 animate-spin" />
              Loading graph sample…
            </div>
          ) : visibleGraph.nodes.length ? (
            <ForceGraphClient
              graph={visibleGraph}
              is3D={is3D}
              showLabels={showLabels}
              selectedNodeId={selectedNodeId}
              selectedLinkId={selectedLinkId}
              onSelectNode={selectNode}
              onSelectLink={selectLink}
              onClear={clearSelection}
              onReady={onReady}
              onUse2D={() => setIs3D(false)}
            />
          ) : (
            <div className="flex h-full flex-col items-center justify-center gap-3 p-8 text-center">
              <Network className="h-8 w-8 text-muted-foreground" />
              <h2 className="font-medium">No nodes to display</h2>
              <p className="max-w-sm text-sm text-muted-foreground">
                {error
                  ? "The request failed. Retry loading the sample."
                  : graph.nodes.length
                    ? "Your filters hide all loaded nodes."
                    : "Search for a document or entity, or load another sample."}
              </p>
              <Button
                variant="outline"
                onClick={() => {
                  setTypeFilter("all");
                  setFocused(false);
                  if (!graph.nodes.length) void loadInitial();
                }}
              >
                Reset view
              </Button>
            </div>
          )}
          <div className="pointer-events-none absolute bottom-3 left-3 right-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-300">
            <span
              role="status"
              className="rounded-md border border-slate-600/40 bg-[#101820ed] px-3 py-2"
            >
              {visibleGraph.nodes.length} nodes · {visibleGraph.links.length}{" "}
              relationships in view{expanding ? " · Expanding…" : ""}
            </span>
            <span className="hidden rounded-md border border-slate-600/40 bg-[#101820ed] px-3 py-2 sm:block">
              Click to inspect · Drag to pin · Scroll to zoom ·{" "}
              {is3D ? "Amber" : "Dashed amber"} = inferred
            </span>
          </div>
          {graph.discarded > 0 && (
            <p
              role="status"
              className="absolute left-3 top-3 max-w-sm rounded border border-amber-400/30 bg-background/95 p-2 text-xs text-amber-300"
            >
              Some returned nodes or edges lack usable identities or endpoints
              and were omitted ({graph.discarded}).
            </p>
          )}
        </section>
        {(selectedNode || selectedLink) && (
          <aside
            aria-label="Evidence inspector"
            className="absolute inset-y-0 right-0 z-30 w-full overflow-y-auto border-l bg-card shadow-xl sm:static sm:w-80 sm:shrink-0 xl:w-[380px]"
          >
            {selectedNode && (
              <>
                <div className="flex items-center justify-between gap-2 border-b px-4 py-2 text-xs">
                  <span className="text-muted-foreground">
                    Entity and source inspection
                  </span>
                  <Button
                    size="sm"
                    variant={focused ? "secondary" : "ghost"}
                    onClick={() => setFocused((value) => !value)}
                    aria-pressed={focused}
                  >
                    <Focus className="h-3 w-3" />
                    {focused ? "Show sample" : "Focus neighborhood"}
                  </Button>
                </div>
                <NodeDetailPanel
                  node={{
                    ...selectedNode,
                    color: getNodeColor(selectedNode.label),
                  }}
                  onClose={clearSelection}
                  onExpandNeighbors={expandNode}
                  expanding={Boolean(expanding)}
                  onSelectNode={selectNode}
                />
              </>
            )}
            {selectedLink && (
              <div className="space-y-5 p-5">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-xs text-muted-foreground">
                      Relationship
                    </p>
                    <h2 className="mt-1 font-semibold">
                      {selectedLink.type.replaceAll("_", " ")}
                    </h2>
                  </div>
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label="Close relationship inspector"
                    onClick={clearSelection}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                </div>
                <div className="space-y-2 rounded-lg border bg-background p-3 text-sm">
                  <p>{labelFor(selectedLink.source)}</p>
                  <ArrowRight className="h-4 w-4 text-muted-foreground" />
                  <p>{labelFor(selectedLink.target)}</p>
                </div>
                <Badge variant="outline">
                  {selectedLink.props.implied
                    ? "Inferred relationship"
                    : "Extracted relationship"}
                </Badge>
                <p className="text-xs leading-relaxed text-muted-foreground">
                  An extracted connection is not an independently verified fact.
                  Inspect the original document before relying on it.
                </p>
                <div>
                  <h3 className="mb-2 text-xs font-medium">
                    Recorded source documents
                  </h3>
                  {sourceDocumentIds(selectedLink.props).map((id) => (
                    <Link
                      key={id}
                      href={`/documents/${id}`}
                      className="mb-2 flex items-center gap-2 rounded border p-3 text-sm hover:bg-accent"
                    >
                      <FileText className="h-4 w-4" />
                      Document #{id}
                    </Link>
                  ))}
                  {!sourceDocumentIds(selectedLink.props).length && (
                    <p className="text-xs text-amber-300">
                      No source document is recorded for this relationship.
                    </p>
                  )}
                </div>
                <details className="text-xs">
                  <summary className="cursor-pointer text-muted-foreground">
                    Recorded properties
                  </summary>
                  <pre className="mt-2 whitespace-pre-wrap break-all rounded bg-background p-3">
                    {JSON.stringify(selectedLink.props, null, 2)}
                  </pre>
                </details>
              </div>
            )}
          </aside>
        )}
      </div>
    </div>
  );
}

export function GraphContainer() {
  return (
    <Suspense
      fallback={
        <div className="flex h-full items-center justify-center">
          Loading graph…
        </div>
      }
    >
      <GraphContent />
    </Suspense>
  );
}
