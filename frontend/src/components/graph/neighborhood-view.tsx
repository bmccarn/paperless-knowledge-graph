"use client";

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { Focus, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { NodeDetailPanel } from "@/components/node-detail-panel";
import { errMsg } from "@/lib/utils";
import { getGraphNodeSummary } from "@/lib/api";
import type { ExplorerGraph, ExplorerLink, ExplorerNode } from "@/lib/graph-data";
import {
  familyCap,
  neighborhoodLayout,
  showMore,
  VIEW_LIMIT,
  type NodeSummary,
} from "@/lib/graph-layout";
import type { GraphHandle } from "./force-graph-client";

const ForceGraphClient = dynamic(
  () => import("./force-graph-client").then((module) => module.ForceGraphClient),
  { ssr: false },
);

const PHONE = "(max-width: 767px)";
const subscribePhone = (change: () => void) => {
  const query = window.matchMedia(PHONE);
  query.addEventListener("change", change);
  return () => query.removeEventListener("change", change);
};

/** `/graph?focus=<id>`: the focus entity and its capped neighbors in fixed radial sectors. */
export function NeighborhoodView({ focusId }: { focusId: string }) {
  const router = useRouter();
  const [summary, setSummary] = useState<NodeSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [extra, setExtra] = useState<Record<string, number>>({});
  const [atLimit, setAtLimit] = useState(false);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedLinkId, setSelectedLinkId] = useState<string | null>(null);
  const [keyboardNodeId, setKeyboardNodeId] = useState<string | null>(null);
  const phone = useSyncExternalStore(subscribePhone, () => window.matchMedia(PHONE).matches, () => false);
  const handleRef = useRef<GraphHandle | null>(null);

  useEffect(() => {
    let active = true;
    getGraphNodeSummary(focusId)
      .then((data) => { if (active) setSummary(data); })
      .catch((error) => { if (active) setError(errMsg(error, "Request failed.")); });
    return () => { active = false; };
  }, [focusId, attempt]);

  const cap = summary ? familyCap(summary.focus.is_hub, phone) : 0;
  const layout = useMemo(() => summary && neighborhoodLayout(summary, cap, extra), [summary, cap, extra]);
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
      links: (layout?.links ?? []).map(({ key, source, target, type, link }) => ({
        id: key, source, target, type,
        props: link
          ? { implied: link.support_state === "inferred", source_doc_ids: link.records.map((record) => record.doc_id) }
          : { aggregate: true },
      })),
      discarded: 0,
    };
    return { graph, positions: new Map(layout?.nodes.map(({ id, x, y }) => [id, { x, y }])) };
  }, [layout, summary]);

  const openFocus = useCallback((id: string) => router.push(`/graph?focus=${encodeURIComponent(id)}`), [router]);
  const selectNode = useCallback((node: ExplorerNode) => {
    if (!summary) return;
    if (node.label === "Aggregate") {
      // shortcut: a pill press adds neighbors; the inspector's pill member list arrives with #131.
      const next = showMore(summary, cap, extra, node.id.slice("pill:".length));
      if (next !== extra) setExtra(next);
      else if (layout && layout.entities >= VIEW_LIMIT) setAtLimit(true);
      return;
    }
    setSelectedNodeId(node.id);
    setSelectedLinkId(null);
    setKeyboardNodeId(null);
  }, [summary, cap, extra, layout]);
  const selectLink = useCallback((link: ExplorerLink) => {
    setSelectedLinkId(link.id);
    setSelectedNodeId(null);
  }, []);
  const clearSelection = useCallback(() => {
    setSelectedNodeId(null);
    setSelectedLinkId(null);
    setKeyboardNodeId(null);
  }, []);
  const onReady = useCallback((handle: GraphHandle | null) => { handleRef.current = handle; }, []);

  const selectedNode = graph.nodes.find((node) => node.id === selectedNodeId && node.label !== "Aggregate");
  const linkTotal = layout?.sectors.reduce((sum, sector) => sum + sector.total, 0) ?? 0;

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3 md:px-5">
        <div>
          <h1 className="page-title">Graph</h1>
          {summary && layout && (
            <p className="mt-1 text-sm text-muted-foreground">
              {summary.focus.name}: {linkTotal} links in {layout.sectors.length} families.
              {summary.focus.is_hub && ` Large hub: the top ${cap} of each family are shown.`}
            </p>
          )}
        </div>
        <Button variant="outline" size="sm" onClick={() => handleRef.current?.zoomToFit(300, 55)} disabled={!layout}>
          <Focus className="h-4 w-4" />
          Fit view
        </Button>
      </header>
      {error && (
        <div role="alert" className="flex items-center justify-between gap-2 border-b border-destructive/30 bg-destructive/10 px-4 py-2 text-sm">
          <span>Could not load the neighborhood: {error}</span>
          <Button size="sm" variant="outline" onClick={() => { setError(null); setAttempt((value) => value + 1); }}>
            Retry
          </Button>
        </div>
      )}
      {atLimit && (
        <p role="status" className="border-b px-4 py-2 text-sm">
          The canvas shows at most {VIEW_LIMIT} entities. No more neighbors can be added to this view.
        </p>
      )}
      <div className="relative flex min-h-0 flex-1">
        <section className="relative min-w-0 flex-1" aria-label="Graph workspace">
          {layout ? (
            <ForceGraphClient
              graph={graph}
              positions={positions}
              focusId={focusId}
              keyboardNodeId={keyboardNodeId}
              showLabels
              selectedNodeId={selectedNodeId}
              selectedLinkId={selectedLinkId}
              onSelectNode={selectNode}
              onSelectLink={selectLink}
              onClear={clearSelection}
              onReady={onReady}
            />
          ) : !error && (
            <div role="status" className="flex h-full items-center justify-center gap-2 text-sm">
              <Loader2 className="h-5 w-5 animate-spin" />
              Loading neighbors…
            </div>
          )}
        </section>
        {selectedNode && (
          <aside
            aria-label="Evidence inspector"
            className="absolute inset-y-0 right-0 z-30 w-full overflow-y-auto border-l bg-card shadow-xl sm:static sm:w-80 sm:shrink-0 xl:w-[380px]"
          >
            <NodeDetailPanel
              key={selectedNode.id}
              node={selectedNode}
              onClose={clearSelection}
              onExpandNeighbors={openFocus}
              expanding={false}
              onSelectNode={(node) => (positions.has(node.id) ? selectNode(node) : openFocus(node.id))}
              onFocusNode={setKeyboardNodeId}
            />
          </aside>
        )}
      </div>
    </div>
  );
}
