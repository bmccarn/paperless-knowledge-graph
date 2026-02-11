"use client";

import { useState, useCallback, useRef, useEffect, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { graphSearch, getGraphNeighbors, getGraphInitial } from "@/lib/api";
import { NodeDetailPanel } from "@/components/node-detail-panel";
import {
  Search,
  Loader2,
  Maximize2,
  ZoomIn,
  ZoomOut,
  Crosshair,
  Tag,
  Atom,
  X,
} from "lucide-react";

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});

interface GNode {
  id: string;
  name: string;
  label: string;
  props: Record<string, unknown>;
  color: string;
  val: number;
}

interface GLink {
  source: string;
  target: string;
  type: string;
}

const NODE_PALETTE: Record<string, { color: string; label: string }> = {
  Person: { color: "#60a5fa", label: "Person" },
  Organization: { color: "#34d399", label: "Organization" },
  Document: { color: "#94a3b8", label: "Document" },
  MedicalResult: { color: "#f87171", label: "Medical" },
  Medical_Result: { color: "#f87171", label: "Medical" },
  FinancialItem: { color: "#fbbf24", label: "Financial" },
  Financial_Item: { color: "#fbbf24", label: "Financial" },
  Address: { color: "#22d3ee", label: "Address" },
  Date: { color: "#fb923c", label: "Date" },
  Account: { color: "#a78bfa", label: "Account" },
};

const DEFAULT_COLOR = "#c084fc";

function getColor(label: string): string {
  return NODE_PALETTE[label]?.color || DEFAULT_COLOR;
}

function GraphContent() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get("q") || "";

  const [graphData, setGraphData] = useState<{ nodes: GNode[]; links: GLink[] }>({
    nodes: [],
    links: [],
  });
  const [selectedNode, setSelectedNode] = useState<GNode | null>(null);
  const [searchQuery, setSearchQuery] = useState(initialQuery);
  const [searchResults, setSearchResults] = useState<
    Array<{ labels: string[]; properties: Record<string, unknown> }>
  >([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [nodeTypes, setNodeTypes] = useState<Set<string>>(new Set());
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(new Set());
  const [showLabels, setShowLabels] = useState(true);
  const [showLegend, setShowLegend] = useState(true);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fgRef = useRef<any>(undefined);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });

  useEffect(() => {
    const updateDimensions = () => {
      if (containerRef.current) {
        setDimensions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight,
        });
      }
    };
    updateDimensions();
    const observer = new ResizeObserver(updateDimensions);
    if (containerRef.current) observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, [selectedNode]);

  useEffect(() => {
    const loadInitial = async () => {
      setInitialLoading(true);
      try {
        const data = await getGraphInitial(300);
        const nodeMap = new Map<string, GNode>();
        const types = new Set<string>();

        for (const node of data.nodes || []) {
          const p = node.props || node.properties || {};
          const id = (p.uuid as string) || `doc-${p.paperless_id}` || (p.name as string);
          if (!id || nodeMap.has(id)) continue;
          const label = node.labels?.[0] || "Unknown";
          types.add(label);
          nodeMap.set(id, {
            id,
            name: (p.name as string) || (p.title as string) || id,
            label,
            props: p,
            color: getColor(label),
            val: label === "Document" ? 2 : 4,
          });
        }

        const links: GLink[] = [];
        for (const rel of data.relationships || []) {
          const src = rel.start || "";
          const tgt = rel.end || "";
          if (src && tgt && nodeMap.has(src) && nodeMap.has(tgt)) {
            links.push({ source: src, target: tgt, type: rel.type });
          }
        }

        setGraphData({ nodes: Array.from(nodeMap.values()), links });
        setNodeTypes(types);
      } catch (e) {
        console.error("Failed to load initial graph:", e);
      } finally {
        setInitialLoading(false);
      }
    };
    loadInitial();
  }, []);

  // Handle initial search query from URL
  useEffect(() => {
    if (initialQuery && !initialLoading) {
      handleSearch(initialQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialLoading]);

  const addNeighbors = useCallback(
    async (nodeId: string) => {
      setLoading(true);
      try {
        const data = await getGraphNeighbors(nodeId, 1);
        setGraphData((prev) => {
          const existingIds = new Set(prev.nodes.map((n) => n.id));
          const newNodes: GNode[] = [];
          const types = new Set(nodeTypes);

          for (const node of data.nodes || []) {
            const p = node.props || node.properties || {};
            const id = (p.uuid as string) || `doc-${p.paperless_id}` || (p.name as string);
            if (!id || existingIds.has(id)) continue;
            const label = node.labels?.[0] || "Unknown";
            types.add(label);
            newNodes.push({
              id,
              name: (p.name as string) || (p.title as string) || id,
              label,
              props: p,
              color: getColor(label),
              val: label === "Document" ? 2 : 4,
            });
            existingIds.add(id);
          }

          const newLinks: GLink[] = [];
          for (const rel of data.relationships || []) {
            const src = rel.start || `doc-${rel.props?.source_doc}`;
            const tgt = rel.end || `doc-${rel.props?.source_doc}`;
            if (src && tgt && existingIds.has(src) && existingIds.has(tgt)) {
              newLinks.push({ source: src, target: tgt, type: rel.type });
            }
          }

          setNodeTypes(types);
          return {
            nodes: [...prev.nodes, ...newNodes],
            links: [...prev.links, ...newLinks],
          };
        });
      } catch (e) {
        console.error("Failed to load neighbors:", e);
      } finally {
        setLoading(false);
      }
    },
    [nodeTypes]
  );

  const handleSearch = async (query?: string) => {
    const q = query || searchQuery;
    if (!q.trim()) return;
    setLoading(true);
    try {
      const data = await graphSearch(q);
      setSearchResults(data.results || []);
    } catch (e) {
      console.error("Search failed:", e);
    } finally {
      setLoading(false);
    }
  };

  const addNodeFromSearch = async (result: {
    labels: string[];
    properties: Record<string, unknown>;
  }) => {
    const p = result.properties;
    const id = (p.uuid as string) || `doc-${p.paperless_id}`;
    if (!id) return;

    const label = result.labels?.[0] || "Unknown";
    const exists = graphData.nodes.find((n) => n.id === id);

    if (!exists) {
      const node: GNode = {
        id,
        name: (p.name as string) || (p.title as string) || id,
        label,
        props: p,
        color: getColor(label),
        val: label === "Document" ? 2 : 4,
      };
      setGraphData((prev) => ({ ...prev, nodes: [...prev.nodes, node] }));
      setNodeTypes((prev) => new Set([...prev, label]));
    }

    setSearchResults([]);
    setSearchQuery("");
    await addNeighbors(id);
  };

  const filteredData = {
    nodes: graphData.nodes.filter((n) => !hiddenTypes.has(n.label)),
    links: graphData.links.filter((l) => {
      const srcId = typeof l.source === "object" ? (l.source as unknown as GNode).id : l.source;
      const tgtId = typeof l.target === "object" ? (l.target as unknown as GNode).id : l.target;
      return (
        graphData.nodes.find((n) => n.id === srcId && !hiddenTypes.has(n.label)) &&
        graphData.nodes.find((n) => n.id === tgtId && !hiddenTypes.has(n.label))
      );
    }),
  };

  const handleZoomIn = () => fgRef.current?.zoom(fgRef.current.zoom() * 1.3, 300);
  const handleZoomOut = () => fgRef.current?.zoom(fgRef.current.zoom() * 0.7, 300);
  const handleZoomReset = () => fgRef.current?.zoomToFit(400, 60);

  return (
    <div className="flex h-full">
      {/* Graph area */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Top bar */}
        <div className="border-b px-4 py-2.5 flex items-center gap-3 bg-card/50 backdrop-blur-sm">
          <h1 className="text-base font-semibold whitespace-nowrap">Graph Explorer</h1>
          <div className="flex-1 flex gap-2 max-w-md ml-2 relative">
            <div className="relative flex-1">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleSearch()}
                placeholder="Search nodes..."
                className="pl-8 h-8 text-sm"
              />
            </div>
            <Button onClick={() => handleSearch()} size="sm" disabled={loading} className="h-8">
              {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Search className="h-3.5 w-3.5" />}
            </Button>

            {/* Search results dropdown */}
            {searchResults.length > 0 && (
              <div className="absolute z-50 top-full mt-1 left-0 right-0 bg-popover border rounded-lg shadow-xl overflow-hidden">
                <ScrollArea className="max-h-64">
                  {searchResults.map((r, i) => {
                    const p = r.properties;
                    return (
                      <button
                        key={i}
                        className="w-full text-left px-3 py-2 text-sm hover:bg-accent/70 flex items-center gap-2 transition-colors"
                        onClick={() => addNodeFromSearch(r)}
                      >
                        <span
                          className="h-2.5 w-2.5 rounded-full shrink-0"
                          style={{ backgroundColor: getColor(r.labels?.[0] || "") }}
                        />
                        <span className="truncate flex-1">
                          {(p.name as string) || (p.title as string)}
                        </span>
                        <Badge variant="secondary" className="text-[10px] shrink-0">
                          {r.labels?.[0]}
                        </Badge>
                      </button>
                    );
                  })}
                </ScrollArea>
                <button
                  className="w-full text-center py-1.5 text-xs text-muted-foreground hover:bg-accent/50 border-t transition-colors"
                  onClick={() => setSearchResults([])}
                >
                  Close
                </button>
              </div>
            )}
          </div>
          <Badge variant="secondary" className="text-xs font-mono shrink-0">
            {filteredData.nodes.length}N · {filteredData.links.length}E
          </Badge>
        </div>

        {/* Graph canvas */}
        <div ref={containerRef} className="flex-1 relative bg-background">
          {initialLoading ? (
            <div className="flex h-full items-center justify-center">
              <div className="text-center space-y-3">
                <Loader2 className="h-10 w-10 mx-auto animate-spin text-primary/40" />
                <p className="text-sm text-muted-foreground">Loading knowledge graph...</p>
              </div>
            </div>
          ) : graphData.nodes.length === 0 ? (
            <div className="flex h-full items-center justify-center text-muted-foreground">
              <div className="text-center space-y-3">
                <Maximize2 className="h-10 w-10 mx-auto opacity-20" />
                <p className="text-sm">No graph data. Try syncing documents first.</p>
              </div>
            </div>
          ) : (
            <>
              <ForceGraph2D
                ref={fgRef}
                graphData={filteredData}
                width={dimensions.width}
                height={dimensions.height}
                nodeLabel={() => ""}
                nodeColor={(node: unknown) => (node as GNode).color}
                nodeVal={(node: unknown) => (node as GNode).val}
                nodeCanvasObject={(node: unknown, ctx: CanvasRenderingContext2D, globalScale: number) => {
                  const n = node as GNode & { x: number; y: number };
                  const fontSize = 10 / globalScale;
                  const r = Math.sqrt(n.val) * 3;

                  // Glow for selected node
                  if (selectedNode?.id === n.id) {
                    ctx.beginPath();
                    ctx.arc(n.x, n.y, r + 4 / globalScale, 0, 2 * Math.PI);
                    ctx.fillStyle = `${n.color}33`;
                    ctx.fill();
                  }

                  // Draw circle
                  ctx.beginPath();
                  ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
                  ctx.fillStyle = n.color;
                  ctx.fill();

                  if (selectedNode?.id === n.id) {
                    ctx.strokeStyle = "#fff";
                    ctx.lineWidth = 2 / globalScale;
                    ctx.stroke();
                  }

                  // Label
                  if (showLabels && globalScale > 1.2) {
                    ctx.font = `${fontSize}px Sans-Serif`;
                    ctx.textAlign = "center";
                    ctx.textBaseline = "top";
                    ctx.fillStyle = "rgba(229, 231, 235, 0.9)";
                    ctx.fillText(n.name, n.x, n.y + r + 2);
                  }
                }}
                nodeCanvasObjectMode={() => "replace"}
                nodePointerAreaPaint={(node: unknown, color: string, ctx: CanvasRenderingContext2D) => {
                  const n = node as GNode & { x: number; y: number };
                  const r = Math.sqrt(n.val) * 3 + 2;
                  ctx.beginPath();
                  ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
                  ctx.fillStyle = color;
                  ctx.fill();
                }}
                linkLabel={(link: unknown) => (link as GLink).type}
                linkColor={() => "rgba(148, 163, 184, 0.12)"}
                linkWidth={0.5}
                linkDirectionalArrowLength={3}
                linkDirectionalArrowRelPos={1}
                linkDirectionalArrowColor={() => "rgba(148, 163, 184, 0.25)"}
                onNodeClick={(node: unknown) => setSelectedNode(node as GNode)}
                onNodeRightClick={async (node: unknown) => {
                  const n = node as GNode;
                  await addNeighbors(n.id);
                }}
                onBackgroundClick={() => setSelectedNode(null)}
                backgroundColor="transparent"
                cooldownTicks={100}
                d3AlphaDecay={0.03}
                d3VelocityDecay={0.3}
              />

              {/* Floating toolbar */}
              <div className="absolute bottom-4 left-1/2 -translate-x-1/2 flex items-center gap-1 bg-card/90 backdrop-blur-md border rounded-lg px-2 py-1.5 shadow-lg">
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={handleZoomIn}>
                      <ZoomIn className="h-3.5 w-3.5" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Zoom in</TooltipContent>
                </Tooltip>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={handleZoomOut}>
                      <ZoomOut className="h-3.5 w-3.5" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Zoom out</TooltipContent>
                </Tooltip>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={handleZoomReset}>
                      <Crosshair className="h-3.5 w-3.5" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Fit to view</TooltipContent>
                </Tooltip>
                <div className="w-px h-4 bg-border mx-0.5" />
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      variant={showLabels ? "secondary" : "ghost"}
                      size="icon"
                      className="h-7 w-7"
                      onClick={() => setShowLabels(!showLabels)}
                    >
                      <Tag className="h-3.5 w-3.5" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Toggle labels</TooltipContent>
                </Tooltip>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      variant={showLegend ? "secondary" : "ghost"}
                      size="icon"
                      className="h-7 w-7"
                      onClick={() => setShowLegend(!showLegend)}
                    >
                      <Atom className="h-3.5 w-3.5" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Toggle legend</TooltipContent>
                </Tooltip>
              </div>

              {/* Legend */}
              {showLegend && nodeTypes.size > 0 && (
                <div className="absolute top-3 left-3 bg-card/90 backdrop-blur-md border rounded-lg px-3 py-2.5 shadow-lg">
                  <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1.5">Legend</p>
                  <div className="space-y-1">
                    {[...nodeTypes].sort().map((type) => (
                      <label
                        key={type}
                        className="flex items-center gap-2 text-xs cursor-pointer hover:text-foreground transition-colors"
                      >
                        <Checkbox
                          checked={!hiddenTypes.has(type)}
                          onCheckedChange={(checked) => {
                            setHiddenTypes((prev) => {
                              const next = new Set(prev);
                              if (checked) next.delete(type);
                              else next.add(type);
                              return next;
                            });
                          }}
                          className="h-3.5 w-3.5"
                        />
                        <span
                          className="h-2.5 w-2.5 rounded-full shrink-0"
                          style={{ backgroundColor: getColor(type) }}
                        />
                        <span className={hiddenTypes.has(type) ? "text-muted-foreground line-through" : ""}>
                          {NODE_PALETTE[type]?.label || type}
                        </span>
                      </label>
                    ))}
                  </div>
                </div>
              )}

              {/* Loading indicator */}
              {loading && (
                <div className="absolute top-3 right-3 bg-card/90 backdrop-blur-md border rounded-lg px-3 py-2 shadow-lg flex items-center gap-2">
                  <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
                  <span className="text-xs">Loading...</span>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* Right panel — node detail */}
      {selectedNode && (
        <div className="w-96 border-l flex flex-col bg-card/50 slide-in-right">
          <NodeDetailPanel
            node={selectedNode}
            onClose={() => setSelectedNode(null)}
            onExpandNeighbors={addNeighbors}
          />
        </div>
      )}
    </div>
  );
}

export default function GraphPage() {
  return (
    <Suspense fallback={
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary/40" />
      </div>
    }>
      <GraphContent />
    </Suspense>
  );
}
