"use client";

import { useState, useCallback, useRef, useEffect } from "react";
import dynamic from "next/dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { graphSearch, getGraphNeighbors } from "@/lib/api";
import { NodeDetailPanel } from "@/components/node-detail-panel";
import { Search, Loader2, Maximize2 } from "lucide-react";

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

const NODE_COLORS: Record<string, string> = {
  Person: "#3b82f6",
  Organization: "#22c55e",
  Document: "#6b7280",
  MedicalResult: "#ef4444",
  Medical_Result: "#ef4444",
  FinancialItem: "#eab308",
  Financial_Item: "#eab308",
  Address: "#06b6d4",
  Date: "#f97316",
  Account: "#8b5cf6",
};

function getColor(label: string): string {
  return NODE_COLORS[label] || "#a855f7";
}

export default function GraphPage() {
  const [graphData, setGraphData] = useState<{ nodes: GNode[]; links: GLink[] }>({
    nodes: [],
    links: [],
  });
  const [selectedNode, setSelectedNode] = useState<GNode | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<
    Array<{ labels: string[]; properties: Record<string, unknown> }>
  >([]);
  const [loading, setLoading] = useState(false);
  const [nodeTypes, setNodeTypes] = useState<Set<string>>(new Set());
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(new Set());
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
    window.addEventListener("resize", updateDimensions);
    return () => window.removeEventListener("resize", updateDimensions);
  }, []);

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

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setLoading(true);
    try {
      const data = await graphSearch(searchQuery);
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

    // Load neighbors
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

  return (
    <div className="flex h-full">
      {/* Graph area */}
      <div className="flex-1 flex flex-col">
        <div className="border-b px-4 py-3 flex items-center gap-3">
          <h1 className="text-lg font-semibold">Graph Explorer</h1>
          <div className="flex-1 flex gap-2 max-w-md ml-4">
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleSearch()}
              placeholder="Search nodes..."
              className="flex-1"
            />
            <Button onClick={handleSearch} size="sm" disabled={loading}>
              {loading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Search className="h-4 w-4" />
              )}
            </Button>
          </div>
          <Badge variant="secondary">
            {filteredData.nodes.length} nodes · {filteredData.links.length} links
          </Badge>
        </div>

        {/* Search results dropdown */}
        {searchResults.length > 0 && (
          <div className="absolute z-50 top-16 left-72 w-96 bg-popover border rounded-md shadow-lg">
            <ScrollArea className="max-h-64">
              {searchResults.map((r, i) => {
                const p = r.properties;
                return (
                  <button
                    key={i}
                    className="w-full text-left px-3 py-2 text-sm hover:bg-accent flex items-center gap-2"
                    onClick={() => addNodeFromSearch(r)}
                  >
                    <Badge
                      variant="secondary"
                      className="text-[10px]"
                      style={{
                        backgroundColor: getColor(r.labels?.[0] || ""),
                        color: "white",
                      }}
                    >
                      {r.labels?.[0]}
                    </Badge>
                    <span className="truncate">
                      {(p.name as string) || (p.title as string)}
                    </span>
                  </button>
                );
              })}
            </ScrollArea>
            <Button
              variant="ghost"
              size="sm"
              className="w-full"
              onClick={() => setSearchResults([])}
            >
              Close
            </Button>
          </div>
        )}

        {/* Graph canvas */}
        <div ref={containerRef} className="flex-1 relative">
          {graphData.nodes.length === 0 ? (
            <div className="flex h-full items-center justify-center text-muted-foreground">
              <div className="text-center">
                <Maximize2 className="h-12 w-12 mx-auto mb-3 opacity-30" />
                <p>Search for a node to start exploring the graph</p>
              </div>
            </div>
          ) : (
            <ForceGraph2D
              ref={fgRef}
              graphData={filteredData}
              width={dimensions.width}
              height={dimensions.height - 60}
              nodeLabel={(node: unknown) => {
                const n = node as GNode;
                return `${n.name} (${n.label})`;
              }}
              nodeColor={(node: unknown) => (node as GNode).color}
              nodeVal={(node: unknown) => (node as GNode).val}
              nodeCanvasObject={(node: unknown, ctx: CanvasRenderingContext2D, globalScale: number) => {
                const n = node as GNode & { x: number; y: number };
                const fontSize = 10 / globalScale;
                const r = Math.sqrt(n.val) * 3;

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
                if (globalScale > 1.5) {
                  ctx.font = `${fontSize}px Sans-Serif`;
                  ctx.textAlign = "center";
                  ctx.textBaseline = "top";
                  ctx.fillStyle = "#e5e7eb";
                  ctx.fillText(n.name, n.x, n.y + r + 2);
                }
              }}
              linkLabel={(link: unknown) => (link as GLink).type}
              linkColor={() => "rgba(255,255,255,0.15)"}
              linkDirectionalArrowLength={3}
              linkDirectionalArrowRelPos={1}
              onNodeClick={(node: unknown) => setSelectedNode(node as GNode)}
              onNodeRightClick={async (node: unknown) => {
                const n = node as GNode;
                await addNeighbors(n.id);
              }}
              onBackgroundClick={() => setSelectedNode(null)}
              backgroundColor="transparent"
              cooldownTicks={100}
            />
          )}
        </div>
      </div>

      {/* Right panel */}
      <div className={`${selectedNode ? "w-96" : "w-72"} border-l flex flex-col transition-all`}>
        {/* Filters */}
        <div className="border-b p-3">
          <p className="text-xs font-medium mb-2">Filter by Type</p>
          <div className="space-y-1.5">
            {[...nodeTypes].sort().map((type) => (
              <label key={type} className="flex items-center gap-2 text-xs cursor-pointer">
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
                />
                <span
                  className="h-2.5 w-2.5 rounded-full"
                  style={{ backgroundColor: getColor(type) }}
                />
                {type}
              </label>
            ))}
            {nodeTypes.size === 0 && (
              <p className="text-xs text-muted-foreground">No nodes loaded</p>
            )}
          </div>
        </div>

        {/* Node detail dossier */}
        {selectedNode ? (
          <NodeDetailPanel
            node={selectedNode}
            onClose={() => setSelectedNode(null)}
            onExpandNeighbors={addNeighbors}
          />
        ) : (
          <div className="p-4 text-xs text-muted-foreground space-y-2">
            <p>Click a node to view its full dossier</p>
            <p>Right-click a node to expand neighbors</p>
          </div>
        )}
      </div>
    </div>
  );
}
