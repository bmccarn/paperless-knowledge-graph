"use client";

import { useState, useCallback, useRef, useEffect, useMemo, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
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
  Box,
  Square,
} from "lucide-react";

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});

const ForceGraph3D = dynamic(() => import("react-force-graph-3d"), {
  ssr: false,
});

interface GNode {
  id: string;
  name: string;
  label: string;
  props: Record<string, unknown>;
  color: string;
  val: number;
  connections?: number;
}

interface GLink {
  source: string;
  target: string;
  type: string;
  weight?: number;
}

const NODE_PALETTE: Record<string, { color: string; label: string }> = {
  Person: { color: "#818cf8", label: "Person" },         // indigo
  Organization: { color: "#34d399", label: "Organization" }, // emerald
  Document: { color: "#64748b", label: "Document" },     // slate
  MedicalResult: { color: "#fb7185", label: "Medical" }, // rose
  Medical_Result: { color: "#fb7185", label: "Medical" },
  FinancialItem: { color: "#fbbf24", label: "Financial" }, // amber
  Financial_Item: { color: "#fbbf24", label: "Financial" },
  Address: { color: "#22d3ee", label: "Address" },       // cyan
  Date: { color: "#fb923c", label: "Date" },             // orange
  Account: { color: "#a78bfa", label: "Account" },       // violet
  Event: { color: "#f472b6", label: "Event" },           // pink
  Location: { color: "#2dd4bf", label: "Location" },     // teal
  Phone: { color: "#38bdf8", label: "Phone" },           // sky
  Email: { color: "#e879f9", label: "Email" },           // fuchsia
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
  const [highlightNodes, setHighlightNodes] = useState<Set<string>>(new Set());
  const [highlightLinks, setHighlightLinks] = useState<Set<string>>(new Set());
  const [hoverNode, setHoverNode] = useState<GNode | null>(null);
  const [searchQuery, setSearchQuery] = useState(initialQuery);
  const [searchResults, setSearchResults] = useState<
    Array<{ labels: string[]; properties: Record<string, unknown> }>
  >([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [nodeTypes, setNodeTypes] = useState<Set<string>>(new Set());
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(new Set());
  const [showLabels, setShowLabels] = useState(false);
  const [showLegend, setShowLegend] = useState(true);
  const [is3D, setIs3D] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("graph-view-mode") === "3d";
    }
    return false;
  });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fgRef = useRef<any>(undefined);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });

  // Persist 3D preference
  useEffect(() => {
    localStorage.setItem("graph-view-mode", is3D ? "3d" : "2d");
  }, [is3D]);

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

  // Compute connection counts
  const connectionCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const link of graphData.links) {
      const src = typeof link.source === "object" ? (link.source as unknown as GNode).id : link.source;
      const tgt = typeof link.target === "object" ? (link.target as unknown as GNode).id : link.target;
      counts.set(src, (counts.get(src) || 0) + 1);
      counts.set(tgt, (counts.get(tgt) || 0) + 1);
    }
    return counts;
  }, [graphData.links]);

  // Update node sizes based on connections
  const nodesWithSize = useMemo(() => {
    return graphData.nodes.map((n) => ({
      ...n,
      val: Math.max(2, Math.min(12, (connectionCounts.get(n.id) || 1) * 1.5)),
      connections: connectionCounts.get(n.id) || 0,
    }));
  }, [graphData.nodes, connectionCounts]);

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
            val: 4,
          });
        }

        const links: GLink[] = [];
        for (const rel of data.relationships || []) {
          const src = rel.start || "";
          const tgt = rel.end || "";
          if (src && tgt && nodeMap.has(src) && nodeMap.has(tgt)) {
            links.push({ source: src, target: tgt, type: rel.type, weight: rel.props?.weight as number });
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

  useEffect(() => {
    if (initialQuery && !initialLoading) {
      handleSearch(initialQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialLoading]);

  // Handle node click highlighting
  const handleNodeClick = useCallback(
    (node: unknown) => {
      const n = node as GNode;
      setSelectedNode(n);

      // Highlight this node and its neighbors
      const connectedNodes = new Set<string>([n.id]);
      const connectedLinks = new Set<string>();
      for (const link of graphData.links) {
        const src = typeof link.source === "object" ? (link.source as unknown as GNode).id : link.source;
        const tgt = typeof link.target === "object" ? (link.target as unknown as GNode).id : link.target;
        if (src === n.id || tgt === n.id) {
          connectedNodes.add(src);
          connectedNodes.add(tgt);
          connectedLinks.add(`${src}-${tgt}`);
        }
      }
      setHighlightNodes(connectedNodes);
      setHighlightLinks(connectedLinks);
    },
    [graphData.links]
  );

  const handleBackgroundClick = useCallback(() => {
    setSelectedNode(null);
    setHighlightNodes(new Set());
    setHighlightLinks(new Set());
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
              val: 4,
            });
            existingIds.add(id);
          }

          const newLinks: GLink[] = [];
          for (const rel of data.relationships || []) {
            const src = rel.start || `doc-${rel.props?.source_doc}`;
            const tgt = rel.end || `doc-${rel.props?.source_doc}`;
            if (src && tgt && existingIds.has(src) && existingIds.has(tgt)) {
              newLinks.push({ source: src, target: tgt, type: rel.type, weight: rel.props?.weight as number });
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
        val: 4,
      };
      setGraphData((prev) => ({ ...prev, nodes: [...prev.nodes, node] }));
      setNodeTypes((prev) => new Set([...prev, label]));
    }

    setSearchResults([]);
    setSearchQuery("");
    await addNeighbors(id);
  };

  const filteredData = useMemo(() => {
    const nodes = nodesWithSize.filter((n) => !hiddenTypes.has(n.label));
    const nodeIds = new Set(nodes.map((n) => n.id));
    const links = graphData.links.filter((l) => {
      const srcId = typeof l.source === "object" ? (l.source as unknown as GNode).id : l.source;
      const tgtId = typeof l.target === "object" ? (l.target as unknown as GNode).id : l.target;
      return nodeIds.has(srcId) && nodeIds.has(tgtId);
    });
    return { nodes, links };
  }, [nodesWithSize, graphData.links, hiddenTypes]);

  const handleZoomIn = () => {
    if (is3D) {
      const camera = fgRef.current?.camera();
      if (camera) {
        const dist = camera.position.length();
        camera.position.setLength(dist * 0.7);
      }
    } else {
      fgRef.current?.zoom(fgRef.current.zoom() * 1.3, 300);
    }
  };
  const handleZoomOut = () => {
    if (is3D) {
      const camera = fgRef.current?.camera();
      if (camera) {
        const dist = camera.position.length();
        camera.position.setLength(dist * 1.3);
      }
    } else {
      fgRef.current?.zoom(fgRef.current.zoom() * 0.7, 300);
    }
  };
  const handleZoomReset = () => fgRef.current?.zoomToFit(400, 60);

  const isHighlightActive = highlightNodes.size > 0;

  const getLinkOpacity = useCallback(
    (link: unknown) => {
      if (!isHighlightActive) return 0.3;
      const l = link as GLink;
      const src = typeof l.source === "object" ? (l.source as unknown as GNode).id : l.source;
      const tgt = typeof l.target === "object" ? (l.target as unknown as GNode).id : l.target;
      return highlightLinks.has(`${src}-${tgt}`) || highlightLinks.has(`${tgt}-${src}`) ? 0.8 : 0.04;
    },
    [isHighlightActive, highlightLinks]
  );

  const getNodeOpacity = useCallback(
    (node: unknown) => {
      if (!isHighlightActive) return 1;
      const n = node as GNode;
      return highlightNodes.has(n.id) ? 1 : 0.12;
    },
    [isHighlightActive, highlightNodes]
  );

  // Shared props for both 2D and 3D
  const sharedProps = {
    graphData: filteredData,
    width: dimensions.width,
    height: dimensions.height,
    nodeLabel: (node: unknown) => {
      const n = node as GNode;
      return `<div style="background:rgba(0,0,0,0.85);padding:4px 10px;border-radius:6px;font-size:12px;color:#fff;border:1px solid ${n.color}40">
        <strong style="color:${n.color}">${n.name}</strong><br/>
        <span style="color:#aaa;font-size:10px">${NODE_PALETTE[n.label]?.label || n.label} · ${n.connections || 0} connections</span>
      </div>`;
    },
    linkWidth: (link: unknown) => {
      const l = link as GLink;
      return l.weight ? Math.min(5, Math.max(2, l.weight)) : 2.5;
    },
    linkDirectionalArrowLength: 7,
    linkDirectionalArrowRelPos: 1,
    linkDirectionalParticles: (link: unknown) => {
      if (!isHighlightActive) return 0;
      const l = link as GLink;
      const src = typeof l.source === "object" ? (l.source as unknown as GNode).id : l.source;
      const tgt = typeof l.target === "object" ? (l.target as unknown as GNode).id : l.target;
      return highlightLinks.has(`${src}-${tgt}`) || highlightLinks.has(`${tgt}-${src}`) ? 3 : 0;
    },
    linkDirectionalParticleWidth: 2.5,
    linkDirectionalParticleSpeed: 0.004,
    onNodeClick: handleNodeClick,
    onNodeRightClick: async (node: unknown) => {
      const n = node as GNode;
      await addNeighbors(n.id);
    },
    onBackgroundClick: handleBackgroundClick,
    cooldownTicks: 120,
    d3AlphaDecay: 0.025,
    d3VelocityDecay: 0.3,
    warmupTicks: 50,
  };

  return (
    <div className="relative flex h-full">
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
              {is3D ? (
                <ForceGraph3D
                  ref={fgRef}
                  {...sharedProps}
                  nodeColor={(node: unknown) => {
                    const n = node as GNode;
                    const opacity = getNodeOpacity(n);
                    if (opacity < 0.5) return `${n.color}20`;
                    return n.color;
                  }}
                  nodeVal={(node: unknown) => (node as GNode).val}
                  nodeOpacity={1}
                  linkColor={(link: unknown) => {
                    const opacity = getLinkOpacity(link);
                    const alpha = Math.round(opacity * 255).toString(16).padStart(2, "0");
                    return `#94a3b8${alpha}`;
                  }}
                  linkOpacity={1}
                  linkDirectionalArrowColor={(link: unknown) => {
                    const opacity = getLinkOpacity(link);
                    const alpha = Math.round(opacity * 255).toString(16).padStart(2, "0");
                    return `#94a3b8${alpha}`;
                  }}
                  backgroundColor="rgba(0,0,0,0)"
                  showNavInfo={false}
                />
              ) : (
                <ForceGraph2D
                  ref={fgRef}
                  {...sharedProps}
                  nodeColor={(node: unknown) => (node as GNode).color}
                  nodeVal={(node: unknown) => (node as GNode).val}
                  nodeCanvasObject={(node: unknown, ctx: CanvasRenderingContext2D, globalScale: number) => {
                    const n = node as GNode & { x: number; y: number };
                    const r = Math.sqrt(n.val) * 3;
                    const opacity = getNodeOpacity(n);
                    const isHovered = hoverNode?.id === n.id;
                    const isSelected = selectedNode?.id === n.id;

                    ctx.globalAlpha = opacity;

                    // Glow effect for hovered/selected
                    if (isHovered || isSelected) {
                      ctx.beginPath();
                      ctx.arc(n.x, n.y, r + 6 / globalScale, 0, 2 * Math.PI);
                      const gradient = ctx.createRadialGradient(n.x, n.y, r, n.x, n.y, r + 6 / globalScale);
                      gradient.addColorStop(0, `${n.color}66`);
                      gradient.addColorStop(1, `${n.color}00`);
                      ctx.fillStyle = gradient;
                      ctx.fill();
                    }

                    // Draw circle
                    ctx.beginPath();
                    ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
                    ctx.fillStyle = n.color;
                    ctx.fill();

                    if (isSelected) {
                      ctx.strokeStyle = "#fff";
                      ctx.lineWidth = 2 / globalScale;
                      ctx.stroke();
                    }

                    // Label on hover or if showLabels
                    if ((isHovered || (showLabels && globalScale > 1.2)) && n.name) {
                      const fontSize = Math.max(10 / globalScale, 3);
                      ctx.font = `600 ${fontSize}px Inter, system-ui, sans-serif`;
                      ctx.textAlign = "center";
                      ctx.textBaseline = "top";

                      // Background for label
                      const textWidth = ctx.measureText(n.name).width;
                      const padding = 2 / globalScale;
                      ctx.fillStyle = "rgba(0, 0, 0, 0.7)";
                      ctx.beginPath();
                      const lx = n.x - textWidth / 2 - padding;
                      const ly = n.y + r + 1 / globalScale;
                      const lw = textWidth + padding * 2;
                      const lh = fontSize + padding * 2;
                      ctx.roundRect(lx, ly, lw, lh, 2 / globalScale);
                      ctx.fill();

                      ctx.fillStyle = "#e5e7eb";
                      ctx.fillText(n.name, n.x, n.y + r + 1 / globalScale + padding);
                    }

                    ctx.globalAlpha = 1;
                  }}
                  nodeCanvasObjectMode={() => "replace"}
                  nodePointerAreaPaint={(node: unknown, color: string, ctx: CanvasRenderingContext2D) => {
                    const n = node as GNode & { x: number; y: number };
                    const r = Math.sqrt(n.val) * 3 + 3;
                    ctx.beginPath();
                    ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
                    ctx.fillStyle = color;
                    ctx.fill();
                  }}
                  onNodeHover={(node: unknown) => setHoverNode(node as GNode | null)}
                  linkCanvasObjectMode={() => "after"}
                  linkCanvasObject={(link: unknown, ctx: CanvasRenderingContext2D, globalScale: number) => {
                    const l = link as GLink & { source: { x: number; y: number }; target: { x: number; y: number } };
                    if (!l.type || !l.source?.x || !l.target?.x) return;

                    const opacity = getLinkOpacity(link);
                    if (opacity < 0.1) return;

                    // Only show labels when zoomed in enough
                    if (globalScale < 1.8) return;

                    const midX = (l.source.x + l.target.x) / 2;
                    const midY = (l.source.y + l.target.y) / 2;
                    const fontSize = Math.max(8 / globalScale, 2);

                    ctx.globalAlpha = Math.min(opacity + 0.2, 0.9);
                    ctx.font = `${fontSize}px Inter, system-ui, sans-serif`;
                    ctx.textAlign = "center";
                    ctx.textBaseline = "middle";

                    const text = l.type.replace(/_/g, " ").toLowerCase();
                    const textWidth = ctx.measureText(text).width;
                    const padding = 1.5 / globalScale;

                    ctx.fillStyle = "rgba(0, 0, 0, 0.75)";
                    ctx.beginPath();
                    ctx.roundRect(
                      midX - textWidth / 2 - padding,
                      midY - fontSize / 2 - padding,
                      textWidth + padding * 2,
                      fontSize + padding * 2,
                      2 / globalScale
                    );
                    ctx.fill();

                    ctx.fillStyle = "rgba(148, 163, 184, 0.9)";
                    ctx.fillText(text, midX, midY);
                    ctx.globalAlpha = 1;
                  }}
                  linkColor={(link: unknown) => {
                    const opacity = getLinkOpacity(link);
                    const alpha = Math.round(opacity * 255).toString(16).padStart(2, "0");
                    return `#94a3b8${alpha}`;
                  }}
                  linkDirectionalArrowColor={(link: unknown) => {
                    const opacity = getLinkOpacity(link);
                    const alpha = Math.round(Math.min(opacity + 0.1, 1) * 255).toString(16).padStart(2, "0");
                    return `#94a3b8${alpha}`;
                  }}
                  backgroundColor="transparent"
                />
              )}

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
                      variant={is3D ? "secondary" : "ghost"}
                      size="icon"
                      className="h-7 w-7"
                      onClick={() => setIs3D(!is3D)}
                    >
                      {is3D ? <Box className="h-3.5 w-3.5" /> : <Square className="h-3.5 w-3.5" />}
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>{is3D ? "Switch to 2D" : "Switch to 3D"}</TooltipContent>
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
        <div className="absolute right-0 top-0 bottom-0 w-[600px] max-w-[60vw] border-l flex flex-col bg-card slide-in-right overflow-y-auto z-40 shadow-2xl p-4">
          <NodeDetailPanel
            node={selectedNode}
            onClose={() => {
              setSelectedNode(null);
              setHighlightNodes(new Set());
              setHighlightLinks(new Set());
            }}
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
