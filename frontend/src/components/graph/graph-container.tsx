'use client';

import { useCallback, useEffect, useMemo, useRef, useState, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import dynamic from 'next/dynamic';
import { Loader2, Maximize2 } from 'lucide-react';
import { useGraphStore } from '@/lib/stores/graph-store';
import { getNodeColor, DEFAULT_NODE_COLOR, NODE_PALETTE } from './graph-legend';
import { GraphControls } from './graph-controls';
import { GraphLegend } from './graph-legend';
import { getGraphInitial, getGraphNeighbors, graphSearch } from '@/lib/api';
import { NodeDetailPanel } from '@/components/node-detail-panel';
import { Sheet, SheetContent } from "@/components/ui/sheet";

const ForceGraphClient = dynamic(
  () => import('./force-graph-client').then((mod) => mod.ForceGraphClient),
  { ssr: false, loading: () => <div className="h-full flex items-center justify-center"><Loader2 className="h-8 w-8 animate-spin text-muted-foreground" /></div> }
);

interface GNode {
  id: string;
  name: string;
  label: string;
  props: Record<string, unknown>;
  color: string;
  val: number;
  connections?: number;
  x?: number;
  y?: number;
  z?: number;
}

interface GLink {
  source: string;
  target: string;
  type: string;
  weight?: number;
}

function lightenColor(hex: string, percent: number): string {
  const num = parseInt(hex.replace('#', ''), 16);
  const r = Math.min(255, (num >> 16) + Math.round(2.55 * percent));
  const g = Math.min(255, ((num >> 8) & 0x00FF) + Math.round(2.55 * percent));
  const b = Math.min(255, (num & 0x0000FF) + Math.round(2.55 * percent));
  return `#${(0x1000000 + r * 0x10000 + g * 0x100 + b).toString(16).slice(1)}`;
}

function darkenColor(hex: string, percent: number): string {
  const num = parseInt(hex.replace('#', ''), 16);
  const r = Math.max(0, (num >> 16) - Math.round(2.55 * percent));
  const g = Math.max(0, ((num >> 8) & 0x00FF) - Math.round(2.55 * percent));
  const b = Math.max(0, (num & 0x0000FF) - Math.round(2.55 * percent));
  return `#${(0x1000000 + r * 0x10000 + g * 0x100 + b).toString(16).slice(1)}`;
}

function GraphContent() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get('q') || '';

  const graphRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const [graphData, setGraphData] = useState<{ nodes: GNode[]; links: GLink[] }>({ nodes: [], links: [] });
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [nodeTypes, setNodeTypes] = useState<Set<string>>(new Set());
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(new Set());
  const [showLabels, setShowLabels] = useState(false);
  const [showLegend, setShowLegend] = useState(true);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [isCssFullscreen, setIsCssFullscreen] = useState(false);
  const [searchResults, setSearchResults] = useState<Array<{ labels: string[]; properties: Record<string, unknown> }>>([]);
  const [nodeLimit, setNodeLimit] = useState(200);

  const { selectedNodeId, hoveredNodeId, is3DMode, selectNode, setHoveredNode, searchQuery, setSearchQuery } = useGraphStore();

  // Connection counts
  const connectionCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const link of graphData.links) {
      const src = typeof link.source === 'object' ? (link.source as unknown as GNode).id : link.source;
      const tgt = typeof link.target === 'object' ? (link.target as unknown as GNode).id : link.target;
      counts.set(src, (counts.get(src) || 0) + 1);
      counts.set(tgt, (counts.get(tgt) || 0) + 1);
    }
    return counts;
  }, [graphData.links]);

  const nodesWithSize = useMemo(() => {
    return graphData.nodes.map((n) => ({
      ...n,
      val: Math.max(2, Math.min(12, (connectionCounts.get(n.id) || 1) * 1.5)),
      connections: connectionCounts.get(n.id) || 0,
    }));
  }, [graphData.nodes, connectionCounts]);

  // Filter by hidden types
  const filteredData = useMemo(() => {
    const nodes = nodesWithSize.filter((n) => !hiddenTypes.has(n.label));
    const nodeIds = new Set(nodes.map((n) => n.id));
    const links = graphData.links.filter((l) => {
      const srcId = typeof l.source === 'object' ? (l.source as unknown as GNode).id : l.source;
      const tgtId = typeof l.target === 'object' ? (l.target as unknown as GNode).id : l.target;
      return nodeIds.has(srcId) && nodeIds.has(tgtId);
    });
    return { nodes, links };
  }, [nodesWithSize, graphData.links, hiddenTypes]);

  // Highlight sets
  const highlightNodes = useMemo(() => {
    const nodeId = hoveredNodeId || selectedNodeId;
    if (!nodeId) return new Set<string>();
    const connected = new Set<string>([nodeId]);
    graphData.links.forEach((link) => {
      const src = typeof link.source === 'object' ? (link.source as unknown as GNode).id : link.source;
      const tgt = typeof link.target === 'object' ? (link.target as unknown as GNode).id : link.target;
      if (src === nodeId) connected.add(tgt);
      if (tgt === nodeId) connected.add(src);
    });
    return connected;
  }, [hoveredNodeId, selectedNodeId, graphData.links]);

  const highlightLinks = useMemo(() => {
    const nodeId = hoveredNodeId || selectedNodeId;
    if (!nodeId) return new Set<string>();
    const connected = new Set<string>();
    graphData.links.forEach((link) => {
      const src = typeof link.source === 'object' ? (link.source as unknown as GNode).id : link.source;
      const tgt = typeof link.target === 'object' ? (link.target as unknown as GNode).id : link.target;
      if (src === nodeId || tgt === nodeId) {
        connected.add(`${src}-${tgt}`);
        connected.add(`${tgt}-${src}`);
      }
    });
    return connected;
  }, [hoveredNodeId, selectedNodeId, graphData.links]);

  const isHighlightActive = highlightNodes.size > 0;

  // Load initial graph
  useEffect(() => {
    const loadInitial = async () => {
      setInitialLoading(true);
      try {
        const data = await getGraphInitial(nodeLimit);
        const nodeMap = new Map<string, GNode>();
        const types = new Set<string>();
        for (const node of data.nodes || []) {
          const p = node.props || node.properties || {};
          const id = (p.uuid as string) || `doc-${p.paperless_id}` || (p.name as string);
          if (!id || nodeMap.has(id)) continue;
          const label = node.labels?.[0] || 'Unknown';
          types.add(label);
          nodeMap.set(id, {
            id, name: (p.name as string) || (p.title as string) || id,
            label, props: p, color: getNodeColor(label), val: 4,
          });
        }
        const links: GLink[] = [];
        for (const rel of data.relationships || []) {
          const src = rel.start || '';
          const tgt = rel.end || '';
          if (src && tgt && nodeMap.has(src) && nodeMap.has(tgt)) {
            links.push({ source: src, target: tgt, type: rel.type, weight: rel.props?.weight as number });
          }
        }
        setGraphData({ nodes: Array.from(nodeMap.values()), links });
        setNodeTypes(types);
      } catch (e) {
        console.error('Failed to load initial graph:', e);
      } finally {
        setInitialLoading(false);
      }
    };
    loadInitial();
  }, [nodeLimit]);

  // Auto-search from URL param
  useEffect(() => {
    if (initialQuery && !initialLoading) handleSearch(initialQuery);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialLoading]);

  // Add neighbors
  const addNeighbors = useCallback(async (nodeId: string) => {
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
          const label = node.labels?.[0] || 'Unknown';
          types.add(label);
          newNodes.push({ id, name: (p.name as string) || (p.title as string) || id, label, props: p, color: getNodeColor(label), val: 4 });
          existingIds.add(id);
        }
        const newLinks: GLink[] = [];
        for (const rel of data.relationships || []) {
          const src = rel.start || '';
          const tgt = rel.end || '';
          if (src && tgt && existingIds.has(src) && existingIds.has(tgt)) {
            newLinks.push({ source: src, target: tgt, type: rel.type, weight: rel.props?.weight as number });
          }
        }
        setNodeTypes(types);
        return { nodes: [...prev.nodes, ...newNodes], links: [...prev.links, ...newLinks] };
      });
    } catch (e) {
      console.error('Failed to load neighbors:', e);
    } finally {
      setLoading(false);
    }
  }, [nodeTypes]);

  // Search
  const handleSearch = async (query: string) => {
    if (!query.trim()) return;
    setLoading(true);
    try {
      const data = await graphSearch(query);
      setSearchResults(data.results || []);
    } catch (e) {
      console.error('Search failed:', e);
    } finally {
      setLoading(false);
    }
  };

  const addNodeFromSearch = async (result: { labels: string[]; properties: Record<string, unknown> }) => {
    const p = result.properties;
    const id = (p.uuid as string) || `doc-${p.paperless_id}`;
    if (!id) return;
    const label = result.labels?.[0] || 'Unknown';
    const exists = graphData.nodes.find((n) => n.id === id);
    if (!exists) {
      const node: GNode = { id, name: (p.name as string) || (p.title as string) || id, label, props: p, color: getNodeColor(label), val: 4 };
      setGraphData((prev) => ({ ...prev, nodes: [...prev.nodes, node] }));
      setNodeTypes((prev) => new Set([...prev, label]));
    }
    setSearchResults([]);
    setSearchQuery('');
    await addNeighbors(id);
  };

  // Node click with camera focus
  const handleNodeClick = useCallback((node: any) => {
    const n = node as GNode;
    selectNode(n.id);
    if (graphRef.current && n.x !== undefined && n.y !== undefined) {
      if (is3DMode) {
        graphRef.current.cameraPosition(
          { x: n.x, y: n.y, z: (n.z || 0) + 200 },
          { x: n.x, y: n.y, z: n.z || 0 },
          1000
        );
      } else {
        graphRef.current.centerAt(n.x, n.y, 1000);
        graphRef.current.zoom(3, 1000);
      }
    }
  }, [selectNode, is3DMode]);

  const handleNodeHover = useCallback((node: any) => {
    setHoveredNode(node?.id || null);
  }, [setHoveredNode]);

  const handleBackgroundClick = useCallback(() => {
    selectNode(null);
  }, [selectNode]);

  // Force configuration
  const handleEngineInit = useCallback((fg: any) => {
    graphRef.current = fg;
    fg.d3Force('charge')?.strength(-2000);
    fg.d3Force('link')?.distance(300);
    fg.d3Force('center')?.strength(0.005);
    import('d3-force').then(({ forceCollide }) => {
      fg.d3Force('collision', forceCollide(30));
      fg.d3ReheatSimulation();
    });
    fg.d3ReheatSimulation();
    setTimeout(() => {
      fg.refresh?.();
      setTimeout(() => fg.zoomToFit(2000, 80), 500);
    }, 200);
  }, []);

  // Zoom
  const handleZoomIn = () => {
    if (is3DMode) {
      const camera = graphRef.current?.camera();
      if (camera) camera.position.setLength(camera.position.length() * 0.7);
    } else {
      graphRef.current?.zoom(graphRef.current.zoom() * 1.3, 300);
    }
  };
  const handleZoomOut = () => {
    if (is3DMode) {
      const camera = graphRef.current?.camera();
      if (camera) camera.position.setLength(camera.position.length() * 1.3);
    } else {
      graphRef.current?.zoom(graphRef.current.zoom() * 0.7, 300);
    }
  };
  const handleResetView = () => graphRef.current?.zoomToFit(400, 60);

  // Fullscreen
  const toggleFullscreen = useCallback(async () => {
    const elem = containerRef.current;
    if (!elem) return;
    const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);
    const fsAvailable = document.fullscreenEnabled || (document as any).webkitFullscreenEnabled;
    if (!isMobile && fsAvailable) {
      if (!document.fullscreenElement) {
        try { await (elem.requestFullscreen?.() || (elem as any).webkitRequestFullscreen?.()); setIsFullscreen(true); } catch {}
      } else {
        try { await (document.exitFullscreen?.() || (document as any).webkitExitFullscreen?.()); setIsFullscreen(false); } catch {}
      }
    } else {
      setIsFullscreen(p => !p);
      setIsCssFullscreen(p => !p);
    }
  }, []);

  // CSS fullscreen body lock
  useEffect(() => {
    if (isCssFullscreen) {
      document.body.style.overflow = 'hidden';
      document.body.style.position = 'fixed';
      document.body.style.width = '100%';
    } else {
      document.body.style.overflow = '';
      document.body.style.position = '';
      document.body.style.width = '';
    }
    return () => { document.body.style.overflow = ''; document.body.style.position = ''; document.body.style.width = ''; };
  }, [isCssFullscreen]);

  useEffect(() => {
    const handler = () => {
      const fs = !!(document.fullscreenElement || (document as any).webkitFullscreenElement);
      setIsFullscreen(fs);
      if (!fs) setIsCssFullscreen(false);
    };
    document.addEventListener('fullscreenchange', handler);
    document.addEventListener('webkitfullscreenchange', handler);
    return () => { document.removeEventListener('fullscreenchange', handler); document.removeEventListener('webkitfullscreenchange', handler); };
  }, []);

  // Custom 2D node rendering with radial gradient
  const nodeCanvasObject = useCallback((node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const n = node as GNode & { x: number; y: number };
    const r = Math.sqrt(n.val || 4) * 3;
    const isHigh = highlightNodes.has(n.id);
    const opacity = isHighlightActive ? (isHigh ? 1 : 0.12) : 1;
    const isHovered = hoveredNodeId === n.id;
    const isSelected = selectedNodeId === n.id;

    ctx.globalAlpha = opacity;

    // Glow for hovered/selected
    if (isHovered || isSelected) {
      ctx.beginPath();
      ctx.arc(n.x, n.y, r + 6 / globalScale, 0, 2 * Math.PI);
      const glow = ctx.createRadialGradient(n.x, n.y, r, n.x, n.y, r + 6 / globalScale);
      glow.addColorStop(0, `${n.color}66`);
      glow.addColorStop(1, `${n.color}00`);
      ctx.fillStyle = glow;
      ctx.fill();
    }

    // Radial gradient fill
    const grad = ctx.createRadialGradient(n.x - r * 0.3, n.y - r * 0.3, 0, n.x, n.y, r);
    grad.addColorStop(0, lightenColor(n.color, 30));
    grad.addColorStop(0.5, n.color);
    grad.addColorStop(1, darkenColor(n.color, 20));

    ctx.beginPath();
    ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
    ctx.fillStyle = grad;
    ctx.fill();

    // White border
    ctx.strokeStyle = 'rgba(255,255,255,0.15)';
    ctx.lineWidth = 1 / globalScale;
    ctx.stroke();

    if (isSelected) {
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 2 / globalScale;
      ctx.stroke();
    }

    // Label on hover or if showLabels
    if ((isHovered || (showLabels && globalScale > 1.2)) && n.name) {
      const fontSize = Math.max(10 / globalScale, 3);
      ctx.font = `600 ${fontSize}px Inter, system-ui, sans-serif`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      const tw = ctx.measureText(n.name).width;
      const pad = 2 / globalScale;
      ctx.fillStyle = 'rgba(0,0,0,0.7)';
      ctx.beginPath();
      ctx.roundRect(n.x - tw / 2 - pad, n.y + r + 1 / globalScale, tw + pad * 2, fontSize + pad * 2, 2 / globalScale);
      ctx.fill();
      ctx.fillStyle = '#e5e7eb';
      ctx.fillText(n.name, n.x, n.y + r + 1 / globalScale + pad);
    }

    ctx.globalAlpha = 1;
  }, [highlightNodes, isHighlightActive, hoveredNodeId, selectedNodeId, showLabels]);

  const nodePointerAreaPaint = useCallback((node: any, color: string, ctx: CanvasRenderingContext2D) => {
    const n = node as GNode & { x: number; y: number };
    const r = Math.sqrt(n.val || 4) * 3 + 3;
    ctx.beginPath();
    ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
  }, []);

  // Link canvas for relationship labels
  const linkCanvasObject = useCallback((link: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const l = link as GLink & { source: { x: number; y: number }; target: { x: number; y: number } };
    if (!l.type || !l.source?.x || !l.target?.x) return;
    const src = typeof l.source === 'object' ? (l.source as unknown as GNode).id : l.source;
    const tgt = typeof l.target === 'object' ? (l.target as unknown as GNode).id : l.target;
    const linkKey = `${src}-${tgt}`;
    const opacity = isHighlightActive ? (highlightLinks.has(linkKey) ? 0.8 : 0.04) : 0.3;
    if (opacity < 0.1 || globalScale < 1.8) return;
    const midX = (l.source.x + l.target.x) / 2;
    const midY = (l.source.y + l.target.y) / 2;
    const fontSize = Math.max(8 / globalScale, 2);
    ctx.globalAlpha = Math.min(opacity + 0.2, 0.9);
    ctx.font = `${fontSize}px Inter, system-ui, sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    const text = l.type.replace(/_/g, ' ').toLowerCase();
    const tw = ctx.measureText(text).width;
    const pad = 1.5 / globalScale;
    ctx.fillStyle = 'rgba(0,0,0,0.75)';
    ctx.beginPath();
    ctx.roundRect(midX - tw / 2 - pad, midY - fontSize / 2 - pad, tw + pad * 2, fontSize + pad * 2, 2 / globalScale);
    ctx.fill();
    ctx.fillStyle = 'rgba(148,163,184,0.9)';
    ctx.fillText(text, midX, midY);
    ctx.globalAlpha = 1;
  }, [isHighlightActive, highlightLinks]);

  const getLinkColor = useCallback((link: any) => {
    const l = link as GLink;
    const src = typeof l.source === 'object' ? (l.source as unknown as GNode).id : l.source;
    const tgt = typeof l.target === 'object' ? (l.target as unknown as GNode).id : l.target;
    const linkKey = `${src}-${tgt}`;
    const opacity = isHighlightActive ? (highlightLinks.has(linkKey) ? 0.8 : 0.04) : 0.3;
    const alpha = Math.round(opacity * 255).toString(16).padStart(2, '0');
    return `#94a3b8${alpha}`;
  }, [isHighlightActive, highlightLinks]);

  const getLinkWidth = useCallback((link: any) => {
    const l = link as GLink;
    const src = typeof l.source === 'object' ? (l.source as unknown as GNode).id : l.source;
    const tgt = typeof l.target === 'object' ? (l.target as unknown as GNode).id : l.target;
    if (isHighlightActive && highlightLinks.has(`${src}-${tgt}`)) return 2;
    return l.weight ? Math.min(5, Math.max(1, l.weight)) : 1;
  }, [isHighlightActive, highlightLinks]);

  const selectedNode = graphData.nodes.find((n) => n.id === selectedNodeId) || null;

  if (initialLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center space-y-3">
          <Loader2 className="h-10 w-10 mx-auto animate-spin text-primary/40" />
          <p className="text-sm text-muted-foreground">Loading knowledge graph...</p>
        </div>
      </div>
    );
  }

  if (graphData.nodes.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        <div className="text-center space-y-3">
          <Maximize2 className="h-10 w-10 mx-auto opacity-20" />
          <p className="text-sm">No graph data. Try syncing documents first.</p>
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={`h-full relative ${isCssFullscreen ? 'fixed inset-0 z-50 w-screen h-screen overflow-hidden' : ''}`}
      style={{ backgroundColor: '#0a0a0a' }}
    >
      <div className="absolute inset-0 pt-12">
        <ForceGraphClient
          key={`graph-${is3DMode ? '3d' : '2d'}`}
          fgRef={graphRef}
          is3DMode={is3DMode}
          graphData={filteredData}
          nodeLabel={(node: any) =>
            `<div style="background:rgba(0,0,0,0.9);padding:6px 10px;border-radius:6px;font-size:12px;color:#fff;border:1px solid ${node.color}40;max-width:300px;">
              <strong style="color:${node.color}">${node.name}</strong><br/>
              <span style="color:#aaa;font-size:10px">${NODE_PALETTE[node.label]?.label || node.label} · ${node.connections || 0} connections</span>
              ${node.props?.description ? `<br/><span style="color:#888;font-size:10px">${(node.props.description as string).substring(0, 100)}${(node.props.description as string).length > 100 ? '...' : ''}</span>` : ''}
            </div>`
          }
          linkLabel={(link: any) =>
            `<div style="padding:4px 8px;background:rgba(0,0,0,0.9);border-radius:4px;font-size:11px;color:#fff;border:1px solid rgba(255,255,255,0.1);">
              <strong>${link.type?.replace(/_/g, ' ') || 'RELATED'}</strong>
            </div>`
          }
          nodeColor={(node: any) => {
            if (!isHighlightActive) return node.color;
            return highlightNodes.has(node.id) ? node.color : `${node.color}20`;
          }}
          nodeVal={(node: any) => node.val}
          nodeRelSize={6}
          nodeCanvasObject={nodeCanvasObject}
          nodeCanvasObjectMode={() => 'replace'}
          nodePointerAreaPaint={nodePointerAreaPaint}
          linkCanvasObject={linkCanvasObject}
          linkCanvasObjectMode={() => 'after'}
          linkColor={getLinkColor}
          linkWidth={getLinkWidth}
          linkDirectionalArrowLength={7}
          linkDirectionalArrowRelPos={1}
          linkDirectionalArrowColor={getLinkColor}
          linkCurvature={0.12}
          linkDirectionalParticles={(link: any) => {
            if (!isHighlightActive) return 0;
            const src = typeof link.source === 'object' ? link.source.id : link.source;
            const tgt = typeof link.target === 'object' ? link.target.id : link.target;
            return highlightLinks.has(`${src}-${tgt}`) ? 4 : 0;
          }}
          linkDirectionalParticleSpeed={0.003}
          linkDirectionalParticleWidth={3}
          linkDirectionalParticleColor={(link: any) => {
            const sourceColor = typeof link.source === 'object' ? link.source.color : null;
            return sourceColor || '#ffffff';
          }}
          onNodeClick={handleNodeClick}
          onNodeHover={handleNodeHover}
          onNodeRightClick={async (node: any) => await addNeighbors(node.id)}
          onBackgroundClick={handleBackgroundClick}
          warmupTicks={200}
          cooldownTicks={100}
          d3AlphaDecay={0.01}
          d3VelocityDecay={0.3}
          onEngineInit={handleEngineInit}
          backgroundColor="#000000"
          nodeOpacity={1}
          linkOpacity={0.8}
          nodeResolution={16}
          enableNodeDrag={true}
          enableNavigationControls={true}
          showNavInfo={false}
        />
      </div>

      <GraphControls
        nodeCount={filteredData.nodes.length}
        linkCount={filteredData.links.length}
        nodeTypes={nodeTypes}
        hiddenTypes={hiddenTypes}
        onToggleType={(type) => setHiddenTypes((prev) => { const next = new Set(prev); if (next.has(type)) next.delete(type); else next.add(type); return next; })}
        onSearch={handleSearch}
        searchResults={searchResults}
        onAddFromSearch={addNodeFromSearch}
        onClearSearch={() => setSearchResults([])}
        loading={loading}
        isFullscreen={isFullscreen}
        onToggleFullscreen={toggleFullscreen}
        onResetView={handleResetView}
        showLabels={showLabels}
        onToggleLabels={() => setShowLabels(p => !p)}
        showLegend={showLegend}
        onToggleLegend={() => setShowLegend(p => !p)}
        onZoomIn={handleZoomIn}
        onZoomOut={handleZoomOut}
        nodeLimit={nodeLimit}
        onNodeLimitChange={setNodeLimit}
      />

      {loading && (
        <div className="absolute top-14 right-3 z-20 bg-card/90 backdrop-blur-md border rounded-lg px-3 py-2 shadow-lg flex items-center gap-2">
          <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
          <span className="text-xs">Loading...</span>
        </div>
      )}

      {/* Node detail panel - desktop sidebar, mobile bottom sheet */}
      {selectedNode && (
        <div className="absolute right-0 top-0 bottom-0 w-[600px] max-w-[60vw] border-l flex-col bg-card overflow-y-auto z-40 shadow-2xl p-4 hidden md:flex">
          <NodeDetailPanel
            node={selectedNode}
            onClose={() => selectNode(null)}
            onExpandNeighbors={addNeighbors}
          />
        </div>
      )}

      {/* Mobile node detail bottom sheet */}
      <Sheet open={!!selectedNode} onOpenChange={(open) => { if (!open) selectNode(null); }}>
        <SheetContent side="bottom" className="md:hidden h-[70vh] overflow-y-auto p-0" showCloseButton={false}>
          {selectedNode && (
            <NodeDetailPanel
              node={selectedNode}
              onClose={() => selectNode(null)}
              onExpandNeighbors={addNeighbors}
            />
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}

export function GraphContainer() {
  return (
    <Suspense fallback={<div className="flex h-full items-center justify-center"><Loader2 className="h-8 w-8 animate-spin text-primary/40" /></div>}>
      <GraphContent />
    </Suspense>
  );
}
