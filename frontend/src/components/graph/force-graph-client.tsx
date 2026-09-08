"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ComponentType, RefAttributes } from "react";
import type {
  ForceGraphMethods,
  ForceGraphProps,
  NodeObject,
  LinkObject,
} from "react-force-graph-2d";
import { AlertCircle, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  escapeGraphLabel,
  type ExplorerGraph,
  type ExplorerNode,
  type ExplorerLink,
} from "@/lib/graph-data";
import { getNodeColor } from "./graph-legend";

type RenderNode = NodeObject<ExplorerNode> & { z?: number; fz?: number };
type RenderLink = LinkObject<
  ExplorerNode,
  Omit<ExplorerLink, "source" | "target">
>;
export type GraphHandle = ForceGraphMethods<
  ExplorerNode,
  Omit<ExplorerLink, "source" | "target">
> & {
  cameraPosition?: (
    position?: { x: number; y: number; z: number },
    lookAt?: { x: number; y: number; z: number },
    duration?: number,
  ) => void;
};
type GraphComponent = ComponentType<
  ForceGraphProps<ExplorerNode, Omit<ExplorerLink, "source" | "target">> &
    RefAttributes<GraphHandle>
>;

interface Props {
  graph: ExplorerGraph;
  is3D: boolean;
  showLabels: boolean;
  selectedNodeId: string | null;
  selectedLinkId: string | null;
  onSelectNode: (node: ExplorerNode) => void;
  onSelectLink: (link: ExplorerLink) => void;
  onClear: () => void;
  onReady: (handle: GraphHandle | null) => void;
  onUse2D: () => void;
}

function endpointId(value: RenderLink["source"]): string {
  return typeof value === "object" && value !== null
    ? String(value.id)
    : String(value);
}

export function ForceGraphClient({
  graph,
  is3D,
  showLabels,
  selectedNodeId,
  selectedLinkId,
  onSelectNode,
  onSelectLink,
  onClear,
  onReady,
  onUse2D,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const handleRef = useRef<GraphHandle | null>(null);
  const renderNodes = useRef(new Map<string, RenderNode>());
  const fitted = useRef(false);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  const [renderer, setRenderer] = useState<{
    mode: boolean;
    Component: GraphComponent | null;
    error: string | null;
  }>({ mode: is3D, Component: null, error: null });

  useEffect(() => {
    let active = true;
    fitted.current = false;
    async function load() {
      try {
        if (is3D) {
          const canvas = document.createElement("canvas");
          if (!canvas.getContext("webgl2") && !canvas.getContext("webgl"))
            throw new Error(
              "3D requires WebGL. The 2D graph works without it.",
            );
        }
        const rendererModule = is3D
          ? await import("react-force-graph-3d")
          : await import("react-force-graph-2d");
        if (active)
          setRenderer({
            mode: is3D,
            Component: rendererModule.default as unknown as GraphComponent,
            error: null,
          });
      } catch (error) {
        if (active)
          setRenderer({
            mode: is3D,
            Component: null,
            error:
              error instanceof Error
                ? error.message
                : "Unable to load graph renderer.",
          });
      }
    }
    void load();
    return () => {
      active = false;
    };
  }, [is3D]);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      setDimensions({ width: Math.round(width), height: Math.round(height) });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  // Mutable force coordinates belong only to the renderer, never graph metadata.
  const data = useMemo(() => {
    const nodes = graph.nodes.map((node) => {
      const previous = renderNodes.current.get(node.id);
      const rendered = {
        ...node,
        props: { ...node.props },
        x: previous?.x,
        y: previous?.y,
        z: previous?.z,
        fx: previous?.fx,
        fy: previous?.fy,
        fz: previous?.fz,
      };
      renderNodes.current.set(node.id, rendered);
      return rendered;
    });
    return {
      nodes,
      links: graph.links.map((link) => ({ ...link, props: { ...link.props } })),
    };
  }, [graph]);

  const highlighted = useMemo(() => {
    const ids = new Set<string>();
    if (selectedNodeId) {
      ids.add(selectedNodeId);
      for (const link of graph.links)
        if (link.source === selectedNodeId || link.target === selectedNodeId) {
          ids.add(link.source);
          ids.add(link.target);
        }
    }
    return ids;
  }, [graph.links, selectedNodeId]);

  const setHandle = useCallback(
    (handle: GraphHandle | null) => {
      handleRef.current = handle;
      onReady(handle);
    },
    [onReady],
  );

  const paintNode = useCallback(
    (node: RenderNode, ctx: CanvasRenderingContext2D, scale: number) => {
      if (node.x === undefined || node.y === undefined) return;
      const selected = node.id === selectedNodeId;
      const radius = (node.label === "Document" ? 5 : 6) / scale;
      ctx.save();
      ctx.globalAlpha = selectedNodeId && !highlighted.has(node.id) ? 0.28 : 1;
      ctx.beginPath();
      if (node.label === "Document")
        ctx.rect(node.x - radius, node.y - radius, radius * 2, radius * 2);
      else ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
      ctx.fillStyle = getNodeColor(node.label);
      ctx.fill();
      if (selected) {
        ctx.strokeStyle = "#f8fafc";
        ctx.lineWidth = 2 / scale;
        ctx.stroke();
      }
      if (
        (showLabels && (graph.nodes.length < 80 || scale > 1.4)) ||
        selected
      ) {
        const fontSize = 11 / scale;
        ctx.font = `${fontSize}px system-ui`;
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        const label =
          node.name.length > 42 ? `${node.name.slice(0, 41)}…` : node.name;
        const width = ctx.measureText(label).width;
        ctx.fillStyle = "#101820e8";
        ctx.fillRect(
          node.x - width / 2 - 3 / scale,
          node.y + radius + 2 / scale,
          width + 6 / scale,
          fontSize + 4 / scale,
        );
        ctx.fillStyle = "#e2e8f0";
        ctx.fillText(label, node.x, node.y + radius + 4 / scale);
      }
      ctx.restore();
    },
    [selectedNodeId, highlighted, showLabels, graph.nodes.length],
  );

  const Component = renderer.mode === is3D ? renderer.Component : null;
  const error = renderer.mode === is3D ? renderer.error : null;
  return (
    <div
      ref={containerRef}
      className="relative h-full w-full min-h-0 overflow-hidden bg-[#0b131c]"
      aria-label={`${is3D ? "3D" : "2D"} relationship graph`}
    >
      {error ? (
        <div
          role="alert"
          className="absolute inset-0 flex flex-col items-center justify-center gap-3 p-6 text-center"
        >
          <AlertCircle className="h-6 w-6 text-amber-400" />
          <p>{error}</p>
          {is3D && <Button onClick={onUse2D}>Use 2D graph</Button>}
        </div>
      ) : !Component || !dimensions.width || !dimensions.height ? (
        <div
          role="status"
          className="absolute inset-0 flex items-center justify-center gap-2 text-sm text-muted-foreground"
        >
          <Loader2 className="h-5 w-5 animate-spin" />
          Loading graph renderer…
        </div>
      ) : (
        <Component
          key={is3D ? "3d" : "2d"}
          ref={setHandle}
          graphData={data}
          width={dimensions.width}
          height={dimensions.height}
          backgroundColor="#0b131c"
          nodeVal={(node) => (node.label === "Document" ? 2 : 3)}
          nodeRelSize={4}
          nodeColor={(node) => getNodeColor(node.label)}
          nodeLabel={(node) =>
            `${escapeGraphLabel(node.name)} · ${escapeGraphLabel(node.label)}`
          }
          linkLabel={(link) =>
            `${escapeGraphLabel(link.type)}${link.props.implied ? " · inferred" : ""}`
          }
          linkColor={(link) =>
            link.id === selectedLinkId
              ? "#f8fafc"
              : link.props.implied
                ? "#eab308aa"
                : "#7295adb0"
          }
          linkWidth={(link) => (link.id === selectedLinkId ? 2.5 : 1.2)}
          linkDirectionalArrowLength={4}
          linkDirectionalArrowRelPos={0.95}
          linkCurvature={0.08}
          linkLineDash={(link) => (link.props.implied ? [3, 2] : null)}
          nodeCanvasObject={is3D ? undefined : paintNode}
          nodePointerAreaPaint={
            is3D
              ? undefined
              : (node, color, ctx, scale) => {
                  if (node.x === undefined || node.y === undefined) return;
                  ctx.fillStyle = color;
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, 9 / scale, 0, Math.PI * 2);
                  ctx.fill();
                }
          }
          onNodeClick={(node) => onSelectNode(node)}
          onBackgroundClick={onClear}
          onLinkClick={(link) =>
            onSelectLink({
              ...link,
              source: endpointId(link.source),
              target: endpointId(link.target),
            })
          }
          onNodeDragEnd={(node) => {
            node.fx = node.x;
            node.fy = node.y;
            if (is3D) (node as RenderNode).fz = (node as RenderNode).z;
          }}
          onEngineStop={() => {
            if (!fitted.current && handleRef.current) {
              fitted.current = true;
              handleRef.current.zoomToFit(350, 65);
            }
          }}
          warmupTicks={50}
          cooldownTicks={80}
          d3AlphaDecay={0.06}
          d3VelocityDecay={0.5}
          enableNodeDrag
          minZoom={0.1}
        />
      )}
    </div>
  );
}
