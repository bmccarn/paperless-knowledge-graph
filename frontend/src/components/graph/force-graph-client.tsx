"use client";

import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { ComponentType, RefAttributes } from "react";
import type {
  ForceGraphMethods,
  ForceGraphProps,
  NodeObject,
  LinkObject,
} from "react-force-graph-2d";
import { AlertCircle, Loader2 } from "lucide-react";
import {
  escapeGraphLabel,
  type ExplorerGraph,
  type ExplorerNode,
  type ExplorerLink,
} from "@/lib/graph-data";
import { nodeLabelText } from "@/lib/categories";
import {
  fitView,
  LABEL_ALL,
  labelCandidates,
  linkWidth,
  nodeLabel,
  nodeRadius,
  placeLabels,
  shapeExtent,
  type Box,
  type LabelRule,
} from "@/lib/graph-encoding";
import { paintShape, readColors, type Colors } from "./canvas-paint";

type RenderNode = NodeObject<ExplorerNode>;
type RenderLink = LinkObject<
  ExplorerNode,
  Omit<ExplorerLink, "source" | "target">
>;
export type GraphHandle = ForceGraphMethods<
  ExplorerNode,
  Omit<ExplorerLink, "source" | "target">
>;
/** The camera controls for the zoom buttons. Moves are instant under reduced motion. */
export interface Camera { zoomBy: (factor: number) => void; fit: () => void }
type GraphComponent = ComponentType<
  ForceGraphProps<ExplorerNode, Omit<ExplorerLink, "source" | "target">> &
    RefAttributes<GraphHandle>
>;

interface Props {
  graph: ExplorerGraph;
  showLabels: boolean;
  selectedNodeId: string | null;
  selectedLinkId: string | null;
  onSelectNode: (node: ExplorerNode, event?: MouseEvent) => void;
  onSelectLink: (link: ExplorerLink) => void;
  onClear: () => void;
  onReady: (camera: Camera | null) => void;
  /** The canvas's accessible name, which summarizes it. */
  name: string;
  /** Below 768 px: overlapping labels are skipped, whatever the node count, and a pill shows only its "+N". */
  phone?: boolean;
  /** Fixed positions: no simulation, no node drag, one fit on the first draw. */
  positions?: ReadonlyMap<string, { x: number; y: number }>;
  /** The focus entity: labeled in 500 weight; other labels sit on the side away from it. */
  focusId?: string | null;
  /** The node whose inspector row has keyboard focus. */
  keyboardNodeId?: string | null;
  /** Links the hovered or focused evidence chip supports, marked like a selected link. */
  markedLinkIds?: ReadonlySet<string>;
}

const LABEL_FONT = 12;
const PILL_HEIGHT = 28;
const HOVER_DELAY = 120;
const docs = (count: number) => `${count} ${count === 1 ? "doc" : "docs"}`;
const nodeDocs = (node: ExplorerNode) => Number(node.props.evidence_doc_count) || 0;
const linkDocs = (link: { props: Record<string, unknown> }) =>
  Array.isArray(link.props.source_doc_ids) ? link.props.source_doc_ids.length : 1;
const ringed = (node: ExplorerNode) => node.props.is_owner === true || node.props.is_hub === true || Number(node.props.link_count) > 100;
/** The full name for the tooltip; the kind and a short id when the node has none. */
const fullName = (node: ExplorerNode) => (node.name && node.name !== node.id ? node.name : nodeLabel("", node.id, node.label));

function endpointId(value: RenderLink["source"]): string {
  return typeof value === "object" && value !== null
    ? String(value.id)
    : String(value);
}

export function ForceGraphClient({
  graph,
  showLabels,
  selectedNodeId,
  selectedLinkId,
  onSelectNode,
  onSelectLink,
  onClear,
  onReady,
  positions,
  focusId = null,
  keyboardNodeId = null,
  markedLinkIds,
  name,
  phone = false,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const handleRef = useRef<GraphHandle | null>(null);
  const renderNodes = useRef(new Map<string, RenderNode>());
  const initialFitPending = useRef(true);
  /** The zoom a running camera move ends at, so a second press builds on it. */
  const zoomTarget = useRef({ k: 0, until: 0 });
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [paint, setPaint] = useState<{ colors: Colors; font: string } | null>(null);
  const [renderer, setRenderer] = useState<{
    Component: GraphComponent | null;
    error: string | null;
  }>({ Component: null, error: null });

  useEffect(() => {
    let active = true;
    initialFitPending.current = true;
    async function load() {
      try {
        const rendererModule = await import("react-force-graph-2d");
        if (active)
          setRenderer({
            Component: rendererModule.default as unknown as GraphComponent,
            error: null,
          });
      } catch (error) {
        if (active)
          setRenderer({
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
  }, []);

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

  // Read the tokens now, at each theme change and once the label font has loaded; a new value repaints.
  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    let active = true;
    const read = () => { if (active) setPaint({ colors: readColors(element), font: getComputedStyle(element).fontFamily }); };
    read();
    const family = getComputedStyle(element).fontFamily;
    void Promise.all([400, 500].map(weight => document.fonts.load(`${weight} ${LABEL_FONT}px ${family}`))).then(read, read);
    const observer = new MutationObserver(read);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "style"] });
    return () => {
      active = false;
      observer.disconnect();
    };
  }, []);

  useEffect(() => () => clearTimeout(hoverTimer.current), []);

  // Mutable force coordinates belong only to the renderer, never graph metadata.
  const data = useMemo(() => {
    const nodes: RenderNode[] = graph.nodes.map((node) => {
      const at = positions?.get(node.id);
      return { ...node, props: { ...node.props }, ...(at && { x: at.x, y: at.y, fx: at.x, fy: at.y }) };
    });
    // A link's own curvature (a link between neighbors bows toward the ring); parallel links between one pair add +/-0.15.
    const pairs = new Map<string, number>();
    const pair = (link: ExplorerLink) => [link.source, link.target].sort().join("\n");
    for (const link of graph.links) pairs.set(pair(link), (pairs.get(pair(link)) ?? 0) + 1);
    const seen = new Map<string, number>();
    return {
      nodes,
      links: graph.links.map((link) => {
        const index = seen.get(pair(link)) ?? 0;
        seen.set(pair(link), index + 1);
        const curvature = (Number(link.props.curvature) || 0)
          + ((pairs.get(pair(link)) ?? 1) > 1 ? 0.15 * (index % 2 ? -1 : 1) * (link.source < link.target ? 1 : -1) : 0);
        return { ...link, props: { ...link.props }, curvature };
      }),
    };
  }, [graph, positions]);

  useLayoutEffect(() => {
    for (const node of data.nodes) {
      const previous = renderNodes.current.get(node.id);
      // Fixed layouts keep their own positions; the cache still serves selection centering.
      if (!positions) Object.assign(node, { x: previous?.x, y: previous?.y, fx: previous?.fx, fy: previous?.fy });
      renderNodes.current.set(node.id, node);
    }
  }, [data, positions]);

  const font = useCallback((weight: number) => `${weight} ${LABEL_FONT}px ${paint?.font}`, [paint]);

  // A 390 px canvas has no room for nine full pills: on phone a pill shows "+N", and selecting it names the family.
  const pillText = useCallback((node: ExplorerNode) => (phone ? node.name.split(" ")[0] : node.name), [phone]);
  /** A "+N Family" pill box around (0, 0) in screen pixels. */
  const pillWidth = useCallback((ctx: CanvasRenderingContext2D, node: ExplorerNode) => {
    ctx.font = font(400);
    return ctx.measureText(pillText(node)).width + 24;
  }, [font, pillText]);

  /** Center and zoom so every node and pill, at its screen size, sits inside the canvas. */
  const fit = useCallback((duration: number) => {
    const handle = handleRef.current;
    const ctx = document.createElement("canvas").getContext("2d");
    if (!handle || !ctx || !dimensions.width || !dimensions.height) return;
    const items = data.nodes.filter(node => node.x !== undefined && node.y !== undefined).map(node => {
      const [halfWidth, halfHeight] = node.label === "Aggregate"
        ? [pillWidth(ctx, node) / 2, PILL_HEIGHT / 2]
        : Array(2).fill(shapeExtent(node.label, nodeRadius(nodeDocs(node))) + 3);
      return { x: node.x!, y: node.y!, halfWidth, halfHeight };
    });
    if (!items.length) return;
    // Room for the controls over the canvas: on phone the Legend button (top) and zoom buttons (bottom) in the
    // height a width-bound ring leaves spare; on desktop the zoom buttons at the right, where a wide canvas has room.
    const pad = phone ? { top: 64, right: 16, bottom: 152, left: 16 } : { top: 48, right: 64, bottom: 48, left: 48 };
    const roomy = fitView(items, dimensions.width, dimensions.height, pad);
    // A short phone canvas (the inspector open) has no spare height: fit to its edges instead of shrinking the zoom.
    const tight = phone ? fitView(items, dimensions.width, dimensions.height, { top: 16, right: 16, bottom: 16, left: 16 }) : roomy;
    const view = roomy.k < tight.k ? tight : roomy;
    handle.centerAt(view.x, view.y, duration);
    handle.zoom(view.k, duration);
    zoomTarget.current = { k: view.k, until: performance.now() + duration };
  }, [data, dimensions, phone, pillWidth]);

  // Fixed layouts have no engine-stop event: fit once, when the first layout is drawn, after the label
  // font has loaded, so pill widths and the fitted zoom are the same on every load.
  const fixed = Boolean(positions);
  useEffect(() => {
    if (!fixed || !renderer.Component || !dimensions.width || !dimensions.height || !paint) return;
    let frame = 0;
    const run = () => {
      frame = requestAnimationFrame(() => {
        if (!initialFitPending.current || !handleRef.current) return;
        initialFitPending.current = false;
        fit(0);
      });
    };
    // A face in the stack that cannot load rejects; the fit then runs with the faces that did.
    void document.fonts.load(font(400)).then(run, run);
    return () => cancelAnimationFrame(frame);
  }, [fixed, renderer.Component, dimensions.width, dimensions.height, paint, font, fit]);

  useEffect(() => {
    if (!renderer.Component || !selectedNodeId ||
        dimensions.width <= 0 || dimensions.height <= 0 ||
        !graph.nodes.some((node) => node.id === selectedNodeId)) return;
    // The inspector and responsive layout resize the canvas. Center after its
    // new dimensions are applied, retaining zoom and renderer-owned positions.
    const frame = requestAnimationFrame(() => {
      const node = renderNodes.current.get(selectedNodeId);
      if (!node || typeof node.x !== "number" || typeof node.y !== "number" ||
          !Number.isFinite(node.x) || !Number.isFinite(node.y)) return;
      handleRef.current?.centerAt(node.x, node.y, 0);
    });
    return () => cancelAnimationFrame(frame);
  }, [selectedNodeId, dimensions.width, dimensions.height, renderer.Component, graph.nodes]);

  const setHandle = useCallback((handle: GraphHandle | null) => { handleRef.current = handle; }, []);
  useEffect(() => {
    const duration = () => (matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 300);
    // A button press takes the camera, as a gesture does.
    const take = () => { initialFitPending.current = false; };
    onReady({
      zoomBy: factor => {
        take();
        const handle = handleRef.current;
        if (!handle) return;
        const ms = duration(), from = performance.now() < zoomTarget.current.until ? zoomTarget.current.k : handle.zoom();
        zoomTarget.current = { k: from * factor, until: performance.now() + ms };
        handle.zoom(from * factor, ms);
      },
      fit: () => { take(); fit(duration()); },
    });
    return () => onReady(null);
  }, [onReady, fit]);

  // A selected pill does not fade the other nodes and links; a selected entity does.
  const entitySelected = Boolean(selectedNodeId) && !graph.nodes.some(node => node.id === selectedNodeId && node.label === "Aggregate");

  // Every shape is drawn in screen pixels around its node's center, so it keeps its size at every zoom.
  const paintNode = useCallback(
    (node: RenderNode, ctx: CanvasRenderingContext2D, scale: number) => {
      if (node.x === undefined || node.y === undefined || !paint) return;
      const { colors } = paint;
      ctx.save();
      ctx.translate(node.x, node.y);
      ctx.scale(1 / scale, 1 / scale);
      if (node.label === "Aggregate") {
        const width = pillWidth(ctx, node);
        const pill = new Path2D();
        pill.roundRect(-width / 2 + 0.5, -PILL_HEIGHT / 2 + 0.5, width - 1, PILL_HEIGHT - 1, (PILL_HEIGHT - 1) / 2);
        const selected = node.id === selectedNodeId;
        if (selected) {
          // A selected pill takes the node selection: a 10 px --mark halo and a --fg ring.
          ctx.strokeStyle = colors.mark;
          ctx.lineWidth = 20;
          ctx.stroke(pill);
        }
        ctx.fillStyle = colors.surface;
        ctx.fill(pill);
        ctx.strokeStyle = selected ? colors.fg : colors["border-strong"];
        ctx.lineWidth = selected ? 2 : 1;
        ctx.stroke(pill);
        ctx.fillStyle = colors.fg;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(pillText(node), 0, 0.5);
        ctx.restore();
        return;
      }
      const selected = node.id === selectedNodeId;
      paintShape(ctx, node.label, nodeRadius(nodeDocs(node)), colors, { selected, faded: entitySelected && !selected,
        hovered: node.id === hoveredId, keyboard: node.id === keyboardNodeId, ringed: ringed(node) });
      ctx.restore();
    },
    [paint, pillWidth, pillText, selectedNodeId, entitySelected, hoveredId, keyboardNodeId],
  );

  const selectedLink = (link: RenderLink) => link.id === selectedLinkId || Boolean(markedLinkIds?.has(link.id));
  const nodeLinked = useCallback((link: RenderLink) =>
    Boolean(selectedNodeId) && (endpointId(link.source) === selectedNodeId || endpointId(link.target) === selectedNodeId), [selectedNodeId]);

  /** A point and direction along a link, straight or curved, at t from its source. */
  const along = (link: RenderLink, t: number) => {
    const start = link.source as RenderNode, end = link.target as RenderNode;
    const control = (link as { __controlPoints?: number[] }).__controlPoints;
    if (control?.length === 2) {
      const [cx, cy] = control;
      const u = 1 - t;
      return {
        x: u * u * start.x! + 2 * u * t * cx + t * t * end.x!, y: u * u * start.y! + 2 * u * t * cy + t * t * end.y!,
        dx: 2 * u * (cx - start.x!) + 2 * t * (end.x! - cx), dy: 2 * u * (cy - start.y!) + 2 * t * (end.y! - cy),
      };
    }
    return { x: start.x! + (end.x! - start.x!) * t, y: start.y! + (end.y! - start.y!) * t, dx: end.x! - start.x!, dy: end.y! - start.y! };
  };

  // Widths and dashes are screen pixels, so a link looks the same at every zoom.
  const paintLink = useCallback(
    (link: RenderLink, ctx: CanvasRenderingContext2D, scale: number) => {
      const start = link.source as RenderNode, end = link.target as RenderNode;
      if (!paint || typeof start !== "object" || typeof end !== "object" || start.x === undefined || end.x === undefined) return;
      const { colors } = paint;
      const aggregate = link.props.aggregate === true;
      const width = aggregate ? 1 : linkWidth(linkDocs(link));
      const trace = () => {
        ctx.beginPath();
        ctx.moveTo(start.x!, start.y!);
        const control = (link as { __controlPoints?: number[] }).__controlPoints;
        if (control?.length === 2) ctx.quadraticCurveTo(control[0], control[1], end.x!, end.y!);
        else ctx.lineTo(end.x!, end.y!);
      };
      if (selectedLink(link)) {
        trace();
        ctx.setLineDash([]);
        ctx.lineCap = "round";
        ctx.strokeStyle = colors.mark;
        ctx.lineWidth = (width + 12) / scale;
        ctx.stroke();
      }
      trace();
      ctx.lineCap = "butt";
      ctx.setLineDash((aggregate ? [3, 3] : link.props.implied === true ? [6, 4] : []).map(value => value / scale));
      ctx.strokeStyle = aggregate ? colors["border-strong"]
        : entitySelected && !nodeLinked(link) ? colors.border : colors["graph-edge"];
      ctx.lineWidth = width / scale;
      ctx.stroke();
      ctx.setLineDash([]);
      if (!aggregate && (selectedLink(link) || nodeLinked(link))) {
        // A 6 px arrowhead at 90% along the link.
        const { x, y, dx, dy } = along(link, 0.9);
        const angle = Math.atan2(dy, dx);
        ctx.save();
        ctx.translate(x, y);
        ctx.rotate(angle);
        ctx.scale(1 / scale, 1 / scale);
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(-6, -3.5);
        ctx.lineTo(-6, 3.5);
        ctx.closePath();
        ctx.fillStyle = colors.fg;
        ctx.fill();
        ctx.restore();
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [paint, selectedNodeId, selectedLinkId, markedLinkIds, nodeLinked, entitySelected],
  );

  // Labels go last, in one pass, so no node paints over them and the skip rule sees every label.
  const paintLabels = useCallback(
    (ctx: CanvasRenderingContext2D, scale: number) => {
      if (!paint) return;
      const { colors } = paint;
      const nodes = data.nodes.filter((node) => node.x !== undefined && node.y !== undefined);
      const focus = nodes.find((node) => node.id === focusId);
      const obstacles: Box[] = [];
      for (const node of nodes) {
        const x = node.x! * scale, y = node.y! * scale;
        if (node.label === "Aggregate") {
          const width = pillWidth(ctx, node);
          obstacles.push({ x0: x - width / 2, y0: y - PILL_HEIGHT / 2, x1: x + width / 2, y1: y + PILL_HEIGHT / 2 });
        } else {
          const extent = shapeExtent(node.label, nodeRadius(nodeDocs(node))) + 3;
          obstacles.push({ x0: x - extent, y0: y - extent, x1: x + extent, y1: y + extent });
        }
      }
      type Label = { x: number; y: number; text: string; weight: number; color: string; chip: boolean;
        candidates: Box[]; rule: LabelRule };
      const labels: Label[] = [];
      // The selected link and the selected node's links: "TYPE · N docs" on a chip near the middle.
      for (const link of data.links) {
        if (link.props.aggregate === true || !(selectedLink(link) || nodeLinked(link))) continue;
        if (typeof link.source !== "object" || (link.source as RenderNode).x === undefined) continue;
        const text = `${link.type} · ${docs(linkDocs(link))}`;
        ctx.font = font(500);
        const width = ctx.measureText(text).width + 6;
        const spots = [0.55, 0.4, 0.7].map((t) => along(link, t));
        const candidates = spots.flatMap(({ x, y }) => [-12, 12].map((dy) =>
          ({ x0: x * scale - width / 2, y0: y * scale + dy - 9, x1: x * scale + width / 2, y1: y * scale + dy + 9 })));
        labels.push({ x: 0, y: 0, text, weight: 500, color: colors.fg, chip: true, candidates, rule: selectedLink(link) ? "force" : "optional" });
      }
      const important = (node: RenderNode) =>
        node.id === focusId || node.id === selectedNodeId || node.id === hoveredId || node.id === keyboardNodeId;
      const all = nodes.length <= LABEL_ALL && showLabels && !phone;
      const ranked = nodes.filter((node) => node.label !== "Aggregate")
        .sort((a, b) => Number(important(b)) - Number(important(a)) || nodeDocs(b) - nodeDocs(a) || a.id.localeCompare(b.id));
      for (const node of ranked) {
        if (!important(node) && !showLabels) continue;
        const text = nodeLabel(node.name, node.id, node.label);
        const weight = node.id === focusId ? 500 : 400;
        ctx.font = font(weight);
        const x = node.x! * scale, y = node.y! * scale;
        const outward: [number, number] | null = focus && node !== focus ? [node.x! - focus.x!, node.y! - focus.y!] : null;
        const extent = shapeExtent(node.label, nodeRadius(nodeDocs(node)));
        const dim = entitySelected && node.id !== selectedNodeId;
        labels.push({ x: node.x!, y: node.y!, text, weight, color: dim ? colors["fg-subtle"] : colors.fg, chip: false,
          candidates: labelCandidates(x, y, extent, ctx.measureText(text).width, outward),
          rule: important(node) ? "force" : all ? "all" : "optional" });
      }
      // The visible canvas in the same space: graph units times the zoom, origin at the graph origin.
      const { a, e, f: top } = ctx.getTransform();
      const ratio = a / scale, left = -e / ratio, upper = -top / ratio;
      const boxes = placeLabels(labels, obstacles, { x0: left, y0: upper, x1: left + dimensions.width, y1: upper + dimensions.height });
      ctx.save();
      ctx.textBaseline = "middle";
      ctx.textAlign = "left";
      ctx.lineJoin = "round";
      labels.forEach((label, index) => {
        const box = boxes[index];
        if (!box) return;
        // Text is drawn with the context at the label's node (or the link label's box) in screen pixels.
        const anchorX = label.chip ? (box.x0 + box.x1) / 2 : label.x * scale;
        const anchorY = label.chip ? (box.y0 + box.y1) / 2 : label.y * scale;
        ctx.save();
        ctx.scale(1 / scale, 1 / scale);
        ctx.translate(anchorX, anchorY);
        const left = box.x0 - anchorX + (label.chip ? 3 : 0), middle = (box.y0 + box.y1) / 2 - anchorY;
        ctx.font = font(label.weight);
        if (label.chip) {
          ctx.fillStyle = colors.bg;
          ctx.beginPath();
          ctx.roundRect(box.x0 - anchorX, box.y0 - anchorY, box.x1 - box.x0, box.y1 - box.y0, 4);
          ctx.fill();
        } else {
          ctx.strokeStyle = colors.bg;
          ctx.lineWidth = 6;
          ctx.strokeText(label.text, left, middle);
        }
        ctx.fillStyle = label.color;
        ctx.fillText(label.text, left, middle);
        ctx.restore();
      });
      ctx.restore();
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [paint, data, focusId, selectedNodeId, selectedLinkId, markedLinkIds, entitySelected, hoveredId, keyboardNodeId, showLabels, phone, nodeLinked, pillWidth, font, dimensions],
  );

  const hover = useCallback((node: RenderNode | null) => {
    clearTimeout(hoverTimer.current);
    if (!node || node.label === "Aggregate") setHoveredId(null);
    else hoverTimer.current = setTimeout(() => setHoveredId(node.id), HOVER_DELAY);
  }, []);

  const preserveUserView = (event: { target: EventTarget }) => {
    if (event.target instanceof HTMLCanvasElement) initialFitPending.current = false;
  };

  const { Component, error } = renderer;
  return (
    <div
      ref={containerRef}
      onWheelCapture={preserveUserView}
      onPointerDownCapture={preserveUserView}
      className="relative h-full w-full min-h-0 overflow-hidden bg-background"
    >
      {error ? (
        <div
          role="alert"
          className="absolute inset-0 flex flex-col items-center justify-center gap-3 p-6 text-center"
        >
          <AlertCircle className="h-6 w-6 text-error" />
          <p>{error}</p>
        </div>
      ) : !Component || !dimensions.width || !dimensions.height || !paint ? (
        <div
          role="status"
          className="absolute inset-0 flex items-center justify-center gap-2 text-sm text-muted-foreground"
        >
          <Loader2 className="h-5 w-5 animate-spin" />
          Loading graph renderer…
        </div>
      ) : (
        // Children of an image are hidden from assistive technology, so the image wraps only the renderer.
        <div role="img" aria-label={name}>
        <Component
          ref={setHandle}
          graphData={data}
          width={dimensions.width}
          height={dimensions.height}
          backgroundColor={paint.colors.bg}
          nodeCanvasObjectMode={() => "replace"}
          nodeCanvasObject={paintNode}
          nodeLabel={(node) =>
            node.label === "Aggregate"
              ? escapeGraphLabel(node.name)
              : `${escapeGraphLabel(fullName(node))} · ${escapeGraphLabel(nodeLabelText(node.label))}`
          }
          linkLabel={(link) =>
            `${escapeGraphLabel(link.type)}${link.props.implied ? " · inferred" : ""}`
          }
          linkCurvature="curvature"
          linkWidth={(link) => (link.props.aggregate ? 1 : linkWidth(linkDocs(link)))}
          linkCanvasObjectMode={() => "replace"}
          linkCanvasObject={paintLink}
          onRenderFramePost={paintLabels}
          nodePointerAreaPaint={(node, color, ctx, scale) => {
            if (node.x === undefined || node.y === undefined) return;
            ctx.fillStyle = color;
            ctx.beginPath();
            if (node.label === "Aggregate") {
              const width = pillWidth(ctx, node) / scale;
              ctx.roundRect(node.x - width / 2, node.y - PILL_HEIGHT / 2 / scale, width, PILL_HEIGHT / scale, PILL_HEIGHT / 2 / scale);
            } else ctx.arc(node.x, node.y, (shapeExtent(node.label, nodeRadius(nodeDocs(node))) + 2) / scale, 0, Math.PI * 2);
            ctx.fill();
          }}
          onNodeHover={hover}
          onNodeClick={(node, event) => onSelectNode(node, event)}
          onBackgroundClick={onClear}
          onLinkClick={(link) =>
            onSelectLink({
              ...link,
              source: endpointId(link.source),
              target: endpointId(link.target),
            })
          }
          onNodeDragEnd={(node) => {
            if (fixed) return;
            node.fx = node.x;
            node.fy = node.y;
          }}
          onEngineStop={() => {
            if (!fixed && initialFitPending.current && handleRef.current) {
              initialFitPending.current = false;
              // An initial tween must not keep overriding later user gestures.
              handleRef.current.zoomToFit(0, 65);
            }
          }}
          warmupTicks={fixed ? 0 : 50}
          cooldownTicks={fixed ? 0 : 80}
          d3AlphaDecay={0.06}
          d3VelocityDecay={0.5}
          enableNodeDrag={!fixed}
          minZoom={0.1}
        />
        </div>
      )}
    </div>
  );
}
