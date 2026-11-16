"use client";

import { useCallback, useEffect, useMemo, useRef, useState, type ComponentType, type RefAttributes } from "react";
import type { ForceGraphMethods, ForceGraphProps, NodeObject } from "react-force-graph-2d";
import { ChevronDown, ChevronUp, Loader2 } from "lucide-react";
import { KindShape } from "@/components/kind-shape";
import { nodeKind, nodeLabelText, NODE_LABELS, type Kind } from "@/lib/categories";
import { escapeGraphLabel } from "@/lib/graph-data";
import { displayName, labelCandidates, linkWidth, nodeLabel, nodeRadius, placeLabels, shapeExtent, type Box, type LabelRule } from "@/lib/graph-encoding";
import type { OverviewMap } from "@/lib/graph-overview";
import { cn } from "@/lib/utils";
import { paintShape, readColors, type Colors } from "./canvas-paint";
import type { Camera } from "./force-graph-client";

/** `at` is the entity's index in the response; the simulation overwrites `index` with the array position. */
type Dot = { id: string; at: number; x: number; y: number; fx: number; fy: number };
type Handle = ForceGraphMethods<Dot, never>;
type GraphComponent = ComponentType<ForceGraphProps<Dot, never> & RefAttributes<Handle>>;

/** Below this scale the map draws dots; at it and above, the shared encoding. */
export const DETAIL_ZOOM = 1;
const LABEL_FONT = 12;
const GROUP_FONT = 15;
/** At most this many entity labels are tried per frame at detail zoom, in evidence order. */
const LABEL_LIMIT = 300;

export const KIND_NAMES: Record<Kind, string> = { people: "People", org: "Organizations", place: "Places", money: "Money", agree: "Agreements",
  thing: "Things", health: "Health", time: "Time", docs: "Documents", unknown: "Other" };
const KIND_ORDER = Object.keys(KIND_NAMES) as Kind[];
const kindToken = (kind: Kind) => (kind === "unknown" ? "fg-subtle" : `cat-${kind}`);
/** Dots 3 to 7 px across, by evidence documents. */
const dotRadius = (documents: number) => 1.5 + 2 * Math.min(1, Math.log2(1 + documents) / 6);

interface Props {
  map: OverviewMap;
  selectedGroup: number | null;
  selectedNode: number | null;
  onSelectGroup: (group: number | null) => void;
  onSelectNode: (node: number) => void;
  onReady: (camera: OverviewCamera | null) => void;
  /** The canvas's accessible name. */
  name: string;
}

/** The zoom buttons, plus a move to one group or entity for the inspector and the list. */
export interface OverviewCamera extends Camera { showGroup: (group: number) => void; showNode: (node: number) => void }

/** Overview on the force-graph canvas with fixed positions: no simulation runs and no node can be dragged. */
export function OverviewCanvas({ map, selectedGroup, selectedNode, onSelectGroup, onSelectNode, onReady, name }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const handle = useRef<Handle | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const [paint, setPaint] = useState<{ colors: Colors; sans: string; serif: string } | null>(null);
  const [Component, setComponent] = useState<GraphComponent | null>(null);
  const [detail, setDetail] = useState(false);
  const [hovered, setHovered] = useState<number | null>(null);
  /** The visible canvas in graph units, from the frame being drawn; nodes outside it are skipped. */
  const view = useRef({ x0: 0, y0: 0, x1: 0, y1: 0 });
  /** Group name boxes in screen pixels from the last frame, for clicks on a name. */
  const nameBoxes = useRef<{ group: number; box: Box }[]>([]);
  const fitted = useRef(false);

  useEffect(() => {
    let active = true;
    void import("react-force-graph-2d").then(module => { if (active) setComponent(() => module.default as unknown as GraphComponent); });
    return () => { active = false; };
  }, []);
  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => setSize({ width: Math.round(entry.contentRect.width), height: Math.round(entry.contentRect.height) }));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  // Colors at each theme change; the first paint waits for both label faces, so names and the fit never shift.
  useEffect(() => {
    const element = container.current;
    if (!element) return;
    let active = true;
    const style = getComputedStyle(element);
    const sans = style.fontFamily, serif = style.getPropertyValue("--font-geist-serif-regular").trim() || "serif";
    const read = () => { if (active) setPaint({ colors: readColors(element), sans, serif }); };
    void Promise.all([`400 ${GROUP_FONT}px ${serif}`, `400 ${LABEL_FONT}px ${sans}`, `500 ${LABEL_FONT}px ${sans}`].map(font => document.fonts.load(font))).then(read, read);
    const observer = new MutationObserver(read);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "style"] });
    return () => { active = false; observer.disconnect(); };
  }, []);

  const data = useMemo(() => ({
    nodes: map.nodes.flatMap((node, index) => {
      const x = map.positions[index * 2], y = map.positions[index * 2 + 1];
      return Number.isNaN(x) ? [] : [{ id: node.id, at: index, x, y, fx: x, fy: y }];
    }),
    links: [] as never[],
  }), [map]);

  /** The top and bottom of each group's dots, where its name sits. */
  const ends = useMemo(() => map.groups.map(group => {
    const ys = group.members.map(node => map.positions[node * 2 + 1]);
    return { top: Math.min(...ys), bottom: Math.max(...ys) };
  }), [map]);

  const fitView = useCallback((ms: number) => {
    const graph = handle.current;
    if (!graph || !size.width || !size.height) return;
    const { x0, y0, x1, y1 } = map.bounds;
    // Room for the toolbar (top) and the zoom buttons (right), and for the legend either at the left or at the bottom.
    const fits = [{ left: 296, bottom: 48 }, { left: 48, bottom: 248 }].map(pad => {
      const k = Math.min(2, (size.width - pad.left - 64) / Math.max(1, x1 - x0), (size.height - 72 - pad.bottom) / Math.max(1, y1 - y0 + 40));
      return { k, x: (x0 + x1) / 2 - (pad.left - 64) / 2 / k, y: (y0 + y1) / 2 - 20 / k + (pad.bottom - 72) / 2 / k };
    });
    const best = fits[0].k >= fits[1].k ? fits[0] : fits[1];
    graph.centerAt(best.x, best.y, ms);
    graph.zoom(best.k, ms);
  }, [map, size]);

  useEffect(() => {
    if (fitted.current || !Component || !paint || !size.width) return;
    const frame = requestAnimationFrame(() => {
      if (!handle.current) return;
      fitted.current = true;
      fitView(0);
    });
    return () => cancelAnimationFrame(frame);
  }, [Component, paint, size, fitView]);

  useEffect(() => {
    const duration = () => (matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 300);
    const take = () => { fitted.current = true; };
    onReady({
      zoomBy: factor => { take(); const graph = handle.current; if (graph) graph.zoom(graph.zoom() * factor, duration()); },
      fit: () => { take(); fitView(duration()); },
      showGroup: group => {
        take();
        const graph = handle.current, target = map.groups[group];
        if (!graph || !target) return;
        graph.centerAt(target.x, target.y, duration());
        graph.zoom(Math.min(DETAIL_ZOOM * 1.5, (Math.min(size.width, size.height) - 120) / (2 * target.r)), duration());
      },
      showNode: node => {
        take();
        const graph = handle.current;
        if (!graph || Number.isNaN(map.positions[node * 2])) return;
        graph.centerAt(map.positions[node * 2], map.positions[node * 2 + 1], duration());
        graph.zoom(Math.max(graph.zoom(), DETAIL_ZOOM * 1.5), duration());
      },
    });
    return () => onReady(null);
  }, [onReady, fitView, map, size]);

  const groupAt = useCallback((x: number, y: number) => {
    const found = map.groups.findIndex(group => Math.hypot(x - group.x, y - group.y) <= group.r);
    return found < 0 ? null : found;
  }, [map]);

  const before = useCallback((ctx: CanvasRenderingContext2D, k: number) => {
    if (!paint) return;
    const { colors } = paint;
    const { a, d, e, f } = ctx.getTransform();
    view.current = { x0: -e / a, y0: -f / d, x1: (ctx.canvas.width - e) / a, y1: (ctx.canvas.height - f) / d };
    const { x0, y0, x1, y1 } = view.current;
    const inView = (x: number, y: number, margin: number) => x > x0 - margin && x < x1 + margin && y > y0 - margin && y < y1 + margin;
    const at = (node: number) => [map.positions[node * 2], map.positions[node * 2 + 1]];
    const zoomed = k >= DETAIL_ZOOM;
    if (selectedGroup !== null && map.groups[selectedGroup]) {
      const { x, y, r } = map.groups[selectedGroup];
      ctx.beginPath();
      ctx.arc(x, y, r + 6 / k, 0, Math.PI * 2);
      ctx.strokeStyle = colors.mark;
      ctx.lineWidth = 10 / k;
      ctx.stroke();
      ctx.strokeStyle = colors.fg;
      ctx.lineWidth = 1.5 / k;
      ctx.stroke();
    }
    // Links in one path per style: links inside a group, then dotted links between groups.
    const styles = new Map<string, { path: Path2D; width: number; dash: number[]; color: string }>();
    for (const link of map.links) {
      const [sx, sy] = at(link.source), [tx, ty] = at(link.target);
      if (Number.isNaN(sx) || Number.isNaN(tx)) continue;
      if (!inView(Math.min(sx, tx), Math.min(sy, ty), Math.abs(sx - tx) + Math.abs(sy - ty))) continue;
      const between = map.groupOf[link.source] !== map.groupOf[link.target] || map.groupOf[link.source] < 0;
      const width = zoomed && !between ? linkWidth(link.weight) : 1;
      const dash = between ? [2, 3] : zoomed && link.support === 2 ? [6, 4] : [];
      const color = between ? colors["border-strong"] : zoomed ? colors["graph-edge"] : colors.border;
      const style = `${width}|${dash}|${color}`;
      if (!styles.has(style)) styles.set(style, { path: new Path2D(), width, dash, color });
      const { path } = styles.get(style)!;
      path.moveTo(sx, sy);
      path.lineTo(tx, ty);
    }
    for (const { path, width, dash, color } of styles.values()) {
      ctx.setLineDash(dash.map(value => value / k));
      ctx.strokeStyle = color;
      ctx.lineWidth = width / k;
      ctx.stroke(path);
    }
    ctx.setLineDash([]);
    if (zoomed) return;
    // Map zoom: one dot per entity in its kind color, in one path per color.
    const dots = new Map<string, Path2D>();
    for (const { at: index, x, y } of data.nodes) {
      if (!inView(x, y, 8 / k)) continue;
      const token = kindToken(nodeKind(map.nodes[index].label).kind);
      const r = dotRadius(map.nodes[index].documents) / k;
      if (!dots.has(token)) dots.set(token, new Path2D());
      dots.get(token)!.moveTo(x + r, y);
      dots.get(token)!.arc(x, y, r, 0, Math.PI * 2);
    }
    for (const [token, path] of dots) {
      ctx.fillStyle = colors[token];
      ctx.fill(path);
    }
    if (selectedNode !== null && !Number.isNaN(map.positions[selectedNode * 2])) {
      const [x, y] = at(selectedNode);
      ctx.beginPath();
      ctx.arc(x, y, 7 / k, 0, Math.PI * 2);
      ctx.strokeStyle = colors.mark;
      ctx.lineWidth = 6 / k;
      ctx.stroke();
      ctx.strokeStyle = colors.fg;
      ctx.lineWidth = 1.5 / k;
      ctx.stroke();
    }
  }, [paint, map, data, selectedGroup, selectedNode]);

  const paintNode = useCallback((node: NodeObject<Dot>, ctx: CanvasRenderingContext2D, k: number) => {
    const { x0, y0, x1, y1 } = view.current;
    if (!paint || node.x === undefined || node.y === undefined || node.x < x0 - 30 / k || node.x > x1 + 30 / k || node.y < y0 - 30 / k || node.y > y1 + 30 / k) return;
    const entity = map.nodes[node.at];
    ctx.save();
    ctx.translate(node.x, node.y);
    ctx.scale(1 / k, 1 / k);
    paintShape(ctx, entity.label, nodeRadius(entity.documents), paint.colors, { selected: node.at === selectedNode, faded: false,
      hovered: node.at === hovered, keyboard: false, ringed: entity.links > 100 });
    ctx.restore();
  }, [paint, map, selectedNode, hovered]);

  const after = useCallback((ctx: CanvasRenderingContext2D, k: number) => {
    if (!paint) return;
    const { colors, sans, serif } = paint;
    const { a, e, f } = ctx.getTransform();
    const ratio = a / k;
    const left = -e / ratio, top = -f / ratio;
    const bounds = { x0: left, y0: top, x1: left + size.width, y1: top + size.height };
    const text = (value: string, x: number, y: number, font: string, color: string) => {
      ctx.font = font;
      ctx.lineWidth = 4;
      ctx.strokeStyle = colors.bg;
      ctx.strokeText(value, x, y);
      ctx.fillStyle = color;
      ctx.fillText(value, x, y);
    };
    ctx.save();
    ctx.scale(1 / k, 1 / k);
    ctx.textBaseline = "middle";
    ctx.lineJoin = "round";
    if (map.band) {
      const x = map.band.x0 * k, y = map.band.y0 * k + 10;
      ctx.textAlign = "left";
      text("Small groups", x, y, `400 13px ${serif}`, colors.fg);
      ctx.font = `400 13px ${serif}`;
      const width = ctx.measureText("Small groups").width;
      const sizes = new Set(map.small.map(set => set.length));
      const low = Math.min(...sizes), high = Math.max(...sizes);
      const words = low === high ? `of ${low} ${low === 1 ? "entity" : "entities"}` : `of ${low} ${high - low === 1 ? "or" : "to"} ${high} entities`;
      text(`${map.small.length.toLocaleString("en-US")} ${map.small.length === 1 ? "group" : "groups"} ${words}`, x + width + 8, y, `400 ${LABEL_FONT}px ${sans}`, colors["fg-muted"]);
    }
    const boxes: { group: number; box: Box }[] = [];
    if (k < DETAIL_ZOOM) {
      // Group names above their circles, largest first; the selected group's name always shows.
      const order = map.groups.map((_, index) => index).sort((p, q) => Number(q === selectedGroup) - Number(p === selectedGroup) || p - q);
      ctx.textAlign = "left";
      for (const index of order) {
        const group = map.groups[index];
        if (index !== selectedGroup && group.r * k < 10) continue;
        const lead = map.nodes[group.lead];
        const label = nodeLabel(lead.name, lead.id, lead.label);
        const count = group.members.length.toLocaleString("en-US");
        ctx.font = `400 ${GROUP_FONT}px ${serif}`;
        const nameWidth = ctx.measureText(label).width;
        ctx.font = `400 ${LABEL_FONT}px ${sans}`;
        const width = nameWidth + 6 + ctx.measureText(count).width;
        // Above the circle, else below it; a name never covers another name or another group.
        const spots = [ends[index].top * k - 16, ends[index].bottom * k + 16].map(cy => ({ cy, box: { x0: group.x * k - width / 2 - 2, y0: cy - 10, x1: group.x * k + width / 2 + 2, y1: cy + 10 } }));
        const free = ({ box }: { box: Box }) => box.x0 > bounds.x0 && box.x1 < bounds.x1 && box.y0 > bounds.y0 && box.y1 < bounds.y1
          && !boxes.some(({ box: other }) => box.x0 < other.x1 && box.x1 > other.x0 && box.y0 < other.y1 && box.y1 > other.y0)
          && !map.groups.some((other, at) => at !== index && Math.hypot(Math.max(box.x0 - other.x * k, 0, other.x * k - box.x1), Math.max(box.y0 - other.y * k, 0, other.y * k - box.y1)) < other.r * k);
        const spot = spots.find(free) ?? (index === selectedGroup ? spots[0] : undefined);
        if (!spot) continue;
        const { cy, box } = spot;
        boxes.push({ group: index, box });
        text(label, box.x0 + 2, cy, `400 ${GROUP_FONT}px ${serif}`, colors.fg);
        text(count, box.x0 + 2 + nameWidth + 6, cy + 1, `400 ${LABEL_FONT}px ${sans}`, colors["fg-muted"]);
      }
    } else {
      // Detail zoom: entity labels in evidence order; a label that would overlap an earlier one or a shape is skipped.
      const shown = data.nodes.filter(({ x, y }) => x * k > bounds.x0 - 40 && x * k < bounds.x1 + 40 && y * k > bounds.y0 - 40 && y * k < bounds.y1 + 40);
      const important = (index: number) => index === selectedNode || index === hovered;
      const ranked = shown.sort((p, q) => Number(important(q.at)) - Number(important(p.at)) || map.nodes[q.at].documents - map.nodes[p.at].documents || p.at - q.at).slice(0, LABEL_LIMIT);
      const obstacles = ranked.map(({ x, y, at: index }) => {
        const extent = shapeExtent(map.nodes[index].label, nodeRadius(map.nodes[index].documents)) + 3;
        return { x0: x * k - extent, y0: y * k - extent, x1: x * k + extent, y1: y * k + extent };
      });
      ctx.font = `400 ${LABEL_FONT}px ${sans}`;
      const labels = ranked.map(({ x, y, at: index }) => {
        const entity = map.nodes[index];
        const value = nodeLabel(entity.name, entity.id, entity.label);
        const rule: LabelRule = important(index) ? "force" : "optional";
        return { value, candidates: labelCandidates(x * k, y * k, shapeExtent(entity.label, nodeRadius(entity.documents)), ctx.measureText(value).width, null), rule };
      });
      const placed = placeLabels(labels, obstacles, bounds);
      ctx.textAlign = "left";
      labels.forEach((label, index) => {
        const box = placed[index];
        if (box) text(label.value, box.x0, (box.y0 + box.y1) / 2, `400 ${LABEL_FONT}px ${sans}`, colors.fg);
      });
    }
    nameBoxes.current = boxes.map(({ group, box }) => ({ group, box: { x0: box.x0 - left, y0: box.y0 - top, x1: box.x1 - left, y1: box.y1 - top } }));
    ctx.restore();
  }, [paint, map, data, ends, size, selectedGroup, selectedNode, hovered]);

  const kinds = useMemo(() => {
    const seen = new Set(data.nodes.map(({ at }) => map.nodes[at].label));
    return { kinds: KIND_ORDER.filter(kind => [...seen].some(label => nodeKind(label).kind === kind)),
      labels: [...NODE_LABELS.filter(label => seen.has(label)), ...([...seen].some(label => nodeKind(label).kind === "unknown") ? ["Other"] : [])],
      inferred: map.links.some(link => link.support === 2), between: map.links.some(link => map.groupOf[link.source] !== map.groupOf[link.target] || map.groupOf[link.source] < 0) };
  }, [map, data]);

  return (
    <div ref={container} className="relative h-full w-full overflow-hidden bg-background">
      {!Component || !paint || !size.width ? (
        <div role="status" className="absolute inset-0 flex items-center justify-center gap-2 copy-14 text-fg-muted">
          <Loader2 className="size-5 animate-spin" />Loading graph renderer…
        </div>
      ) : (
        <div role="img" aria-label={name}>
          <Component
            ref={graph => { handle.current = graph; }}
            graphData={data}
            width={size.width}
            height={size.height}
            backgroundColor={paint.colors.bg}
            nodeVisibility={() => detail}
            nodeCanvasObjectMode={() => "replace"}
            nodeCanvasObject={paintNode}
            nodePointerAreaPaint={(node, color, ctx, k) => {
              if (node.x === undefined || node.y === undefined) return;
              const entity = map.nodes[node.at];
              ctx.fillStyle = color;
              ctx.beginPath();
              ctx.arc(node.x, node.y, (shapeExtent(entity.label, nodeRadius(entity.documents)) + 2) / k, 0, Math.PI * 2);
              ctx.fill();
            }}
            nodeLabel={node => `${escapeGraphLabel(displayName(map.nodes[node.at].name, node.id, map.nodes[node.at].label))} · ${escapeGraphLabel(nodeLabelText(map.nodes[node.at].label))}`}
            onNodeHover={node => setHovered(node ? node.at : null)}
            onNodeClick={node => onSelectNode(node.at)}
            onBackgroundClick={event => {
              const graph = handle.current;
              if (!graph) return;
              const hit = nameBoxes.current.find(({ box }) => event.offsetX >= box.x0 && event.offsetX <= box.x1 && event.offsetY >= box.y0 && event.offsetY <= box.y1);
              const { x, y } = graph.screen2GraphCoords(event.offsetX, event.offsetY);
              onSelectGroup(hit ? hit.group : groupAt(x, y));
            }}
            onZoom={({ k }) => setDetail(k >= DETAIL_ZOOM)}
            onRenderFramePre={before}
            onRenderFramePost={after}
            cooldownTicks={0}
            warmupTicks={0}
            enableNodeDrag={false}
            minZoom={0.02}
            maxZoom={8}
          />
        </div>
      )}
      {Component && paint && <OverviewLegend detail={detail} {...kinds} />}
    </div>
  );
}

/** Only what is drawn: kind colors at map zoom, shapes and line styles at detail zoom. */
function OverviewLegend({ detail, kinds, labels, inferred, between }: { detail: boolean; kinds: Kind[]; labels: string[]; inferred: boolean; between: boolean }) {
  const [open, setOpen] = useState(true);
  const line = (dash: string | undefined, text: string, stroke = "var(--graph-edge)") => (
    <li className="flex items-center gap-2"><svg aria-hidden width="24" height="10"><line x1="1" y1="5" x2="23" y2="5" stroke={stroke} strokeWidth={detail ? 2 : 1} strokeDasharray={dash} /></svg>{text}</li>);
  return (
    <section aria-labelledby="overview-legend-title" className="absolute bottom-4 left-4 w-[260px] rounded-(--r-panel) border border-border bg-surface px-3 pb-3 pt-2 shadow-(--shadow-overlay) max-md:hidden">
      <div className="flex h-7 items-center justify-between">
        <h2 id="overview-legend-title" className="label-13 font-semibold text-fg">Legend</h2>
        <button type="button" aria-expanded={open} aria-controls="overview-legend-content" onClick={() => setOpen(!open)}
          className="inline-flex size-7 items-center justify-center rounded-md text-fg-muted hover:bg-raised">
          {open ? <ChevronDown aria-hidden className="size-3.5" /> : <ChevronUp aria-hidden className="size-3.5" />}
          <span className="sr-only">{open ? "Collapse legend" : "Expand legend"}</span>
        </button>
      </div>
      {open && (
        <div id="overview-legend-content" className="mt-1 label-12 text-fg-muted">
          {detail ? (
            <ul aria-label="Kinds" className="grid grid-cols-2 gap-x-3 gap-y-1">
              {labels.map(label => <li key={label}><KindShape label={label} className="whitespace-nowrap label-12 text-fg-muted" /></li>)}
            </ul>
          ) : (
            <ul aria-label="Kinds" className="grid grid-cols-2 gap-x-3 gap-y-1">
              {kinds.map(kind => (
                <li key={kind} className="flex items-center gap-2">
                  <span aria-hidden className="size-2 shrink-0 rounded-full" style={{ background: `var(--${kindToken(kind)})` }} />{KIND_NAMES[kind]}
                </li>))}
            </ul>
          )}
          <ul aria-label="Links" className={cn("mt-2 grid gap-1 border-t border-border pt-2")}>
            {detail && line(undefined, "Supported")}
            {detail && inferred && line("6 4", "Inferred")}
            {between && line("2 3", "Between groups", "var(--border-strong)")}
          </ul>
          <p className="mt-2 border-t border-border pt-2">
            {detail ? "Size: evidence documents. Zoom out for the map of groups." : "Dots show kind by color. Zoom in for shapes, sizes and names."}
          </p>
        </div>
      )}
    </section>
  );
}
