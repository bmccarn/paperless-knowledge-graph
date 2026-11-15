/** Pure canvas encoding: node size, link width, shapes, label text and label placement, in screen pixels. */
import { nodeKind, nodeLabelText, type Kind } from "./categories";

const clamp = (value: number, low: number, high: number) => Math.min(high, Math.max(low, value));

/** Node radius from its evidence documents. */
export const nodeRadius = (documents: number) => clamp(6 + 2.5 * Math.log2(1 + Math.max(0, documents)), 6, 18);

/** Link width from its supporting documents. */
export const linkWidth = (weight: number) => clamp(1 + 0.75 * Math.log2(Math.max(1, weight)), 1, 4);

/** With this many nodes or fewer, every node is labeled. */
export const LABEL_ALL = 40;

/** The name, or the kind and a short id when the node has none; never the full id. */
export function displayName(name: string, id: string, label: string): string {
  return name && name !== id ? name : `${nodeLabelText(label)} ${id.replace(/^doc-/, "").slice(0, 6)}`;
}

/** The canvas label: the display name cut at 28 characters. */
export function nodeLabel(name: string, id: string, label: string): string {
  const text = displayName(name, id, label);
  return text.length > 28 ? `${text.slice(0, 27)}…` : text;
}

// Shapes from the approved mockup; each has the visual weight of a circle of radius r.
const EXTENT: Record<Kind, number> = {
  people: 1, unknown: 1, org: 1.15, place: 1.22, money: 1.08, agree: 1.14, thing: 1.2, health: 1.2, time: 1.32, docs: 1.1,
};

/** Outer radius of a kind's shape, for rings drawn around it. */
export const shapeExtent = (label: string, r: number) => r * EXTENT[nodeKind(label).kind];

const f = (value: number) => (Math.round(value * 100) / 100 || 0).toString();
function polygon(sides: number, r: number, inner?: number): string {
  const steps = inner ? sides * 2 : sides;
  const points = Array.from({ length: steps }, (_, index) => {
    const angle = ((-90 + (index * 360) / steps) * Math.PI) / 180;
    const radius = inner && index % 2 ? inner : r;
    return `${f(radius * Math.cos(angle))},${f(radius * Math.sin(angle))}`;
  });
  return `M${points.join("L")}Z`;
}

/** SVG path data for a node's shape around (0, 0). */
export function shapePath(label: string, r: number): string {
  switch (nodeKind(label).kind) {
    case "org": {
      const h = r * 0.9, k = h * 0.44;
      return `M${f(-h + k)},${f(-h)}H${f(h - k)}Q${f(h)},${f(-h)} ${f(h)},${f(-h + k)}V${f(h - k)}Q${f(h)},${f(h)} ${f(h - k)},${f(h)}`
        + `H${f(-h + k)}Q${f(-h)},${f(h)} ${f(-h)},${f(h - k)}V${f(-h + k)}Q${f(-h)},${f(-h)} ${f(-h + k)},${f(-h)}Z`;
    }
    case "place": return polygon(4, r * 1.22);
    case "money": return polygon(6, r * 1.08);
    case "agree": return polygon(5, r * 1.14);
    case "thing": {
      // The centroid sits a little low, like the visual center.
      const radius = r * 1.32, dy = radius * 0.12;
      return `M0,${f(dy - radius)}L${f(radius * Math.cos(Math.PI / 6))},${f(dy + radius / 2)}L${f(-radius * Math.cos(Math.PI / 6))},${f(dy + radius / 2)}Z`;
    }
    case "health": {
      const R = r * 1.12, a = R / 3;
      return `M${f(-a)},${f(-R)}H${f(a)}V${f(-a)}H${f(R)}V${f(a)}H${f(a)}V${f(R)}H${f(-a)}V${f(a)}H${f(-R)}V${f(-a)}H${f(-a)}Z`;
    }
    case "time": return polygon(5, r * 1.32, r * 0.66);
    case "docs": {
      const w = r * 0.82, h = r * 1.06, fold = w * 0.55;
      return `M${f(-w)},${f(-h)}H${f(w - fold)}L${f(w)},${f(-h + fold)}V${f(h)}H${f(-w)}Z`;
    }
    default: return `M${f(-r)},0A${f(r)},${f(r)} 0 1,0 ${f(r)},0A${f(r)},${f(r)} 0 1,0 ${f(-r)},0Z`;
  }
}

export interface Box { x0: number; y0: number; x1: number; y1: number }
const LINE = 16;

/**
 * Label boxes for a 12 px label of `width` beside a node at (x, y) with shape extent `extent`.
 * `outward` points away from the focus: the outer side comes first, or above or below for a node
 * near 12 or 6 o'clock. Without it the label goes 4 px below the node first.
 */
export function labelCandidates(x: number, y: number, extent: number, width: number, outward: [number, number] | null): Box[] {
  const at = (left: number, top: number): Box => ({ x0: x + left, y0: y + top, x1: x + left + width, y1: y + top + LINE });
  const below = at(-width / 2, extent + 4);
  const above = at(-width / 2, -extent - 4 - LINE);
  if (!outward) return [below, above, at(extent + 6, -LINE / 2), at(-extent - 6 - width, -LINE / 2)];
  const [dx, dy] = outward;
  const length = Math.hypot(dx, dy) || 1;
  const sin = dx / length, down = dy / length;
  const right = sin >= 0;
  const side = (shift: number) => at(right ? extent + 6 : -extent - 6 - width, -LINE / 2 + shift);
  const nudges = (down < 0 ? [-9, 9, -18, 18] : [9, -9, 18, -18]).map(side);
  const shifted = [16, 32].flatMap(shift => [below, above].map(box => ({ ...box, x0: box.x0 + (right ? shift : -shift), x1: box.x1 + (right ? shift : -shift) })));
  const ends = down < 0 ? [above, below] : [below, above];
  // Last, the inner side, for a node at the canvas edge.
  const inner = at(right ? -extent - 6 - width : extent + 6, -LINE / 2);
  return Math.abs(sin) < 0.3
    ? [ends[0], side(0), ...nudges, ends[1], ...shifted, inner]
    : [side(0), ...nudges, ...ends, ...shifted, inner];
}

const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && a.x1 > b.x0 && a.y0 < b.y1 && a.y1 > b.y0;
const inside = (a: Box, bounds?: Box) => !bounds || (a.x0 >= bounds.x0 && a.y0 >= bounds.y0 && a.x1 <= bounds.x1 && a.y1 <= bounds.y1);

/**
 * How a label yields when crowded: `force` always draws (focus, selection, hover); `all` (40 or fewer
 * nodes) may overlap another label but never a node or a pill; `optional` is skipped on any overlap.
 */
export type LabelRule = "force" | "all" | "optional";

/**
 * Greedy placement in priority order: each label takes its first candidate inside `bounds` and clear of
 * the obstacles (nodes and pills) and of the labels placed before it, then falls back by its rule.
 */
export function placeLabels(items: { candidates: Box[]; rule: LabelRule }[], obstacles: Box[], bounds?: Box): (Box | null)[] {
  const placed: Box[] = [];
  const clear = (box: Box, others: Box[]) => inside(box, bounds) && !others.some(other => overlaps(box, other));
  return items.map(({ candidates, rule }) => {
    const box = candidates.find(candidate => clear(candidate, [...obstacles, ...placed]))
      ?? (rule === "optional" ? undefined : candidates.find(candidate => clear(candidate, obstacles)))
      ?? (rule === "force" ? candidates[0] : undefined);
    if (box) placed.push(box);
    return box ?? null;
  });
}
