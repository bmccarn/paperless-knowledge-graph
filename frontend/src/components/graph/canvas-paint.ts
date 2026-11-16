/** Canvas colors from the CSS tokens, and the entity shape of the shared encoding, for every graph canvas. */
import { nodeKind } from "@/lib/categories";
import { shapeExtent, shapePath } from "@/lib/graph-encoding";
import { iconPath } from "./canvas-icons";

const KINDS = ["people", "org", "place", "money", "agree", "thing", "health", "time", "docs"];
const TOKENS = ["bg", "surface", "fg", "fg-muted", "fg-subtle", "border", "border-strong", "ring", "mark",
  "graph-edge", "graph-faded", ...KINDS.flatMap(kind => [`cat-${kind}`, `on-cat-${kind}`])];
export type Colors = Record<string, string>;

/** Every canvas color from the CSS tokens, resolved to sRGB through a 1 x 1 canvas. */
export function readColors(element: Element): Colors {
  const style = getComputedStyle(element);
  const context = Object.assign(document.createElement("canvas"), { width: 1, height: 1 })
    .getContext("2d", { willReadFrequently: true })!;
  return Object.fromEntries(TOKENS.map(name => {
    context.clearRect(0, 0, 1, 1);
    context.fillStyle = style.getPropertyValue(`--${name}`).trim() || "transparent";
    context.fillRect(0, 0, 1, 1);
    const [r, g, b] = context.getImageData(0, 0, 1, 1).data;
    return [name, `rgb(${r}, ${g}, ${b})`];
  }));
}

export interface ShapeState { selected: boolean; faded: boolean; hovered: boolean; keyboard: boolean; ringed: boolean }

/** One entity's shape, icon and rings around (0, 0), in screen pixels. */
export function paintShape(ctx: CanvasRenderingContext2D, label: string, r: number, colors: Colors, { selected, faded, hovered, keyboard, ringed }: ShapeState) {
  const { kind, outlined } = nodeKind(label);
  const extent = shapeExtent(label, r);
  const color = faded ? colors["graph-faded"] : kind === "unknown" ? colors["fg-subtle"] : colors[`cat-${kind}`];
  const shape = (radius: number) => new Path2D(shapePath(label, radius));
  const ring = (path: Path2D, stroke: string, width: number, dash: number[] = []) => {
    ctx.setLineDash(dash);
    ctx.strokeStyle = stroke;
    ctx.lineWidth = width;
    ctx.stroke(path);
    ctx.setLineDash([]);
  };
  ctx.lineJoin = "round";
  if (selected) ring(shape(r + 3), colors.mark, 20);
  if (ringed) {
    const hub = new Path2D();
    hub.arc(0, 0, extent + 3, 0, Math.PI * 2);
    ring(hub, colors["fg-muted"], 2);
  }
  const body = shape(r);
  if (outlined || kind === "unknown") {
    // A 3 px --bg rim, then a 2 px outline over a --bg fill.
    ring(body, colors.bg, 6);
    ctx.fillStyle = colors.bg;
    ctx.fill(body);
    ring(body, color, 2, kind === "unknown" ? [3.2, 2.4] : []);
  } else {
    ring(body, colors.bg, 4);
    ctx.fillStyle = color;
    ctx.fill(body);
  }
  if (selected) ring(shape(r + 1.5), colors.fg, 3);
  if (hovered && !selected) ring(shape(r + 3), colors.fg, 2);
  if (keyboard) ring(shape(r + 4), colors.ring, 2, [4, 3]);
  const icon = r >= 10 ? iconPath(label) : null;
  if (icon) {
    const size = r >= 14 ? 16 : 12;
    ctx.translate(-size / 2, -size / 2 + (kind === "thing" ? r * 0.16 : 0));
    ctx.scale(size / 24, size / 24);
    ctx.lineCap = "round"; // lucide draws its dots as zero-length round-capped segments
    ring(icon, outlined || kind === "unknown" ? color : faded ? colors.bg : colors[`on-cat-${kind}`], size < 16 ? 2 : 1.9);
  }
}
