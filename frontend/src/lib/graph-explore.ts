/** Pure Explore layout: the start ring, expansions in `x` order, a fan around each expanded node, and collapse. */
import { displayName, nodeRadius, shapeExtent } from "./graph-encoding";
import { plural } from "./graph-inspector";
import { drawableLinks, FAMILIES, placeNeighbors, polar, type Neighbor, type NodeSummary, type Subgraph, type SubgraphLink, type SummaryLink } from "./graph-layout";

export interface ExploreLimits {
  /** Links on the start ring. */
  start: number;
  /** New entities per expansion. */
  step: number;
  /** Entities in view. */
  entities: number;
}
export const exploreLimits = (phone: boolean): ExploreLimits =>
  phone ? { start: 6, step: 4, entities: 60 } : { start: 8, step: 6, entities: 150 };
export const START_RADIUS = 160;
/** A node at this hop cannot expand; it opens in Focus instead. */
export const MAX_HOP = 3;
const FAN_RADII = [120, 150, 180, 210];
const FAN_TURNS = [0, 12, -12, 24, -24];
/** Graph units kept clear between two shapes, which leaves room for labels. */
const GAP = 32;

export type ExploreEntity = SummaryLink["neighbor"];
export interface ExploreNode extends ExploreEntity {
  x: number;
  y: number;
  hop: number;
  parent: string | null;
  /** Degrees clockwise from 12 o'clock, from the parent to this node. */
  angle: number;
}
export interface ExploreLink { key: string; source: string; target: string; type: string; family: string; link: SubgraphLink }
/** One `x` entry that was laid out: its index in `x`, the node, and the entities it added. */
export interface Expansion { index: number; id: string; added: string[] }
export interface ExploreLayout {
  nodes: ExploreNode[];
  links: ExploreLink[];
  expansions: Expansion[];
  /** The index in `x` of the first expansion whose summary has not arrived; nothing from there on is laid out. */
  pending: number | null;
  /** An expansion stopped at the entity limit. */
  clipped: boolean;
  hops: number;
}

const round = (value: number) => Math.round(value * 1000) / 1000 || 0;
const extent = (entity: ExploreEntity) => shapeExtent(entity.label, nodeRadius(entity.evidence_doc_count));
const familyOrder: string[] = FAMILIES.map(([key]) => key);

/** Each neighbor once, strongest first: supporting documents, latest document date, name, then id (as in Focus). */
function ranked(summary: NodeSummary): Neighbor[] {
  return [...placeNeighbors(summary).values()].flat().sort((a, b) => b.documents - a.documents
    || b.latest.localeCompare(a.latest) || a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
}

/** A summary link with `source` and `target`, as `GET /graph/subgraph` returns it. */
export function asSubgraphLink(summary: NodeSummary, { direction, neighbor, ...link }: SummaryLink): SubgraphLink {
  const [source, target] = direction === "out" ? [summary.focus.id, neighbor.id] : [neighbor.id, summary.focus.id];
  return { ...link, source, target };
}

/** Entities linked to this summary's entity that are not in view. */
export function hiddenNeighbors(summary: NodeSummary, inView: ReadonlySet<string>): number {
  return ranked(summary).filter(neighbor => !inView.has(neighbor.id)).length;
}

/**
 * Lay out the root, its strongest links on a ring, then each expansion in `x` order. The same summaries and `x`
 * give the same positions, and an expansion never moves a placed node. Entries for nodes not in view, at the last
 * hop, or repeated past what the node still has, add nothing. `summaries` are already filtered.
 */
export function exploreLayout(root: NodeSummary, summaries: ReadonlyMap<string, NodeSummary>, x: readonly string[],
  limits: ExploreLimits): ExploreLayout {
  const nodes = new Map<string, ExploreNode>();
  const links = new Map<string, ExploreLink>();
  const place = (entity: ExploreEntity, at: { x: number; y: number }, hop: number, parent: string | null, angle: number) => {
    const { id, label, name, evidence_doc_count, link_count, is_owner } = entity;
    nodes.set(id, { id, label, name, evidence_doc_count, link_count, is_owner, x: at.x, y: at.y, hop, parent, angle });
  };
  // Every link of an expanded entity to an entity in view, including loops back to earlier branches.
  const draw = (summary: NodeSummary) => {
    for (const link of drawableLinks(summary)) {
      if (!nodes.has(link.neighbor.id) || links.has(link.key)) continue;
      const item = asSubgraphLink(summary, link);
      links.set(link.key, { key: link.key, source: item.source, target: item.target, type: link.type, family: link.family, link: item });
    }
  };
  const { focus } = root;
  place(focus, { x: 0, y: 0 }, 0, null, 0);
  // The strongest links, spaced from 12 o'clock in family order, then rank (the sort is stable).
  const start = ranked(root).slice(0, Math.min(limits.start, limits.entities - 1))
    .sort((a, b) => familyOrder.indexOf(a.family) - familyOrder.indexOf(b.family));
  start.forEach((neighbor, index) => {
    const angle = (index * 360) / start.length;
    place(neighbor.links[0].neighbor, polar(angle, START_RADIUS), 1, focus.id, angle);
  });
  draw(root);
  const expansions: Expansion[] = [];
  let pending: number | null = null;
  let clipped = false;
  for (const [index, id] of x.entries()) {
    const node = nodes.get(id);
    if (!node || node.hop >= MAX_HOP) continue;
    const summary = id === focus.id ? root : summaries.get(id);
    if (!summary) {
      pending = index;
      break;
    }
    const fresh = ranked(summary).filter(neighbor => !nodes.has(neighbor.id));
    const kids = fresh.slice(0, Math.max(0, Math.min(limits.step, limits.entities - nodes.size)));
    if (kids.length < Math.min(limits.step, fresh.length)) clipped = true;
    // The root's children use the full ring; any other node fans them around its own direction.
    const span = node.parent === null ? 360 : Math.min(150, 25 * kids.length);
    kids.forEach((kid, slot) => {
      const entity = kid.links[0].neighbor;
      const base = node.parent === null ? ((slot + 0.5) * 360) / kids.length : node.angle - span / 2 + ((slot + 0.5) * span) / kids.length;
      const spot = (angle: number, radius: number) => {
        const offset = polar(angle, radius);
        return { x: round(node.x + offset.x), y: round(node.y + offset.y) };
      };
      const free = (at: { x: number; y: number }) => [...nodes.values()]
        .every(other => Math.hypot(at.x - other.x, at.y - other.y) >= extent(entity) + extent(other) + GAP);
      let chosen: { at: { x: number; y: number }; angle: number } | null = null;
      for (const radius of FAN_RADII) {
        const angle = FAN_TURNS.map(turn => base + turn).find(angle => free(spot(angle, radius)));
        if (angle !== undefined) {
          chosen = { at: spot(angle, radius), angle };
          break;
        }
      }
      chosen ??= { at: spot(base + 24, 210), angle: base + 24 };
      place(entity, chosen.at, node.hop + 1, id, chosen.angle);
    });
    draw(summary);
    expansions.push({ index, id, added: kids.map(kid => kid.id) });
  }
  const placed = [...nodes.values()];
  return { nodes: placed, links: [...links.values()], expansions, pending, clipped, hops: Math.max(...placed.map(node => node.hop)) };
}

/**
 * `x` without the given expansions, later expansions of the same entity (they grow from it), and every later expansion
 * of an entity they added, directly or through others. Entries that added nothing because their node was not in view
 * are dropped too; entries still loading are kept.
 */
export function collapse(layout: ExploreLayout, x: readonly string[], indices: readonly number[]): string[] {
  const removed = new Set(indices);
  const gone = new Set<string>();
  const cut = new Set<string>();
  for (const expansion of layout.expansions) {
    if (removed.has(expansion.index) || gone.has(expansion.id) || cut.has(expansion.id)) {
      removed.add(expansion.index);
      cut.add(expansion.id);
      for (const id of expansion.added) gone.add(id);
    }
  }
  const live = new Set(layout.expansions.map(expansion => expansion.index));
  return x.filter((_, index) => !removed.has(index) && (live.has(index) || (layout.pending !== null && index >= layout.pending)));
}

/** Links among the entities in view that no expanded summary drew, strongest first, at most `cap`. */
export function exploreCrossLinks(layout: ExploreLayout, subgraph: Subgraph, cap: number): { links: ExploreLink[]; total: number } {
  const ids = new Set(layout.nodes.map(node => node.id));
  const seen = new Set(layout.links.map(link => link.key));
  const docs = (link: SubgraphLink) => new Set(link.records.map(record => record.doc_id)).size;
  const kept = subgraph.links.filter(link => {
    if (link.family === "evidence" || link.source === link.target || !ids.has(link.source) || !ids.has(link.target) || seen.has(link.key)) return false;
    seen.add(link.key);
    return true;
  }).sort((a, b) => docs(b) - docs(a) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return {
    links: kept.slice(0, cap).map(link => ({ key: link.key, source: link.source, target: link.target, type: link.type, family: link.family, link })),
    total: kept.length,
  };
}

/** "Explore from <root>: N entities, L links, H hops." */
export function exploreName(root: NodeSummary["focus"], entities: number, links: number, hops: number): string {
  return `Explore from ${displayName(root.name, root.id, root.label)}: ${plural(entities, "entity", "entities")}, ${plural(links, "link")}, ${plural(hops, "hop")}.`;
}
