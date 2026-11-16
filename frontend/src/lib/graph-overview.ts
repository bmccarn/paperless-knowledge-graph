/** Overview: groups by label propagation and a fixed map layout, from `GET /graph/overview`. Pure; runs in a Web Worker. */
import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation } from "d3-force-3d";
import { nodeRadius } from "./graph-encoding";
import type { GraphFilters } from "./graph-filters";

export interface OverviewResponse {
  version: string;
  columns: string[];
  /** [id, label, name, evidence_doc_count, link_count], sorted by id. */
  nodes: unknown[][];
  link_columns: string[];
  /** [source, target, weight, support, families]: one row per pair, by node index. */
  links: number[][];
  owner: number | null;
  unlinked: number;
  truncated: boolean;
}

export interface OverviewNode { id: string; label: string; name: string; documents: number; links: number }
/** A pair of entities with all their links merged: support 0 supported, 1 mixed, 2 inferred. */
export interface OverviewLink { source: number; target: number; weight: number; support: 0 | 1 | 2; families: number }

/** The bit order of `families`: the fixed family order without `evidence`. */
export const OVERVIEW_FAMILIES = ["parties", "money", "coverage", "health", "service", "places", "time", "doclinks", "loose", "other"];
/** A label shared by at least this many entities is a group; smaller sets go to "Small groups". */
export const GROUP_MIN = 4;
const ROUNDS = 20;
const COLLIDE_TICKS = 40;
/** Graph units between two group circles, so a group's name fits above it. */
const GROUP_GAP = 36;
/** The small groups band: one cell per set, in rows above the map. */
const CELL = 26;
const ROW = 22;

export function readOverview(response: OverviewResponse): { nodes: OverviewNode[]; links: OverviewLink[] } {
  const at = (name: string) => response.columns.indexOf(name);
  const [id, label, name, documents, links] = ["id", "label", "name", "evidence_doc_count", "link_count"].map(at);
  const linkAt = ["source", "target", "weight", "support", "families"].map(column => response.link_columns.indexOf(column));
  return {
    nodes: response.nodes.map(row => ({ id: String(row[id]), label: String(row[label]), name: String(row[name] ?? ""),
      documents: Number(row[documents]) || 0, links: Number(row[links]) || 0 })),
    links: response.links.map(row => {
      const [source, target, weight, support, families] = linkAt.map(index => row[index]);
      if (!(source >= 0 && source < response.nodes.length && target >= 0 && target < response.nodes.length))
        throw new Error(`A link names node ${source} or ${target}, outside the ${response.nodes.length} nodes.`);
      return { source, target, weight: weight || 0, support: (support === 1 || support === 2 ? support : 0) as 0 | 1 | 2, families: families || 0 };
    }),
  };
}

/**
 * The pairs the map draws: never the owner's; the family filter hides a pair only when none of its families is shown.
 * Dates, document types and confidence do not apply to merged pairs.
 */
export function overviewLinks(links: OverviewLink[], owner: number | null, filters: Pick<GraphFilters, "families" | "support" | "minDocs">): OverviewLink[] {
  const shown = OVERVIEW_FAMILIES.reduce((mask, family, bit) => filters.families.includes(family) ? mask | (1 << bit) : mask, 0);
  return links.filter(link => link.source !== owner && link.target !== owner && (link.families & shown) !== 0
    && (filters.support === "all" || (filters.support === "supported" ? link.support !== 2 : link.support === 2))
    // A pair without document records stays at the default of 1 document.
    && (filters.minDocs <= 1 || link.weight >= filters.minDocs));
}

/**
 * Label propagation: nodes are visited in index (id) order and updated in place; a node takes the label most common
 * among its neighbors, ties to the smaller label. Stops after 20 rounds or when nothing changes. Deterministic.
 * `start` gives each node its first label (by default its index).
 */
export function propagateLabels(count: number, links: Pick<OverviewLink, "source" | "target">[], start?: Int32Array, rounds = ROUNDS): Int32Array {
  const neighbors: number[][] = Array.from({ length: count }, () => []);
  for (const { source, target } of links) {
    neighbors[source].push(target);
    neighbors[target].push(source);
  }
  const labels = start ? Int32Array.from(start) : Int32Array.from({ length: count }, (_, index) => index);
  const tally = new Map<number, number>();
  for (let round = 0; round < rounds; round++) {
    let changed = false;
    for (let node = 0; node < count; node++) {
      if (!neighbors[node].length) continue;
      tally.clear();
      for (const other of neighbors[node]) tally.set(labels[other], (tally.get(labels[other]) ?? 0) + 1);
      let best = -1, most = 0;
      for (const [label, votes] of tally) if (votes > most || (votes === most && label < best)) [best, most] = [label, votes];
      if (best !== labels[node]) {
        labels[node] = best;
        changed = true;
      }
    }
    if (!changed) break;
  }
  return labels;
}

export interface KindCount { label: string; count: number }
export interface OverviewGroup {
  /** The lead entity's id: the group's key in the URL. */
  id: string;
  lead: number;
  /** Node indexes, lead first, then by evidence documents, links and id. */
  members: number[];
  links: number;
  kinds: KindCount[];
  x: number;
  y: number;
  r: number;
}
export interface OverviewMap {
  nodes: OverviewNode[];
  links: OverviewLink[];
  /** x, y per node index; NaN for an entity that is not drawn. */
  positions: Float64Array;
  groups: OverviewGroup[];
  /** Sets of 2 or 3 entities (1 only if propagation stopped early), largest first, then name. */
  small: number[][];
  /** Entities, not drawn, whose only entity links lead to the owner. */
  ownerOnly: number;
  /** Entities, not drawn, whose links the filters hide. */
  hidden: number;
  band: { x0: number; y0: number; x1: number; y1: number } | null;
  bounds: { x0: number; y0: number; x1: number; y1: number };
  /** Group index per node; -1 in the small groups band or not drawn. */
  groupOf: Int32Array;
  timings: { groups: number; layout: number };
}

/** The lead order: more evidence documents, then more links, then the smaller id (index). */
export const leadOrder = (nodes: OverviewNode[]) => (a: number, b: number) =>
  nodes[b].documents - nodes[a].documents || nodes[b].links - nodes[a].links || a - b;

const nameKey = (node: OverviewNode) => node.name.toLowerCase();

/**
 * A group laid out on its own by the d3-force-3d simulation, from its phyllotaxis start in id order; centered on 0, 0.
 * Afterwards the collide force moves each entity's shape, at its detail zoom size, clear of the others.
 */
export function layoutGroup(members: number[], links: OverviewLink[], entities: OverviewNode[]): { x: number[]; y: number[]; r: number } {
  const order = [...members].sort((a, b) => a - b);
  const local = new Map(order.map((node, index) => [node, index]));
  const nodes: { index?: number; x?: number; y?: number }[] = order.map(() => ({}));
  const edges = links.filter(link => local.has(link.source) && local.has(link.target))
    .map(link => ({ source: local.get(link.source)!, target: local.get(link.target)! }));
  const simulation = forceSimulation(nodes, 2)
    .force("link", forceLink(edges))
    .force("charge", forceManyBody())
    .force("center", forceCenter())
    .stop();
  simulation.tick(Math.min(300, 60 + members.length));
  // Then the collide force alone, which is costly, spreads shapes that overlap; the links keep their shape.
  simulation.force("charge", null).force("center", null)
    .force("collide", forceCollide((node: { index: number }) => nodeRadius(entities[order[node.index]].documents) + 8))
    .tick(COLLIDE_TICKS);
  const cx = nodes.reduce((sum, node) => sum + node.x!, 0) / nodes.length;
  const cy = nodes.reduce((sum, node) => sum + node.y!, 0) / nodes.length;
  const x: number[] = [], y: number[] = [];
  let r = 0;
  for (const node of members) {
    const at = nodes[local.get(node)!];
    x.push(at.x! - cx);
    y.push(at.y! - cy);
    r = Math.max(r, Math.hypot(at.x! - cx, at.y! - cy));
  }
  return { x, y, r: r + 12 };
}

/**
 * Circles placed largest first along a spiral from the center, each at the first spot where it touches no other.
 * A step inside another circle skips ahead by the overlap, which never passes a free spot. Radii must not increase.
 */
export function packCircles(radii: number[], gap = GROUP_GAP): { x: number; y: number }[] {
  const placed: { x: number; y: number; r: number }[] = [];
  const pitch = 2 * gap;
  const a = pitch / (2 * Math.PI);
  // A grid of placed circles: a later circle is no larger, so it can touch circle i only within 2 r_i + gap of its center.
  const CELL = 64;
  const grid = new Map<number, number[]>();
  const key = (gx: number, gy: number) => gx * 1e6 + gy;
  for (const r of radii) {
    let theta = 0;
    for (;;) {
      const rho = a * theta;
      const x = rho * Math.cos(theta), y = rho * Math.sin(theta);
      let overlap = 0;
      for (const index of grid.get(key(Math.floor(x / CELL), Math.floor(y / CELL))) ?? []) {
        const other = placed[index];
        overlap = Math.max(overlap, r + other.r + gap - Math.hypot(x - other.x, y - other.y));
      }
      if (overlap <= 0) {
        const reach = 2 * r + gap;
        for (let gx = Math.floor((x - reach) / CELL); gx <= Math.floor((x + reach) / CELL); gx++)
          for (let gy = Math.floor((y - reach) / CELL); gy <= Math.floor((y + reach) / CELL); gy++) {
            const cell = grid.get(key(gx, gy));
            if (cell) cell.push(placed.length);
            else grid.set(key(gx, gy), [placed.length]);
          }
        placed.push({ x, y, r });
        break;
      }
      // Steps of half the radius sample the spiral finer than any circle; arc length to angle: ds = sqrt(rho^2 + a^2) dtheta.
      theta += Math.max(overlap, r / 2) / Math.hypot(rho, a);
    }
  }
  return placed.map(({ x, y }) => ({ x, y }));
}

/** Groups, the per-group layout and the small groups band, for one response and filter. */
export function arrangeOverview(response: OverviewResponse, filters: Pick<GraphFilters, "families" | "support" | "minDocs">, now = () => performance.now()): OverviewMap {
  const start = now();
  const { nodes, links: all } = readOverview(response);
  const owner = response.owner !== null && response.owner >= 0 && response.owner < nodes.length ? response.owner : null;
  const links = overviewLinks(all, owner, filters);
  const lead = leadOrder(nodes);
  // Each entity starts with its rank in the lead order, so a tie goes to the side of the better documented entity:
  // two neighbors of a hub that also link to each other stay in the hub's group.
  const rank = new Int32Array(nodes.length);
  nodes.map((_, index) => index).sort(lead).forEach((node, at) => { rank[node] = at; });
  const labels = propagateLabels(nodes.length, links, rank);
  // Only entities with a shown link are drawn; the rest are counted.
  const shown = new Uint8Array(nodes.length);
  for (const link of links) shown[link.source] = shown[link.target] = 1;
  const sets = new Map<number, number[]>();
  nodes.forEach((_, index) => {
    if (!shown[index]) return;
    const set = sets.get(labels[index]);
    if (set) set.push(index);
    else sets.set(labels[index], [index]);
  });
  const ranked = [...sets.values()].map(members => members.sort(lead));
  const groupOf = new Int32Array(nodes.length).fill(-1);
  const big = ranked.filter(members => members.length >= GROUP_MIN)
    .sort((a, b) => b.length - a.length || lead(a[0], b[0]));
  big.forEach((members, group) => { for (const node of members) groupOf[node] = group; });
  const inner = new Int32Array(big.length);
  for (const link of links) if (groupOf[link.source] >= 0 && groupOf[link.source] === groupOf[link.target]) inner[groupOf[link.source]]++;
  const small = ranked.filter(members => members.length < GROUP_MIN)
    .sort((a, b) => b.length - a.length || (nameKey(nodes[a[0]]) < nameKey(nodes[b[0]]) ? -1 : nameKey(nodes[a[0]]) > nameKey(nodes[b[0]]) ? 1 : a[0] - b[0]));
  const linked = new Uint8Array(nodes.length);
  for (const link of all) if (link.source !== owner && link.target !== owner) linked[link.source] = linked[link.target] = 1;
  const ownerOnly = nodes.filter((_, index) => index !== owner && !linked[index]).length;
  const hidden = nodes.filter((_, index) => linked[index] && !shown[index]).length;
  const grouped = now();

  const positions = new Float64Array(nodes.length * 2).fill(NaN);
  const shapes = big.map(members => layoutGroup(members, links, nodes));
  const centers = packCircles(shapes.map(shape => shape.r));
  const groups: OverviewGroup[] = big.map((members, index) => {
    const { x, y, r } = shapes[index];
    const center = centers[index];
    members.forEach((node, k) => {
      positions[node * 2] = center.x + x[k];
      positions[node * 2 + 1] = center.y + y[k];
    });
    const kinds = new Map<string, number>();
    for (const node of members) kinds.set(nodes[node].label, (kinds.get(nodes[node].label) ?? 0) + 1);
    return { id: nodes[members[0]].id, lead: members[0], members, links: inner[index], x: center.x, y: center.y, r,
      kinds: [...kinds].map(([label, count]) => ({ label, count })).sort((p, q) => q.count - p.count || (p.label < q.label ? -1 : 1)) };
  });
  const bounds = groups.length
    ? { x0: Math.min(...groups.map(g => g.x - g.r)), y0: Math.min(...groups.map(g => g.y - g.r)),
        x1: Math.max(...groups.map(g => g.x + g.r)), y1: Math.max(...groups.map(g => g.y + g.r)) }
    : { x0: -240, y0: 0, x1: 240, y1: 0 };
  let band = null;
  if (small.length) {
    // A band above the map, as wide as the map (at least 24 cells), in rows of cells.
    const perRow = Math.max(24, Math.floor((bounds.x1 - bounds.x0) / CELL));
    const rows = Math.ceil(small.length / perRow);
    const y0 = (groups.length ? bounds.y0 - GROUP_GAP * 1.5 : 0) - rows * ROW - 28;
    const x0 = groups.length ? bounds.x0 : -perRow * CELL / 2;
    small.forEach((members, index) => {
      const cx = x0 + (index % perRow) * CELL + CELL / 2, cy = y0 + 28 + Math.floor(index / perRow) * ROW + ROW / 2;
      // One dot, two side by side, or three in a triangle.
      const spots = [[[0, 0]], [[-4, 0], [4, 0]], [[-4.5, 3], [4.5, 3], [0, -4.5]]][members.length - 1];
      members.forEach((node, k) => {
        positions[node * 2] = cx + spots[k][0];
        positions[node * 2 + 1] = cy + spots[k][1];
      });
    });
    band = { x0, y0, x1: x0 + Math.min(perRow, small.length) * CELL, y1: y0 + 28 + rows * ROW };
  }
  const box = band && groups.length
    ? { x0: Math.min(bounds.x0, band.x0), y0: band.y0, x1: Math.max(bounds.x1, band.x1), y1: bounds.y1 }
    : band ?? bounds;
  return { nodes, links, positions, groups, small, ownerOnly, hidden, band, bounds: box, groupOf,
    timings: { groups: grouped - start, layout: now() - grouped } };
}

/** "Person, organization, financial item": the three most common kinds of a group, in words. */
export const mainKinds = (kinds: KindCount[], words: (label: string) => string) => kinds.slice(0, 3).map(kind => words(kind.label)).join(", ");
