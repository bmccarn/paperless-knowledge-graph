/** Pure neighborhood layout: ranking, caps, "+N more", placement and fixed radial sectors. */

/** Drawn families in sector order; `evidence` feeds the evidence shelf and is never drawn. */
export const FAMILIES = [
  ["parties", "Parties and roles"],
  ["money", "Money and accounts"],
  ["coverage", "Coverage and obligations"],
  ["health", "Health"],
  ["service", "Service and government"],
  ["places", "Places"],
  ["time", "Time"],
  ["doclinks", "Document links"],
  ["loose", "Loose association"],
  ["other", "Other"],
] as const;
export const RING_RADIUS = 210;
/** Pills sit just outside the ring, clear of the neighbor labels, as in the approved mockup. */
export const PILL_RADIUS = RING_RADIUS + 34;
export const MORE_STEP = 25;
export const VIEW_LIMIT = 150;

export interface SummaryRecord { doc_id: number; inferred: boolean; confidence: number | null }
export interface SummaryLink {
  key: string;
  type: string;
  family: string;
  direction: "in" | "out";
  support_state: "supported" | "inferred" | "mixed";
  stated_count: number;
  inferred_count: number;
  records: SummaryRecord[];
  neighbor: { id: string; label: string; name: string; evidence_doc_count: number; link_count: number; is_owner: boolean };
}
export interface NodeSummary {
  focus: { id: string; label: string; name: string; aliases: string[]; evidence_doc_count: number;
           link_count: number; is_hub: boolean; is_owner: boolean };
  links: SummaryLink[];
  documents: Record<string, { title: string; date: string | null; doc_type: string | null }>;
  owner_status: string;
}

/** Neighbors per family before "+N more": 2 for a hub, else 6 (5 on phone). */
export function familyCap(isHub: boolean, phone: boolean): number {
  return isHub ? 2 : phone ? 5 : 6;
}

export interface Neighbor {
  id: string;
  label: string;
  name: string;
  family: string;
  /** Links to this neighbor in its own family; links in its other families stay off the canvas. */
  links: SummaryLink[];
  documents: number;
  latest: string;
}

const ORDER: string[] = FAMILIES.map(([key]) => key);
const familyIndex = (family: string) => (ORDER.includes(family) ? ORDER.indexOf(family) : ORDER.length - 1);
const drawnFamily = (family: string) => ORDER[familyIndex(family)];

/** Entity-to-entity links only: no evidence family, no Document neighbor, no self link. */
export function drawableLinks(summary: NodeSummary): SummaryLink[] {
  return summary.links.filter(link => link.family !== "evidence" && link.neighbor.label !== "Document"
    && link.neighbor.id !== summary.focus.id);
}

/**
 * Each neighbor once, in the family whose links to it have the most evidence documents
 * (ties: earlier family). Ranked by documents, latest document date, name, then id.
 */
export function placeNeighbors(summary: NodeSummary): Map<string, Neighbor[]> {
  const byNeighbor = new Map<string, Map<string, SummaryLink[]>>();
  for (const link of drawableLinks(summary)) {
    const families = byNeighbor.get(link.neighbor.id) ?? new Map<string, SummaryLink[]>();
    const family = drawnFamily(link.family);
    families.set(family, [...(families.get(family) ?? []), link]);
    byNeighbor.set(link.neighbor.id, families);
  }
  const placed = new Map<string, Neighbor[]>();
  for (const families of byNeighbor.values()) {
    const candidates = [...families].map(([family, links]) => {
      const docs = new Set(links.flatMap(link => link.records.map(record => record.doc_id)));
      const latest = [...docs].map(id => summary.documents[String(id)]?.date || "").sort().pop() || "";
      const { id, label, name } = links[0].neighbor;
      return { id, label, name, family, links: [...links].sort((a, b) => a.key.localeCompare(b.key)), documents: docs.size, latest };
    });
    candidates.sort((a, b) => b.documents - a.documents || familyIndex(a.family) - familyIndex(b.family));
    const best = candidates[0];
    placed.set(best.family, [...(placed.get(best.family) ?? []), best]);
  }
  for (const members of placed.values())
    members.sort((a, b) => b.documents - a.documents || b.latest.localeCompare(a.latest)
      || a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
  return placed;
}

export interface LayoutNode {
  id: string;
  kind: "focus" | "neighbor" | "pill";
  label: string;
  name: string;
  family: string | null;
  x: number;
  y: number;
  /** Hidden links of the family, on a pill. */
  count?: number;
}
export interface LayoutLink { key: string; source: string; target: string; type: string; family: string; link: SummaryLink | null }
export interface LayoutSector {
  family: string;
  name: string;
  /** Degrees clockwise from 12 o'clock. */
  start: number;
  end: number;
  total: number;
  drawn: number;
  hidden: number;
  members: number;
  shown: number;
}
export interface NeighborhoodLayout {
  nodes: LayoutNode[];
  links: LayoutLink[];
  sectors: LayoutSector[];
  /** Focus plus drawn neighbors; pills do not count. */
  entities: number;
}

const polar = (degrees: number, radius: number) => {
  const angle = (degrees * Math.PI) / 180;
  // Rounding keeps positions byte-identical across engines and reloads.
  const round = (value: number) => Math.round(value * 1000) / 1000 || 0;
  return { x: round(radius * Math.sin(angle)), y: round(-radius * Math.cos(angle)) };
};

/**
 * Lay out the focus and its capped neighbors. `extra` holds the neighbors added per family
 * by "+N more". Pill counts plus drawn links equal each family's link total.
 */
export function neighborhoodLayout(summary: NodeSummary, cap: number, extra: Record<string, number> = {}): NeighborhoodLayout {
  const placed = placeNeighbors(summary);
  const totals = new Map<string, number>();
  for (const link of drawableLinks(summary)) totals.set(drawnFamily(link.family), (totals.get(drawnFamily(link.family)) ?? 0) + 1);
  const present = ORDER.filter(family => totals.has(family));
  const width = present.length ? 360 / present.length : 360;
  const { focus } = summary;
  const nodes: LayoutNode[] = [{ id: focus.id, kind: "focus", label: focus.label, name: focus.name, family: null, x: 0, y: 0 }];
  const links: LayoutLink[] = [];
  const sectors: LayoutSector[] = [];
  present.forEach((family, index) => {
    const members = placed.get(family) ?? [];
    const shown = members.slice(0, cap + (extra[family] ?? 0));
    const drawn = shown.reduce((sum, member) => sum + member.links.length, 0);
    const total = totals.get(family)!;
    const hidden = total - drawn;
    const start = index * width - width / 2;
    const slots = shown.length + (hidden ? 1 : 0);
    const angle = (slot: number) => start + ((slot + 0.5) / slots) * width;
    shown.forEach((member, slot) => {
      nodes.push({ id: member.id, kind: "neighbor", label: member.label, name: member.name, family, ...polar(angle(slot), RING_RADIUS) });
      for (const link of member.links) {
        const [source, target] = link.direction === "out" ? [focus.id, member.id] : [member.id, focus.id];
        links.push({ key: link.key, source, target, type: link.type, family, link });
      }
    });
    if (hidden) {
      const id = `pill:${family}`;
      nodes.push({ id, kind: "pill", label: "Aggregate", name: `+${hidden} ${FAMILIES[ORDER.indexOf(family)][1]}`,
        family, count: hidden, ...polar(angle(shown.length), PILL_RADIUS) });
      links.push({ key: `${focus.id}~${id}`, source: focus.id, target: id, type: "AGGREGATE", family, link: null });
    }
    sectors.push({ family, name: FAMILIES[ORDER.indexOf(family)][1], start, end: start + width, total, drawn, hidden,
      members: members.length, shown: shown.length });
  });
  return { nodes, links, sectors, entities: nodes.filter(node => node.kind !== "pill").length };
}

/**
 * One press of "+N more": add up to 25 hidden neighbors of the family, never past 150
 * entities in view. Returns the same object when nothing can be added.
 */
export function showMore(summary: NodeSummary, cap: number, extra: Record<string, number>, family: string): Record<string, number> {
  const layout = neighborhoodLayout(summary, cap, extra);
  const sector = layout.sectors.find(item => item.family === family);
  if (!sector) return extra;
  const added = Math.min(MORE_STEP, sector.members - sector.shown, VIEW_LIMIT - layout.entities);
  return added > 0 ? { ...extra, [family]: (extra[family] ?? 0) + added } : extra;
}
