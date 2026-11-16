/** Shared graph filters: document records are reduced before link-level predicates run. */
import { drawableLinks, FAMILIES, MORE_STEP, neighborhoodLayout, VIEW_LIMIT, type NeighborhoodLayout, type NodeSummary, type SubgraphLink, type SummaryLink } from "./graph-layout";
import type { GraphView } from "./graph-url-state";

export interface GraphFilters {
  families: string[];
  support: "all" | "supported" | "inferred";
  minConfidence: number;
  noScore: boolean;
  minDocs: number;
  from: string;
  to: string;
  types: string[] | null;
  owner: "show" | "hide" | null;
}
const familyKeys = FAMILIES.map(([key]) => key) as string[];
export const defaultFilters = (): GraphFilters => ({ families: [...familyKeys], support: "all", minConfidence: 0,
  noScore: true, minDocs: 1, from: "", to: "", types: null, owner: null });
const month = (value: string | null) => /^\d{4}-(0[1-9]|1[0-2])$/.test(value ?? "") ? value! : "";

export function parseFilters(params: URLSearchParams): GraphFilters {
  const defaults = defaultFilters();
  const confidence = Number(params.get("minconf"));
  const documents = Number(params.get("mindocs"));
  return { ...defaults,
    families: params.has("fam") ? params.get("fam")!.split(",").filter(key => familyKeys.includes(key)) : defaults.families,
    support: params.get("support") === "supported" ? "supported" : params.get("support") === "inferred" ? "inferred" : "all",
    minConfidence: Number.isFinite(confidence) && confidence >= 0 && confidence <= 1 ? confidence : 0,
    noScore: params.get("noscore") !== "0",
    minDocs: [1, 2, 3, 5, 10].includes(documents) ? documents : 1,
    from: month(params.get("from")), to: month(params.get("to")),
    types: params.has("dtype") ? params.get("dtype")!.split(",").filter(Boolean) : null,
    owner: params.get("owner") === "hide" ? "hide" : params.get("owner") === "show" ? "show" : null,
  };
}

export const ownerHidden = (filters: GraphFilters, view: GraphView = "focus") =>
  view === "overview" || (filters.owner ? filters.owner === "hide" : view === "explore" || view === "path");

export function filterUrl(current: URLSearchParams, filters: GraphFilters): string {
  const params = new URLSearchParams(current);
  const values = { fam: familyKeys.every(key => filters.families.includes(key)) ? null : filters.families.join(","),
    support: filters.support === "all" ? null : filters.support,
    minconf: filters.minConfidence ? filters.minConfidence.toFixed(2) : null, noscore: filters.noScore ? null : "0",
    mindocs: filters.minDocs === 1 ? null : String(filters.minDocs), from: filters.from || null, to: filters.to || null,
    dtype: filters.types?.join(",") ?? null, owner: filters.owner };
  for (const [key, value] of Object.entries(values)) {
    if (value === null) params.delete(key);
    else params.set(key, value);
  }
  return `/graph${params.size ? `?${params}` : ""}`;
}

export function filterLinks<T extends SummaryLink | SubgraphLink>(links: T[], documents: NodeSummary["documents"],
  filters: GraphFilters, ownerIds: Set<string> = new Set(), view: GraphView = "focus"): T[] {
  const overview = view === "overview";
  return links.flatMap(link => {
    if (!filters.families.includes(link.family) || ("neighbor" in link && link.neighbor.label === "Document")) return [];
    if (ownerHidden(filters, view) && ("neighbor" in link ? link.neighbor.is_owner : ownerIds.has(link.source) || ownerIds.has(link.target))) return [];
    const records = overview || (!filters.from && !filters.to && filters.types === null) ? link.records : link.records.filter(record => {
      const document = documents[String(record.doc_id)];
      const date = document?.date?.slice(0, 7);
      return (!filters.from || Boolean(date && date >= filters.from)) && (!filters.to || Boolean(date && date <= filters.to))
        && (filters.types === null || Boolean(document?.doc_type && filters.types.includes(document.doc_type)));
    });
    if (!records.length || new Set(records.map(record => record.doc_id)).size < filters.minDocs) return [];
    const stated = records.filter(record => !record.inferred).length;
    const inferred = records.length - stated;
    const support_state = stated ? inferred ? "mixed" : "supported" : "inferred";
    if ((filters.support === "supported" && !stated) || (filters.support === "inferred" && stated)) return [];
    const confidence = records.some(record => record.confidence === null) ? null : Math.min(...records.map(record => record.confidence!));
    if (!overview && (confidence === null ? !filters.noScore : confidence < filters.minConfidence)) return [];
    return [{ ...link, records, stated_count: stated, inferred_count: inferred, support_state }];
  });
}

/** Keep the original slots while removing hidden neighbors; "+N more" still extends the original ring. */
export function filterLayout(layout: NeighborhoodLayout, summary: NodeSummary): NeighborhoodLayout {
  const semantic = drawableLinks(summary);
  const matching = new Map(semantic.map(link => [link.key, link]));
  const links = layout.links.filter(item => !item.link || matching.has(item.key)).map(item => item.link ? { ...item, link: matching.get(item.key)! } : item);
  // If a neighbor's primary family is hidden, keep its slot for a matching link in another family.
  const drawnIds = new Set(layout.nodes.filter(node => node.kind === "neighbor").map(node => node.id));
  const keptIds = new Set(links.filter(item => item.link).map(item => item.link!.neighbor.id));
  for (const link of semantic) if (drawnIds.has(link.neighbor.id) && !keptIds.has(link.neighbor.id)) {
    const [source, target] = link.direction === "out" ? [summary.focus.id, link.neighbor.id] : [link.neighbor.id, summary.focus.id];
    links.push({ key: link.key, source, target, type: link.type, family: link.family, link });
  }
  const neighbors = new Set(links.filter(item => item.link).flatMap(item => [item.source, item.target]));
  const sectors = layout.sectors.flatMap(sector => {
    const total = semantic.filter(link => link.family === sector.family).length;
    const drawn = links.filter(link => link.family === sector.family && link.link).length;
    const hidden = total - drawn;
    return total ? [{ ...sector, total, drawn, hidden, shown: layout.nodes.filter(node => node.family === sector.family && neighbors.has(node.id)).length }] : [];
  });
  const nodes = layout.nodes.flatMap(node => {
    if (node.kind === "focus" || neighbors.has(node.id)) return [node];
    const sector = sectors.find(sector => sector.family === node.family);
    return node.kind === "pill" && sector?.hidden ? [{ ...node, count: sector.hidden, name: `+${sector.hidden} ${sector.name}` }] : [];
  });
  const ids = new Set(nodes.map(node => node.id));
  return { nodes, sectors, links: links.filter(link => ids.has(link.source) && ids.has(link.target)), entities: nodes.filter(node => node.kind !== "pill").length };
}

/** Extend through hidden records until up to 25 matching links are added, with at most 150 visible entities. */
export function showMoreFiltered(raw: NodeSummary, filtered: NodeSummary, cap: number, extra: Record<string, number>, family: string): Record<string, number> {
  const base = neighborhoodLayout(raw, cap, extra);
  const before = filterLayout(base, filtered).sectors.find(sector => sector.family === family)?.drawn ?? 0;
  let low = 0;
  let high = base.sectors.find(sector => sector.family === family)?.hidden ?? 0;
  while (low < high) {
    const added = Math.ceil((low + high) / 2);
    const next = { ...extra, [family]: (extra[family] ?? 0) + added };
    const original = neighborhoodLayout(raw, cap, next);
    const layout = filterLayout(original, filtered);
    const drawn = (layout.sectors.find(sector => sector.family === family)?.drawn ?? 0) - before;
    // Keep the original slot budget capped too: clearing filters must never reveal more than 150 entities.
    if (original.entities > VIEW_LIMIT || drawn > MORE_STEP) high = added - 1;
    else low = added;
  }
  const next = { ...extra, [family]: (extra[family] ?? 0) + low };
  const after = filterLayout(neighborhoodLayout(raw, cap, next), filtered).sectors.find(sector => sector.family === family)?.drawn ?? 0;
  return after > before ? next : extra;
}
