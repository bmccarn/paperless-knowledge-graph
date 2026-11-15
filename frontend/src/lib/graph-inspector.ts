/** Pure inspector, evidence shelf and legend data, from the node summary and the drawn layout. */
import { NODE_LABELS, nodeKind } from "./categories";
import { displayName } from "./graph-encoding";
import { drawableLinks, FAMILIES, type NeighborhoodLayout, type NodeSummary, type SummaryLink } from "./graph-layout";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "2026-01-31" becomes "31 Jan 2026"; the stored date has no time zone to shift. */
export function shortDate(value: string | null | undefined): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(value || "");
  return match ? `${Number(match[3])} ${MONTHS[Number(match[2]) - 1]} ${match[1]}` : "";
}

/** "PREMIUM_PAID_TO" becomes "Premium paid to". */
export const typeWords = (type: string) => {
  const words = type.replace(/_/g, " ").toLowerCase();
  return words.charAt(0).toUpperCase() + words.slice(1);
};

/** "Supported", "Inferred", or "Stated in X, inferred in Y" for a mixed link. */
export function supportText(link: Pick<SummaryLink, "support_state" | "stated_count" | "inferred_count">): string {
  if (link.support_state === "mixed") return `Stated in ${link.stated_count}, inferred in ${link.inferred_count}`;
  return link.support_state === "inferred" ? "Inferred" : "Supported";
}

export const plural = (count: number, one: string, many = `${one}s`) => `${count} ${count === 1 ? one : many}`;

export interface SummaryDocument { id: number; title: string; date: string | null; doc_type: string | null }

/** The summary's documents, newest first; undated last; then title and id, so equal dates keep one order. */
export function documentsByDate(summary: NodeSummary): SummaryDocument[] {
  return Object.entries(summary.documents)
    .map(([id, doc]) => ({ id: Number(id), title: doc.title || "Untitled document", date: doc.date, doc_type: doc.doc_type }))
    .sort((a, b) => (b.date || "").localeCompare(a.date || "") || a.title.localeCompare(b.title) || a.id - b.id);
}

/** "Seen in N documents from <first> to <last>. K links are inferred." */
export function supportSummary(summary: NodeSummary): string {
  const documents = documentsByDate(summary);
  const dates = documents.map(doc => doc.date).filter((date): date is string => Boolean(shortDate(date))).sort();
  const first = shortDate(dates[0]), last = shortDate(dates[dates.length - 1]);
  const seen = `Seen in ${plural(documents.length, "document")}`;
  const span = !first ? "." : first === last ? `, all dated ${first}.` : ` from ${first} to ${last}.`;
  const inferred = drawableLinks(summary).filter(link => link.support_state === "inferred").length;
  return `${seen}${span}${inferred ? ` ${plural(inferred, "link")} ${inferred === 1 ? "is" : "are"} inferred.` : ""}`;
}

const latest = (summary: NodeSummary, link: SummaryLink) =>
  link.records.map(record => summary.documents[String(record.doc_id)]?.date || "").sort().pop() || "";

/** Rank links like the canvas: supporting documents, then latest document date, then name and key. */
export function rankLinks(summary: NodeSummary, links: SummaryLink[]): SummaryLink[] {
  return [...links].sort((a, b) => b.records.length - a.records.length || latest(summary, b).localeCompare(latest(summary, a))
    || a.neighbor.name.localeCompare(b.neighbor.name) || a.key.localeCompare(b.key));
}

export interface FamilyGroup { family: string; name: string; links: SummaryLink[]; inferred: number }

/** Entity-to-entity links by family in sector order, ranked; unknown families count as Other, as on the canvas. */
export function linksByFamily(summary: NodeSummary): FamilyGroup[] {
  const order: string[] = FAMILIES.map(([key]) => key);
  const groups = new Map<string, SummaryLink[]>();
  for (const link of drawableLinks(summary)) {
    const family = order.includes(link.family) ? link.family : "other";
    groups.set(family, [...(groups.get(family) ?? []), link]);
  }
  return FAMILIES.filter(([key]) => groups.has(key)).map(([family, name]) => {
    const links = rankLinks(summary, groups.get(family)!);
    return { family, name, links, inferred: links.filter(link => link.support_state === "inferred").length };
  });
}

/** Up to 4 key facts: the strongest link of parties, money, coverage and time, in that order. */
export function keyFacts(summary: NodeSummary): SummaryLink[] {
  const groups = linksByFamily(summary);
  return ["parties", "money", "coverage", "time"]
    .map(family => groups.find(group => group.family === family)?.links[0])
    .filter((link): link is SummaryLink => Boolean(link));
}

/** Keys of the links whose records cite a document. */
export function linksOfDocument(summary: NodeSummary, docId: number): Set<string> {
  return new Set(summary.links.filter(link => link.records.some(record => record.doc_id === docId)).map(link => link.key));
}

/** A neighbor or the focus by id, for the inspector, with its display name. */
export function entityName(entity: { id: string; name: string; label: string }): string {
  return displayName(entity.name, entity.id, entity.label);
}

/** The question "Ask about this" puts in the composer. */
export const askAbout = (name: string) => `What do my documents say about ${name}?`;

export type LinkStyle = "supported" | "inferred" | "aggregate";
export interface LegendItems { labels: string[]; styles: LinkStyle[] }

/**
 * Only what is drawn: each node label once in category order (labels outside the kinds collapse to one
 * "Other" entry), then the link styles in view.
 */
export function legendItems(layout: Pick<NeighborhoodLayout, "nodes" | "links">): LegendItems {
  const seen = new Set(layout.nodes.filter(node => node.kind !== "pill").map(node => node.label));
  const known = NODE_LABELS.filter(label => seen.has(label));
  const other = [...seen].some(label => nodeKind(label).kind === "unknown");
  const styles: LinkStyle[] = [];
  if (layout.links.some(link => link.link && link.link.support_state !== "inferred")) styles.push("supported");
  if (layout.links.some(link => link.link?.support_state === "inferred")) styles.push("inferred");
  if (layout.links.some(link => !link.link)) styles.push("aggregate");
  return { labels: other ? [...known, "Other"] : known, styles };
}
