/** Pure helpers for the /graph landing: search rows, typeahead groups, Recent and the `?q=` rule. */
import { nodeKind, nodeLabelText, type Kind } from "./categories";

/** `ownerMatch`: another Person that matches the owner setting, not the one the owner rule chose. */
export interface EntityRow { id: string; name: string; label: string; documents: number; alias: string | null; owner: boolean;
  ownerMatch: boolean }

/** A `/graph/search` result as a landing row; documents and rows without an entity id are dropped. */
export function entityRow(raw: unknown): EntityRow | null {
  const row = (raw ?? {}) as { labels?: string[]; properties?: Record<string, unknown>; evidence_doc_count?: number;
    matched_alias?: string | null; is_owner?: boolean; owner_match?: boolean };
  const label = row.labels?.[0] ?? "";
  const id = row.properties?.uuid;
  if (typeof id !== "string" || !id || nodeKind(label).kind === "docs") return null;
  return { id, label, name: String(row.properties?.name ?? ""), documents: Number(row.evidence_doc_count) || 0,
    alias: row.matched_alias || null, owner: row.is_owner === true, ownerMatch: row.owner_match === true };
}

/** The name, or the kind and a short id for an unnamed entity; never the bare id. */
export const entityName = ({ id, name, label }: { id: string; name: string; label: string }) =>
  name && name !== id ? name : `${nodeLabelText(label)} ${id.slice(0, 6)}`;

const GROUPS: [string, Kind[]][] = [
  ["People", ["people"]], ["Organizations", ["org"]], ["Money and agreements", ["money", "agree"]], ["Places", ["place"]],
  ["Health", ["health"]], ["Things", ["thing"]], ["Time", ["time"]], ["Other", ["unknown"]],
];

/** The first 8 rows in search order, grouped by kind in the fixed group order. */
export function groupResults(rows: EntityRow[]): { title: string; rows: EntityRow[] }[] {
  const top = rows.slice(0, 8);
  return GROUPS.map(([title, kinds]) => ({ title, rows: top.filter(row => kinds.includes(nodeKind(row.label).kind)) }))
    .filter(group => group.rows.length);
}

/** The entity to open for `/graph?q=`: the only row whose name equals the text, ignoring case. */
export function exactMatch(rows: EntityRow[], text: string): EntityRow | null {
  const wanted = text.trim().toLocaleLowerCase();
  const matches = rows.filter(row => row.name.trim().toLocaleLowerCase() === wanted);
  return matches.length === 1 ? matches[0] : null;
}

/** The top 10 by evidence documents, without the owner and the other Persons that match the owner setting. */
export const mostConnected = (rows: EntityRow[]) => rows.filter(row => !row.owner && !row.ownerMatch).slice(0, 10);

export type RecentEntity = Pick<EntityRow, "id" | "name" | "label" | "documents">;

/** Put an entity first in Recent, once, keeping the last 10. */
export const addRecent = (list: RecentEntity[], entity: RecentEntity) =>
  [entity, ...list.filter(item => item.id !== entity.id)].slice(0, 10);

/** Recent from its stored text; anything malformed reads as empty. */
export function parseRecent(text: string | null): RecentEntity[] {
  try {
    const list = JSON.parse(text ?? "[]");
    return Array.isArray(list) ? list.filter(item => typeof item?.id === "string" && typeof item?.label === "string").slice(0, 10) : [];
  } catch {
    return [];
  }
}

/** Text split into parts that match a query word (ignoring case) and parts that do not. */
export function highlight(text: string, query: string): { text: string; match: boolean }[] {
  const words = query.toLocaleLowerCase().split(/\s+/).filter(Boolean);
  const lower = text.toLocaleLowerCase();
  const parts: { text: string; match: boolean }[] = [];
  let at = 0;
  while (at < text.length) {
    const hits = words.map(word => [lower.indexOf(word, at), word.length]).filter(([index]) => index >= 0);
    if (!hits.length) break;
    const [index, length] = hits.reduce((best, hit) => hit[0] < best[0] || (hit[0] === best[0] && hit[1] > best[1]) ? hit : best);
    if (index > at) parts.push({ text: text.slice(at, index), match: false });
    parts.push({ text: text.slice(index, index + length), match: true });
    at = index + length;
  }
  if (at < text.length) parts.push({ text: text.slice(at), match: false });
  return parts;
}

export const RECENT_KEY = "graph-recent";
