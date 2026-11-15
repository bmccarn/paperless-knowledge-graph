/** Table rows come from every semantic link, independently of the Focus canvas cap. */
import { nodeLabelText } from "./categories";
import { FAMILIES, type NeighborhoodLayout, type NodeSummary, type SummaryLink } from "./graph-layout";
import { entityName, supportText, typeWords } from "./graph-inspector";

export const TABLE_PAGE_SIZE = 50;
export const TABLE_COLUMNS = [
  ["onCanvas", "On canvas"], ["family", "Family"], ["relationship", "Relationship"], ["direction", "Direction"],
  ["name", "Other entity"], ["type", "Type"], ["support", "Support"], ["documents", "Documents"],
  ["confidence", "Lowest confidence"], ["latest", "Latest document"],
] as const;
export type TableColumn = typeof TABLE_COLUMNS[number][0];
export interface LinkTableRow {
  link: SummaryLink;
  onCanvas: boolean;
  family: string;
  relationship: string;
  direction: string;
  name: string;
  type: string;
  support: string;
  documents: number;
  confidence: number | null;
  latest: string | null;
}

export function tableRows(summary: NodeSummary, layout: NeighborhoodLayout): LinkTableRow[] {
  const drawn = new Set(layout.links.filter(link => link.link).map(link => link.key));
  return summary.links.filter(link => link.family !== "evidence" && link.neighbor.label !== "Document").map(link => {
    const dates = link.records.map(record => summary.documents[String(record.doc_id)]?.date).filter((date): date is string => Boolean(date));
    return {
      link, onCanvas: drawn.has(link.key),
      family: FAMILIES.find(([key]) => key === link.family)?.[1] ?? "Other",
      relationship: typeWords(link.type), direction: link.direction === "in" ? "In" : "Out",
      name: entityName(link.neighbor), type: nodeLabelText(link.neighbor.label), support: supportText(link),
      documents: new Set(link.records.map(record => record.doc_id)).size,
      confidence: !link.records.length || link.records.some(record => record.confidence === null)
        ? null : Math.min(...link.records.map(record => record.confidence!)),
      latest: dates.sort().pop() ?? null,
    };
  });
}

/** Missing scores and dates stay last in either direction; the stable key breaks ties. */
export function sortTableRows(rows: LinkTableRow[], column: TableColumn, descending: boolean): LinkTableRow[] {
  return [...rows].sort((a, b) => {
    const left = a[column], right = b[column];
    if (left === null || right === null) return left === right ? a.link.key.localeCompare(b.link.key) : left === null ? 1 : -1;
    const order = typeof left === "string" && typeof right === "string" ? left.localeCompare(right) : Number(left) - Number(right);
    return (descending ? -order : order) || a.link.key.localeCompare(b.link.key);
  });
}
