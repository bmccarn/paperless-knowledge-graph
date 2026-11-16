/** Timeline dates use the backend's parsed calendar date, never browser date guessing. */
import { FAMILIES, placeNeighbors, type NodeSummary, type SummaryLink } from "./graph-layout";
import { entityName, plural, typeWords } from "./graph-inspector";

export function documentMonth(date: string | null | undefined): string | null {
  return /^\d{4}-(0[1-9]|1[0-2])(?:-\d{2})?$/.test(date ?? "") ? date!.slice(0, 7) : null;
}
export const monthNumber = (month: string) => Number(month.slice(0, 4)) * 12 + Number(month.slice(5)) - 1;
export const monthValue = (index: number) => `${Math.floor(index / 12).toString().padStart(4, "0")}-${(index % 12 + 1).toString().padStart(2, "0")}`;
export const monthLabel = (month: string, long = false) => new Date(`${month}-01T00:00:00Z`).toLocaleDateString("en-US", { month: long ? "long" : "short", year: "numeric", timeZone: "UTC" });
export const inRange = (month: string | null, from: string, to: string) => (!from && !to) || Boolean(month && (!from || month >= from) && (!to || month <= to));

export function timelineAxis(summary: NodeSummary): string[] {
  const months = Object.values(summary.documents).map(doc => documentMonth(doc.date)).filter((month): month is string => month !== null).sort();
  if (!months.length) return [];
  const start = monthNumber(months[0]) - 1, end = monthNumber(months[months.length - 1]) + 1;
  return Array.from({ length: end - start + 1 }, (_, index) => monthValue(start + index));
}

/** Busy links draw one mark per month; the mark is inferred only when every record is inferred. */
export function timelineMarks(link: SummaryLink, documents: NodeSummary["documents"]) {
  const records = [...new Map(link.records.map(record => [record.doc_id, record])).values()];
  const merged = link.records.length > 60;
  const groups = new Map<string, { month: string | null; inferred: boolean; ids: number[] }>();
  for (const record of records) {
    const month = documentMonth(documents[String(record.doc_id)]?.date);
    const key = merged ? `${month}` : String(record.doc_id);
    const group = groups.get(key) ?? { month, inferred: record.inferred, ids: [] };
    group.inferred = group.inferred && record.inferred;
    group.ids.push(record.doc_id);
    groups.set(key, group);
  }
  return [...groups.values()].sort((a, b) => (a.month ?? "9999").localeCompare(b.month ?? "9999") || a.ids[0] - b.ids[0]);
}

export function timelineLanes(summary: NodeSummary, extra: Record<string, number> = {}) {
  const placed = placeNeighbors(summary);
  const cap = summary.focus.is_hub ? 2 : 6;
  return FAMILIES.flatMap(([family, name]) => {
    const members = placed.get(family) ?? [];
    return members.length ? [{ family, name, rows: members.slice(0, cap + (extra[family] ?? 0)), hidden: Math.max(0, members.length - cap - (extra[family] ?? 0)) }] : [];
  });
}

export function timelineRowName(neighbor: Parameters<typeof entityName>[0], links: SummaryLink[], documents: NodeSummary["documents"]) {
  return `${entityName(neighbor)}, ${links.map(link => {
    const months = link.records.map(record => documentMonth(documents[String(record.doc_id)]?.date)).filter((month): month is string => month !== null).sort();
    const dates = months.length ? ` from ${monthLabel(months[0], true)} to ${monthLabel(months[months.length - 1], true)}` : ", no dated documents";
    const support = link.support_state === "mixed" ? `stated in ${link.stated_count}, inferred in ${link.inferred_count}` : link.support_state === "inferred" ? "all inferred" : "all stated";
    return `${link.direction === "in" ? "incoming" : "outgoing"} ${typeWords(link.type).toLowerCase()} link, ${plural(new Set(link.records.map(record => record.doc_id)).size, "document")}${dates}, ${support}`;
  }).join("; ")}.`;
}

/** Keep every source mark inside its month or No date column; dense months stack downward. */
export function timelineTracks(links: SummaryLink[], documents: NodeSummary["documents"], monthSpacing: number) {
  let y = 22;
  const tracks = links.map(link => {
    const marks = timelineMarks(link, documents);
    const seen = new Map<string | null, number>();
    let depth = 1;
    const positioned = marks.map(mark => {
      const count = marks.filter(other => other.month === mark.month).length;
      const columns = Math.max(1, Math.min(count, mark.month === null ? 4 : Math.max(2, Math.floor(monthSpacing / 12))));
      const index = seen.get(mark.month) ?? 0;
      seen.set(mark.month, index + 1);
      depth = Math.max(depth, Math.floor(index / columns) + 1);
      return { ...mark, dx: (index % columns - (columns - 1) / 2) * 12, dy: Math.floor(index / columns) * 12 };
    });
    const track = { link, marks: positioned, y };
    y += depth * 12;
    return track;
  });
  return { tracks, height: Math.max(44, y + 10) };
}

/** Label months on short spans and years on long spans, with room between the labels. */
export function timelineLabels(months: string[], width: number) {
  const long = months.length > 18;
  const x = (index: number) => 16 + index / Math.max(1, months.length - 1) * (width - 100);
  const candidates = months.flatMap((month, index) => !long || index === 0 || month.endsWith('-01')
    ? [{ index, text: long ? month.slice(0, 4) : monthLabel(month) }] : []);
  if (long && candidates.length > 1 && x(candidates[1].index) - x(0) < 48) candidates.shift();
  let previous = -Infinity;
  return candidates.filter(label => {
    if (x(label.index) - previous < (long ? 48 : 80)) return false;
    previous = x(label.index);
    return true;
  });
}
