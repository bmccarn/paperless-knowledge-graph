/** Own graph identity and merging; renderer mutations never belong in this data. */
export interface ExplorerNode {
  id: string;
  name: string;
  label: string;
  props: Record<string, unknown>;
}

export interface ExplorerLink {
  id: string;
  source: string;
  target: string;
  type: string;
  props: Record<string, unknown>;
}

export interface ExplorerGraph {
  nodes: ExplorerNode[];
  links: ExplorerLink[];
  discarded: number;
}

export const emptyGraph = (): ExplorerGraph => ({
  nodes: [],
  links: [],
  discarded: 0,
});

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function documentId(value: unknown): number | null {
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && !/^\d+$/.test(value)) return null;
  const id = Number(value);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

export function nodeFromPayload(raw: unknown): ExplorerNode | null {
  const item = record(raw);
  const props = record(item.props ?? item.properties);
  const pid = documentId(props.paperless_id);
  const id =
    typeof props.uuid === "string" && props.uuid.trim()
      ? props.uuid
      : pid !== null
        ? `doc-${pid}`
        : null;
  if (!id) return null;
  const labels = Array.isArray(item.labels) ? item.labels : [];
  const label = labels.includes("Document")
    ? "Document"
    : String(labels[0] || "Unknown");
  return {
    id,
    name: String(props.name || props.title || id),
    label,
    props: structuredClone(props),
  };
}

export function sourceDocumentIds(props: Record<string, unknown>): number[] {
  const values = [
    props.source_doc,
    ...(Array.isArray(props.source_doc_ids) ? props.source_doc_ids : []),
  ];
  return [
    ...new Set(
      values.map(documentId).filter((id): id is number => id !== null),
    ),
  ].sort((a, b) => a - b);
}

export function relationshipDirection(
  value: unknown,
): "incoming" | "outgoing" | "unknown" {
  if (value === "in" || value === "incoming") return "incoming";
  if (value === "out" || value === "outgoing") return "outgoing";
  return "unknown";
}

export function relationshipSupport(props: Record<string, unknown>): Array<{
  documentId: number; inferred: boolean; quotes: string[]; rationale: string[];
}> {
  const parsed = (value: unknown): unknown => {
    if (typeof value !== "string") return value;
    try { return JSON.parse(value); } catch { return undefined; }
  };
  const records = Array.isArray(props.support_records) ? [...props.support_records] : [];
  const legacyId = documentId(props.source_doc);
  if (legacyId !== null && !records.some(raw => documentId(record(parsed(raw)).source_doc) === legacyId)) {
    records.push(props);
  }
  return records.flatMap((raw) => {
    const support = record(parsed(raw));
    const id = documentId(support.source_doc);
    if (id === null) return [];
    const quotes = [...new Set([support.evidence_spans, support.evidence_json].flatMap(value => {
      const spans = parsed(value);
      return Array.isArray(spans) ? spans.flatMap(span => {
        const quote = record(span).quote;
        return typeof quote === "string" ? [quote] : [];
      }) : [];
    }))];
    const rationale = (Array.isArray(support.rationale) ? support.rationale : [support.rationale])
      .filter((value): value is string => typeof value === "string");
    return [{ documentId: id, inferred: support.inferred === true || support.implied === true, quotes, rationale }];
  });
}

export function mergeGraph(
  current: ExplorerGraph,
  payload: unknown,
): ExplorerGraph {
  const raw = record(payload);
  const nodes = new Map(current.nodes.map((node) => [node.id, node]));
  let discarded = current.discarded;
  for (const item of Array.isArray(raw.nodes) ? raw.nodes : []) {
    const node = nodeFromPayload(item);
    if (node) nodes.set(node.id, node);
    else discarded++;
  }
  const links = new Map(current.links.map((link) => [link.id, link]));
  for (const item of Array.isArray(raw.relationships)
    ? raw.relationships
    : []) {
    const rel = record(item);
    const source = rel.start ?? rel.start_uuid;
    const target = rel.end ?? rel.end_uuid;
    if (
      typeof source !== "string" ||
      typeof target !== "string" ||
      !nodes.has(source) ||
      !nodes.has(target) ||
      typeof rel.type !== "string"
    ) {
      discarded++;
      continue;
    }
    const props = structuredClone(record(rel.props));
    const id =
      typeof rel.id === "string"
        ? rel.id
        : JSON.stringify([source, target, rel.type, sourceDocumentIds(props)]);
    links.set(id, { id, source, target, type: rel.type, props });
  }
  return { nodes: [...nodes.values()], links: [...links.values()], discarded };
}

export function escapeGraphLabel(value: unknown): string {
  return String(value).replace(
    /[&<>"']/g,
    (char) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        char
      ]!,
  );
}
