/** Pure path view data: the `/graph/paths` response, its text and the layered layout. */

export interface PathNode {
  id: string; label: string; name: string;
  evidence_doc_count?: number; link_count?: number; is_hub?: boolean; is_owner?: boolean;
}
export interface PathLink {
  source: string; type: string; target: string; family: string; weight: number;
  support_state: "supported" | "inferred" | "mixed"; stated_count: number; inferred_count: number;
}
export interface GraphPath { nodes: string[]; links: string[]; length: number; weakest_weight: number; has_inferred: boolean }
export interface PathsResponse {
  paths: GraphPath[];
  exhausted: boolean;
  /** Every node on a path; with no path, the two ends. */
  nodes: Record<string, PathNode>;
  links_detail: Record<string, PathLink>;
}

/** Most links per path: 2 to 4, 4 by default. */
export const MAX_LEN = [2, 3, 4] as const;
export function parseMaxLen(value: string | null): number {
  const length = Number(value);
  return MAX_LEN.includes(length as 2 | 3 | 4) ? length : 4;
}

/** Documents behind a link: its distinct support records. */
export const linkDocs = (link: PathLink) => link.stated_count + link.inferred_count;

/** The documents behind the weakest link of a path. */
export const weakestDocs = (data: PathsResponse, path: GraphPath) =>
  Math.min(...path.links.map(key => linkDocs(data.links_detail[key])));

/** The entities between the two ends, "Loan 88-2031 → Northgate", or "Direct link". */
export function routeText(data: PathsResponse, path: GraphPath, name: (node: PathNode) => string): string {
  const middle = path.nodes.slice(1, -1).map(id => name(data.nodes[id]));
  return middle.length ? middle.join(" → ") : "Direct link";
}

/** Whether link `key` runs along the path from its k-th node, or against it. */
export const forward = (data: PathsResponse, path: GraphPath, k: number) =>
  data.links_detail[path.links[k]].source === path.nodes[k];

export const askRelated = (a: string, b: string) => `How is ${a} related to ${b}?`;

export interface PathLayout {
  /** Node centers in screen pixels; the first path that reaches a node places it. */
  positions: Map<string, { x: number; y: number }>;
  /** Per path, how far a direct A to B link bows from the straight line. */
  bow: number[];
}

/** Room at each side for the end labels, and the most space between two lanes. */
export const PATH_PAD = { x: 120, y: 70 };
const LANE = 140;

/**
 * Layered left to right: A at the left, B at the right, and node k of a path of L links at k / L of the way.
 * Each path with entities between the ends has its own lane, top to bottom in rank order, centered on A and B.
 * When a direct link exists, no lane sits on the straight line between the ends.
 */
export function pathLayout(data: PathsResponse, a: string, b: string, width: number, height: number): PathLayout {
  const cy = height / 2;
  const x0 = PATH_PAD.x, x1 = Math.max(x0 + 1, width - PATH_PAD.x);
  const positions = new Map([[a, { x: x0, y: cy }], [b, { x: x1, y: cy }]]);
  const laned = data.paths.filter(path => path.length > 1);
  const direct = data.paths.length - laned.length;
  const slots = laned.length + (direct && laned.length % 2 ? 1 : 0);
  const gap = Math.max(0, Math.min(LANE, (height - 2 * PATH_PAD.y) / Math.max(1, slots - 1)));
  laned.forEach((path, lane) => {
    // An even slot count has no middle slot; with a direct link the extra slot stays empty.
    const y = cy + (lane - (slots - 1) / 2) * gap;
    path.nodes.forEach((id, k) => {
      if (!positions.has(id)) positions.set(id, { x: x0 + ((x1 - x0) * k) / path.length, y });
    });
  });
  let seen = 0;
  // Parallel direct links bow alternately above and below the line, 40 px further each.
  const bow = data.paths.map(path => (path.length > 1 ? 0 : (seen++ % 2 ? 1 : -1) * Math.ceil((seen - 1) / 2) * 40));
  return { positions, bow };
}
