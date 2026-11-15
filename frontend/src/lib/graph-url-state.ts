/** Graph identity in URLs uses entity ids and summary link keys, never database element ids. */
export type GraphView = "neighborhood" | "table" | "path";
export interface GraphUrlState {
  focus: string | null;
  view: GraphView;
  selection: string | null;
}

export function linkKey(source: string, type: string, target: string): string {
  return `${source}~${type}~${target}`;
}

export function parseLinkKey(key: string): { source: string; type: string; target: string } | null {
  const parts = key.split("~");
  return parts.length === 3 && parts.every(Boolean)
    ? { source: parts[0], type: parts[1], target: parts[2] }
    : null;
}

export function parseGraphState(params: URLSearchParams): GraphUrlState {
  const view = params.get("view");
  return {
    focus: params.get("focus") || null,
    view: view === "table" || view === "path" ? view : "neighborhood",
    selection: params.get("sel") || null,
  };
}

export function graphUrl(state: GraphUrlState, current = new URLSearchParams()): string {
  const params = new URLSearchParams(current);
  for (const [key, value] of [["focus", state.focus], ["view", state.view === "neighborhood" ? null : state.view], ["sel", state.selection]] as const) {
    if (value) params.set(key, value);
    else params.delete(key);
  }
  const query = params.toString();
  return `/graph${query ? `?${query}` : ""}`;
}

/** Focus/view are history steps; selection edits the current step. */
export function graphNavigation(current: URLSearchParams, change: Partial<GraphUrlState>) {
  const previous = parseGraphState(current);
  const next = { ...previous, ...change };
  if (next.focus !== previous.focus && change.selection === undefined) next.selection = null;
  return {
    href: graphUrl(next, current),
    method: next.focus !== previous.focus || next.view !== previous.view ? "push" as const : "replace" as const,
  };
}
