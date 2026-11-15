/** Graph identity in URLs uses entity ids and summary link keys, never database element ids. */
export type GraphView = "focus" | "explore" | "timeline" | "table" | "path" | "overview";

export interface GraphViewInfo {
  id: GraphView;
  label: string;
  question: string;
  /** Letter key kept from the first graph release. */
  alias?: string;
  needsFocus: boolean;
  /** In the switcher and on its keys. A view's ticket turns this on; Overview waits for its acceptance (#204). */
  listed: boolean;
}

/** Switcher order; keys 1 to 6 follow it. */
export const GRAPH_VIEWS: readonly GraphViewInfo[] = [
  { id: "focus", label: "Focus", question: "What is this entity linked to, and how do those things link to each other?", alias: "n", needsFocus: true, listed: true },
  { id: "explore", label: "Explore", question: "What lies two or three steps away, and where do the links loop back?", needsFocus: true, listed: false },
  { id: "timeline", label: "Timeline", question: "When was each link documented, and what arrived when?", needsFocus: true, listed: false },
  { id: "table", label: "Table", question: "Every link of this entity, sorted and filtered.", alias: "t", needsFocus: true, listed: false },
  { id: "path", label: "Paths", question: "How are these two entities connected?", alias: "p", needsFocus: false, listed: false },
  { id: "overview", label: "Overview", question: "What is in the archive, and which parts hang together?", needsFocus: false, listed: false },
];

export const listedViews = (views: readonly GraphViewInfo[] = GRAPH_VIEWS) => views.filter(view => view.listed);

/** The view the page draws: an unlisted view in the URL draws as Focus. */
export const shownView = (view: GraphView, views: readonly GraphViewInfo[] = GRAPH_VIEWS): GraphView =>
  views.find(entry => entry.id === view)?.listed ? view : "focus";

/** The view a key press opens, or null. Number keys keep their place even while earlier views are unlisted. */
export function viewForKey(key: string, views: readonly GraphViewInfo[] = GRAPH_VIEWS): GraphView | null {
  const view = /^[1-6]$/.test(key) ? views[Number(key) - 1] : views.find(view => view.alias === key);
  return view?.listed ? view.id : null;
}

export interface ViewKeyEvent { key: string; ctrlKey: boolean; metaKey: boolean; altKey: boolean; defaultPrevented: boolean }

/** The view a key press on the graph page switches to, or null when the key does nothing. */
export function keyedView(event: ViewKeyEvent, inTextField: boolean, state: GraphUrlState, views: readonly GraphViewInfo[] = GRAPH_VIEWS): GraphView | null {
  if (inTextField || event.defaultPrevented || event.ctrlKey || event.metaKey || event.altKey) return null;
  const view = views.find(entry => entry.id === viewForKey(event.key, views));
  if (!view || (view.needsFocus && !state.focus) || view.id === shownView(state.view, views)) return null;
  return view.id;
}

/** The toolbar button that Left, Right, Home or End moves to, or null for any other key. */
export function rovingIndex(index: number, key: string, count: number): number | null {
  if (key === "ArrowRight") return (index + 1) % count;
  if (key === "ArrowLeft") return (index - 1 + count) % count;
  if (key === "Home") return 0;
  if (key === "End") return count - 1;
  return null;
}
export interface GraphUrlState {
  focus: string | null;
  view: GraphView;
  selection: string | null;
  /** Focus draws the links between neighbors unless the URL says `cross=0`. */
  cross: boolean;
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
  // `neighborhood` is Focus's name in older links.
  const view = GRAPH_VIEWS.find(view => view.id === params.get("view"))?.id ?? "focus";
  return {
    focus: params.get("focus") || null,
    view,
    selection: params.get("sel") || null,
    cross: params.get("cross") !== "0",
  };
}

export function graphUrl(state: GraphUrlState, current = new URLSearchParams()): string {
  const params = new URLSearchParams(current);
  for (const [key, value] of [["focus", state.focus], ["view", state.view === "focus" ? null : state.view], ["sel", state.selection], ["cross", state.cross ? null : "0"]] as const) {
    if (value) params.set(key, value);
    else params.delete(key);
  }
  const query = params.toString();
  return `/graph${query ? `?${query}` : ""}`;
}

/** A pill exists only in Focus and a group only in Overview; any other selection is kept for the new view to resolve. */
const viewHolds = (view: GraphView, selection: string) =>
  selection.startsWith("pill:") ? view === "focus" : selection.startsWith("grp:") ? view === "overview" : true;

/** Focus/view are history steps; selection edits the current step. */
export function graphNavigation(current: URLSearchParams, change: Partial<GraphUrlState>) {
  const previous = parseGraphState(current);
  const next = { ...previous, ...change };
  if (next.focus !== previous.focus && change.selection === undefined) next.selection = null;
  if (next.view !== previous.view && next.selection && change.selection === undefined && !viewHolds(next.view, next.selection)) next.selection = null;
  return {
    href: graphUrl(next, current),
    method: next.focus !== previous.focus || next.view !== previous.view ? "push" as const : "replace" as const,
  };
}
