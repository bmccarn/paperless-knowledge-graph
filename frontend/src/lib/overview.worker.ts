/** Overview's groups and layout off the main thread: one response and filter in, one map out. */
import { arrangeOverview, type OverviewResponse } from "./graph-overview";
import type { GraphFilters } from "./graph-filters";

export type ArrangeRequest = { response: OverviewResponse; filters: Pick<GraphFilters, "families" | "support" | "minDocs"> };

const scope = self as unknown as { onmessage: ((event: MessageEvent<ArrangeRequest>) => void) | null; postMessage: (message: unknown, transfer?: Transferable[]) => void };
scope.onmessage = ({ data }) => {
  try {
    const map = arrangeOverview(data.response, data.filters);
    scope.postMessage({ map }, [map.positions.buffer, map.groupOf.buffer]);
  } catch (error) {
    scope.postMessage({ error: error instanceof Error ? error.message : String(error) });
  }
};
