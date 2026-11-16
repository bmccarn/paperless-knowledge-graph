"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { drawnView, parseGraphState } from "@/lib/graph-url-state";
import { GraphOpener } from "./graph-landing";
import { NeighborhoodView } from "./neighborhood-view";
import { OverviewPage } from "./overview-view";
import { PathPage } from "./path-view";
import { useViewKeys } from "./view-switcher";

function GraphRoute() {
  const params = useSearchParams();
  const focus = params.get("focus");
  const query = params.get("q") ?? "";
  useViewKeys();
  const view = drawnView(parseGraphState(params).view);
  // Overview shows the whole archive, whatever the focus.
  if (view === "overview") return <OverviewPage />;
  if (focus) return <NeighborhoodView key={focus} focusId={focus} />;
  // Paths has its own pickers, so it opens without a focus entity.
  return view === "path" ? <PathPage /> : <GraphOpener query={query} />;
}

export function GraphContainer() {
  return (
    <Suspense
      fallback={
        <div className="flex h-full flex-col items-center justify-center gap-4">
          <h1 className="page-title">Graph</h1>
          Loading graph…
        </div>
      }
    >
      <GraphRoute />
    </Suspense>
  );
}
