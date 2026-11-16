"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { parseGraphState, shownView } from "@/lib/graph-url-state";
import { GraphLanding } from "./graph-landing";
import { NeighborhoodView } from "./neighborhood-view";
import { PathPage } from "./path-view";
import { useViewKeys } from "./view-switcher";

function GraphRoute() {
  const params = useSearchParams();
  const focus = params.get("focus");
  const query = params.get("q") ?? "";
  useViewKeys();
  if (focus) return <NeighborhoodView key={focus} focusId={focus} />;
  // Paths has its own pickers, so it opens without a focus entity.
  return shownView(parseGraphState(params).view) === "path" ? <PathPage /> : <GraphLanding key={query} initialQuery={query} />;
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
