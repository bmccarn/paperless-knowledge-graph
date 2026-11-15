"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { GraphLanding } from "./graph-landing";
import { NeighborhoodView } from "./neighborhood-view";

function GraphRoute() {
  const params = useSearchParams();
  const focus = params.get("focus");
  const query = params.get("q") ?? "";
  return focus ? <NeighborhoodView key={focus} focusId={focus} /> : <GraphLanding key={query} initialQuery={query} />;
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
