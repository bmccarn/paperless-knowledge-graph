"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  getEntityReviewCandidates,
  ignoreEntityCandidate,
  mergeEntityCandidate,
  splitEntityCandidate,
} from "@/lib/api";
import { GitMerge, Loader2, RefreshCw, Split, X } from "lucide-react";

interface Candidate {
  score: number;
  label: string;
  left: { uuid: string; name: string; properties: Record<string, unknown> };
  right: { uuid: string; name: string; properties: Record<string, unknown> };
}

export default function EntityReviewPage() {
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyKey, setBusyKey] = useState("");

  const load = async () => {
    setLoading(true);
    try {
      const data = await getEntityReviewCandidates(75);
      setCandidates(data.candidates || []);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const act = async (key: string, fn: () => Promise<unknown>) => {
    setBusyKey(key);
    try {
      await fn();
      await load();
    } finally {
      setBusyKey("");
    }
  };

  return (
    <div className="h-full overflow-y-auto p-4 md:p-6 lg:p-8 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Entity Review</h1>
          <p className="text-sm text-muted-foreground mt-1">Likely duplicate entities and bad merge candidates.</p>
        </div>
        <Button onClick={load} disabled={loading} variant="outline" size="sm" className="gap-2">
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          Refresh
        </Button>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading candidates...</div>
      ) : candidates.length === 0 ? (
        <Card><CardContent className="py-10 text-center text-muted-foreground">No review candidates found.</CardContent></Card>
      ) : (
        <div className="grid gap-3">
          {candidates.map((candidate) => {
            const key = `${candidate.left.uuid}:${candidate.right.uuid}`;
            const busy = busyKey === key;
            return (
              <Card key={key} className="border-border/60">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <Badge variant="secondary">{candidate.label}</Badge>
                    <span>{Math.round(candidate.score)}% similar</span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="grid gap-3 md:grid-cols-2">
                    {[candidate.left, candidate.right].map((entity) => (
                      <div key={entity.uuid} className="rounded-lg border bg-card p-3">
                        <p className="font-medium text-sm">{entity.name}</p>
                        <p className="text-[10px] text-muted-foreground mt-1 break-all">{entity.uuid}</p>
                        {typeof entity.properties.description === "string" && entity.properties.description && (
                          <p className="text-xs text-muted-foreground mt-2 line-clamp-3">{String(entity.properties.description)}</p>
                        )}
                      </div>
                    ))}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      size="sm"
                      disabled={busy}
                      onClick={() => act(key, () => mergeEntityCandidate(candidate.left.uuid, candidate.right.uuid))}
                      className="gap-2"
                    >
                      {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <GitMerge className="h-3.5 w-3.5" />}
                      Merge right into left
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={busy}
                      onClick={() => act(key, () => splitEntityCandidate(candidate.left.uuid, candidate.right.uuid, "Not the same entity"))}
                      className="gap-2"
                    >
                      <Split className="h-3.5 w-3.5" /> Keep split
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busy}
                      onClick={() => act(key, () => ignoreEntityCandidate(candidate.left.uuid, candidate.right.uuid, "Reviewed"))}
                      className="gap-2"
                    >
                      <X className="h-3.5 w-3.5" /> Ignore
                    </Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}
