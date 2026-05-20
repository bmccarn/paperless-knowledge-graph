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
import { AlertCircle, CheckCircle2, GitMerge, Loader2, RefreshCw, Split, X } from "lucide-react";

interface Candidate {
  score: number;
  label: string;
  left: { uuid: string; name: string; properties: Record<string, unknown> };
  right: { uuid: string; name: string; properties: Record<string, unknown> };
}

interface Notice {
  kind: "success" | "error";
  text: string;
}

export default function EntityReviewPage() {
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyKey, setBusyKey] = useState("");
  const [busyAction, setBusyAction] = useState("");
  const [error, setError] = useState("");
  const [notice, setNotice] = useState<Notice | null>(null);

  const load = async (showSpinner = true) => {
    if (showSpinner) setLoading(true);
    setError("");
    try {
      const data = await getEntityReviewCandidates(75);
      setCandidates(data.candidates || []);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Could not load review candidates.";
      if (showSpinner) {
        setError(message);
      } else {
        setNotice({ kind: "error", text: `Action succeeded, but refresh failed: ${message}` });
      }
    } finally {
      if (showSpinner) setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const act = async (key: string, action: string, successText: string, fn: () => Promise<unknown>) => {
    setBusyKey(key);
    setBusyAction(action);
    setNotice(null);
    setError("");
    try {
      await fn();
      setCandidates((prev) => prev.filter((candidate) => `${candidate.left.uuid}:${candidate.right.uuid}` !== key));
      setNotice({ kind: "success", text: successText });
      void load(false);
    } catch (err) {
      setNotice({
        kind: "error",
        text: err instanceof Error ? err.message : "Review action failed.",
      });
    } finally {
      setBusyKey("");
      setBusyAction("");
    }
  };

  return (
    <div className="h-full overflow-y-auto p-4 md:p-6 lg:p-8 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Entity Review</h1>
          <p className="text-sm text-muted-foreground mt-1">Likely duplicate entities and bad merge candidates.</p>
        </div>
        <Button onClick={() => load()} disabled={loading} variant="outline" size="sm" className="gap-2">
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          Refresh
        </Button>
      </div>

      {notice && (
        <div
          className={`flex items-start gap-2 rounded-md border px-3 py-2 text-sm ${
            notice.kind === "success"
              ? "border-emerald-200 bg-emerald-50 text-emerald-900 dark:border-emerald-900/60 dark:bg-emerald-950/40 dark:text-emerald-200"
              : "border-destructive/30 bg-destructive/10 text-destructive"
          }`}
          role={notice.kind === "error" ? "alert" : "status"}
        >
          {notice.kind === "success" ? (
            <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
          ) : (
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          )}
          <span>{notice.text}</span>
        </div>
      )}

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading candidates...</div>
      ) : error ? (
        <Card><CardContent className="py-10 text-center text-destructive">{error}</CardContent></Card>
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
                      onClick={() =>
                        act(
                          key,
                          "merge",
                          `Merged "${candidate.right.name}" into "${candidate.left.name}".`,
                          () => mergeEntityCandidate(candidate.left.uuid, candidate.right.uuid),
                        )
                      }
                      className="gap-2"
                    >
                      {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <GitMerge className="h-3.5 w-3.5" />}
                      {busy && busyAction === "merge" ? "Merging..." : "Merge right into left"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={busy}
                      onClick={() =>
                        act(
                          key,
                          "split",
                          `Kept "${candidate.left.name}" and "${candidate.right.name}" split.`,
                          () => splitEntityCandidate(candidate.left.uuid, candidate.right.uuid, "Not the same entity"),
                        )
                      }
                      className="gap-2"
                    >
                      {busy && busyAction === "split" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Split className="h-3.5 w-3.5" />}
                      {busy && busyAction === "split" ? "Saving..." : "Keep split"}
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busy}
                      onClick={() =>
                        act(
                          key,
                          "ignore",
                          `Ignored "${candidate.left.name}" and "${candidate.right.name}".`,
                          () => ignoreEntityCandidate(candidate.left.uuid, candidate.right.uuid, "Reviewed"),
                        )
                      }
                      className="gap-2"
                    >
                      {busy && busyAction === "ignore" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <X className="h-3.5 w-3.5" />}
                      {busy && busyAction === "ignore" ? "Saving..." : "Ignore"}
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
