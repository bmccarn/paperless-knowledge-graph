"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  getEntityReviewCandidates,
  getTask,
  ignoreEntityCandidate,
  mergeEntityCandidate,
  runEntitySteward,
  splitEntityCandidate,
} from "@/lib/api";
import { AlertCircle, CheckCircle2, GitMerge, Loader2, RefreshCw, Split, X } from "lucide-react";

interface Candidate {
  score: number;
  label: string;
  left: { uuid: string; name: string; properties: Record<string, unknown> };
  right: { uuid: string; name: string; properties: Record<string, unknown> };
  steward?: {
    decision?: string;
    deterministic?: {
      score?: number;
      risk?: string;
      recommendation?: string;
      reasons?: string[];
      shared_identifiers?: string[];
    };
    agent?: {
      recommendation?: string;
      confidence?: number;
      risk?: string;
      reasons?: string[];
    };
  };
}

interface Notice {
  kind: "success" | "error";
  text: string;
}

interface TaskStatus {
  status?: string;
  result?: {
    reviewed_count?: number;
    suggest_merge?: number;
    suggest_split?: number;
    suggest_review?: number;
  };
  error?: string;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export default function EntityReviewPage() {
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyKey, setBusyKey] = useState("");
  const [busyAction, setBusyAction] = useState("");
  const [stewardRunning, setStewardRunning] = useState(false);
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

  const runSteward = async () => {
    setStewardRunning(true);
    setNotice(null);
    setError("");
    try {
      const start = await runEntitySteward(75);
      setNotice({ kind: "success", text: "Entity steward started. Reviewing candidates in the background..." });
      let task: TaskStatus | null = null;
      for (let attempt = 0; attempt < 160; attempt += 1) {
        const latest = await getTask(start.task_id) as TaskStatus;
        task = latest;
        if (latest.status === "completed") break;
        if (latest.status === "failed") throw new Error(latest.error || "Entity steward run failed.");
        await sleep(1500);
      }
      if (!task || task.status !== "completed") {
        throw new Error("Entity steward is still running. Refresh in a moment to see new suggestions.");
      }
      const result = task.result || {};
      setNotice({
        kind: "success",
        text: `Entity steward reviewed ${result.reviewed_count || 0} candidates: ${result.suggest_merge || 0} merge, ${result.suggest_split || 0} split, ${result.suggest_review || 0} review.`,
      });
      await load(false);
    } catch (err) {
      setNotice({
        kind: "error",
        text: err instanceof Error ? err.message : "Entity steward run failed.",
      });
    } finally {
      setStewardRunning(false);
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
        <div className="flex gap-2">
          <Button onClick={runSteward} disabled={loading || stewardRunning} variant="secondary" size="sm" className="gap-2">
            {stewardRunning ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <GitMerge className="h-3.5 w-3.5" />}
            Steward
          </Button>
          <Button onClick={() => load()} disabled={loading} variant="outline" size="sm" className="gap-2">
            {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
            Refresh
          </Button>
        </div>
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
                    {candidate.steward?.decision && (
                      <Badge variant="outline">{candidate.steward.decision.replace("suggest_", "")}</Badge>
                    )}
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {candidate.steward && (
                    <div className="rounded-md border bg-muted/40 px-3 py-2 text-xs space-y-1">
                      <div className="flex flex-wrap gap-2">
                        {candidate.steward.deterministic?.risk && (
                          <Badge variant="outline" className="text-[9px]">risk: {candidate.steward.deterministic.risk}</Badge>
                        )}
                        {candidate.steward.deterministic?.score != null && (
                          <Badge variant="outline" className="text-[9px]">score: {Math.round(candidate.steward.deterministic.score * 100)}%</Badge>
                        )}
                        {candidate.steward.agent?.recommendation && (
                          <Badge variant="secondary" className="text-[9px]">
                            agent: {candidate.steward.agent.recommendation}
                            {candidate.steward.agent.confidence != null ? ` ${Math.round(candidate.steward.agent.confidence * 100)}%` : ""}
                          </Badge>
                        )}
                      </div>
                      {(candidate.steward.agent?.reasons || candidate.steward.deterministic?.reasons || []).slice(0, 3).map((reason, idx) => (
                        <p key={idx} className="text-muted-foreground">{reason}</p>
                      ))}
                    </div>
                  )}
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
