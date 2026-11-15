"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Toaster, type ToastMessage } from "@/components/ui/toast";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { KindShape } from "@/components/kind-shape";
import {
  getEntityMerges,
  getEntityReviewCandidates,
  ignoreEntityCandidate,
  mergeEntityCandidate,
  runEntitySteward,
  splitEntityCandidate,
  unmergeEntity,
  waitForTask,
  type EntityMerge,
} from "@/lib/api";
import { errMsg } from "@/lib/utils";
import { AlertCircle, GitMerge, Info, RefreshCw, Split, TriangleAlert, Undo2, X } from "lucide-react";

interface Entity { uuid: string; name: string; properties: Record<string, unknown> }

interface Candidate {
  score: number;
  label: string;
  left: Entity;
  right: Entity;
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
  kind: "info" | "error";
  text: string;
}

interface StewardResult {
  reviewed_count?: number;
  suggest_merge?: number;
  suggest_split?: number;
  suggest_review?: number;
}

type MergeRef = Pick<EntityMerge, "merge_id" | "primary_name" | "duplicate_name">;

const SUGGESTIONS: Record<string, string> = {
  suggest_merge: "Steward suggests merging",
  suggest_split: "Steward suggests keeping them split",
  suggest_review: "Steward suggests a closer look",
};
const RISK_COLORS: Record<string, string> = { low: "var(--verified)", medium: "var(--partial)", high: "var(--error)" };
const FLAG_WORDS: Record<string, string> = {
  shared_source_document: "Shared source document",
  direct_edge: "Directly linked",
  conflicting_identifiers: "Conflicting identifiers",
  different_identity_hints: "Different identity hints",
  status_carried_over: "Review status carried over",
};

const sentence = (text: string) => text.charAt(0).toUpperCase() + text.slice(1).replaceAll("_", " ");
const flagWord = (flag: string) =>
  flag.startsWith("scalar_clash:") ? `Different ${flag.slice("scalar_clash:".length).replaceAll("_", " ")}` : FLAG_WORDS[flag] ?? sentence(flag);
const pairKey = (candidate: Candidate) => `${candidate.left.uuid}:${candidate.right.uuid}`;
const graphHref = (uuid: string) => `/graph?focus=${encodeURIComponent(uuid)}`;
const pad = (value: number) => String(value).padStart(2, "0");
const when = (iso: string) => {
  const date = new Date(iso);
  return `${pad(date.getDate())} ${date.toLocaleString("en-US", { month: "short" })} ${date.getFullYear()}, ${pad(date.getHours())}:${pad(date.getMinutes())}`;
};

function Dot({ color, children }: { color: string; children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span aria-hidden className="size-1.5 shrink-0 rounded-full" style={{ background: color }} />
      {children}
    </span>
  );
}

function EntityName({ uuid, name }: { uuid: string; name: string }) {
  return (
    <Link href={graphHref(uuid)} className="underline decoration-border-strong underline-offset-4 hover:decoration-fg">
      {name}
    </Link>
  );
}

export default function EntityReviewPage() {
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyKey, setBusyKey] = useState("");
  const [busyAction, setBusyAction] = useState("");
  const [stewardRunning, setStewardRunning] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState<Notice | null>(null);
  const [toast, setToast] = useState<ToastMessage | null>(null);
  const toastId = useRef(0);
  const [mergeTarget, setMergeTarget] = useState<Candidate | null>(null);
  const [merges, setMerges] = useState<EntityMerge[]>([]);
  const [mergesError, setMergesError] = useState("");

  const showToast = (text: string, action?: ToastMessage["action"]) => setToast({ id: ++toastId.current, text, action });

  // Separate from load(): a failed merge list must not hide the candidates.
  const loadMerges = async () => {
    try {
      setMerges((await getEntityMerges()).merges || []);
      setMergesError("");
    } catch (err) {
      setMergesError(errMsg(err, "Could not load recent merges."));
    }
  };

  const load = async (showSpinner = true) => {
    if (showSpinner) setLoading(true);
    setError("");
    try {
      const data = await getEntityReviewCandidates(75);
      setCandidates(data.candidates || []);
    } catch (err) {
      const message = errMsg(err, "Could not load review candidates.");
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
      showToast("Entity steward started. It reviews the candidates in the background.");
      const task = await waitForTask(start.task_id);
      const result: StewardResult = task.result || {};
      showToast(
        `Entity steward reviewed ${result.reviewed_count || 0} candidates: ${result.suggest_merge || 0} merge, ${result.suggest_split || 0} split, ${result.suggest_review || 0} review.`,
      );
      await load(false);
    } catch (err) {
      setNotice({ kind: "error", text: errMsg(err, "Entity steward run failed.") });
    } finally {
      setStewardRunning(false);
    }
  };

  useEffect(() => { load(); loadMerges(); }, []);

  const act = async <T,>(key: string, action: string, fn: () => Promise<T>, done: (result: T) => void) => {
    setBusyKey(key);
    setBusyAction(action);
    setNotice(null);
    setError("");
    try {
      const result = await fn();
      setCandidates((prev) => prev.filter((candidate) => pairKey(candidate) !== key));
      done(result);
      void load(false);
      void loadMerges();
    } catch (err) {
      setNotice({ kind: "error", text: errMsg(err, "Review action failed.") });
    } finally {
      setBusyKey("");
      setBusyAction("");
    }
  };

  const undo = async (merge: MergeRef) => {
    const duplicate = merge.duplicate_name ?? "the merged entity";
    const primary = merge.primary_name ?? "the kept entity";
    setBusyKey(merge.merge_id);
    setBusyAction("undo");
    setNotice(null);
    try {
      const result = await unmergeEntity(merge.merge_id, "Undone from entity review");
      if (result.status === "conflict") {
        setNotice({
          kind: "info",
          text: `${primary} changed after the merge, so its merged fields were reset and the documents of ${duplicate} are being reindexed.`,
        });
      } else {
        showToast(`Undid the merge of ${duplicate} into ${primary}.`);
      }
      void load(false);
      void loadMerges();
    } catch (err) {
      setNotice({ kind: "error", text: errMsg(err, "Undo failed.") });
    } finally {
      setBusyKey("");
      setBusyAction("");
    }
  };

  const merge = (candidate: Candidate) => {
    const { left, right } = candidate;
    void act(pairKey(candidate), "merge", () => mergeEntityCandidate(left.uuid, right.uuid), (result) => {
      const mergeId: string | undefined = result?.entity?.merge_id;
      const text = `Merged ${right.name} into ${left.name}.`;
      showToast(text, mergeId
        ? { label: "Undo", onClick: () => void undo({ merge_id: mergeId, primary_name: left.name, duplicate_name: right.name }) }
        : undefined);
    });
  };

  const locked = busyKey !== "";

  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-[1280px] space-y-6 p-4 md:px-12 md:py-10">
        <header className="flex flex-wrap items-end justify-between gap-4">
          <div className="space-y-1">
            <h1 className="page-title">Entities</h1>
            <p className="copy-14 text-fg-muted">
              Review possible duplicates before they merge. You can undo a merge from{" "}
              <a href="#recent-merges" className="text-fg underline underline-offset-4">Recent merges</a>.
            </p>
          </div>
          <div className="flex gap-2">
            <Button onClick={runSteward} disabled={loading || stewardRunning} loading={stewardRunning} variant="secondary" size="sm">
              <GitMerge />
              Run steward
            </Button>
            <Button onClick={() => load()} disabled={loading} variant="outline" size="sm">
              <RefreshCw />
              Refresh
            </Button>
          </div>
        </header>

        {notice && (
          <Alert variant={notice.kind === "error" ? "destructive" : "default"} role={notice.kind === "error" ? "alert" : "status"}>
            {notice.kind === "error" ? <AlertCircle aria-hidden /> : <Info aria-hidden />}
            <AlertDescription>{notice.text}</AlertDescription>
          </Alert>
        )}

        <div className="grid items-start gap-8 min-[1180px]:grid-cols-[minmax(0,1fr)_340px]">
          <section aria-labelledby="queue-title" className="min-w-0">
            <div className="flex items-baseline gap-2 border-b border-border pb-2">
              <h2 id="queue-title" className="heading-16">Duplicate candidates</h2>
              {!loading && !error && <span className="label-13 text-fg-muted tabular-nums">{candidates.length}</span>}
            </div>
            {loading ? (
              <div role="status" aria-busy="true" aria-label="Loading candidates" className="divide-y divide-border">
                {[0, 1, 2].map((row) => (
                  <div key={row} className="space-y-3 py-5">
                    <Skeleton className="h-4 w-56" />
                    <div className="grid gap-3 md:grid-cols-2"><Skeleton className="h-20" /><Skeleton className="h-20" /></div>
                  </div>
                ))}
              </div>
            ) : error ? (
              <div role="alert" className="flex flex-wrap items-center gap-3 py-8 copy-14 text-error">
                <span>{error}</span>
                <Button variant="outline" size="sm" onClick={() => load()}>Retry</Button>
              </div>
            ) : candidates.length === 0 ? (
              <div className="flex flex-wrap items-center gap-3 py-8 copy-14 text-fg-muted">
                <span>No possible duplicates to review.</span>
                <Button variant="outline" size="sm" onClick={() => load()}>Refresh</Button>
              </div>
            ) : (
              <div className="divide-y divide-border">
                {candidates.map((candidate) => {
                  const key = pairKey(candidate);
                  const busy = busyKey === key;
                  const { deterministic, agent, decision } = candidate.steward || {};
                  const reasons = (agent?.reasons || deterministic?.reasons || []).slice(0, 3);
                  return (
                    <article key={key} aria-label={`${candidate.left.name} and ${candidate.right.name}`} className="space-y-3 py-5">
                      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 label-13 text-fg-muted">
                        <KindShape label={candidate.label} />
                        <span className="tabular-nums">{Math.round(candidate.score)}% similar</span>
                        {decision && <span>{SUGGESTIONS[decision] ?? `Steward: ${sentence(decision)}`}</span>}
                        {deterministic?.risk && (
                          <Dot color={RISK_COLORS[deterministic.risk] ?? "var(--fg-muted)"}>{sentence(deterministic.risk)} risk</Dot>
                        )}
                      </div>
                      <div className="grid gap-3 md:grid-cols-2">
                        {([[candidate.left, "Kept"], [candidate.right, "Merged into it"]] as const).map(([entity, role]) => (
                          <div key={entity.uuid} className="min-w-0 space-y-1 rounded-[10px] border border-border bg-surface px-4 py-3">
                            <p className="label-12 text-fg-muted">{role}</p>
                            <p className="heading-16 break-words"><EntityName uuid={entity.uuid} name={entity.name} /></p>
                            {typeof entity.properties.description === "string" && entity.properties.description && (
                              <p className="copy-14 line-clamp-2 text-fg-muted">{entity.properties.description}</p>
                            )}
                          </div>
                        ))}
                      </div>
                      {reasons.length > 0 && (
                        <ul className="list-disc space-y-1 pl-5 copy-14 text-fg-muted">
                          {reasons.map((reason, index) => <li key={index}>{reason}</li>)}
                        </ul>
                      )}
                      <div className="flex flex-wrap gap-2">
                        <Button size="sm" disabled={locked} loading={busy && busyAction === "merge"} onClick={() => setMergeTarget(candidate)}>
                          <GitMerge />
                          {busy && busyAction === "merge" ? "Merging..." : "Merge"}
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={locked}
                          loading={busy && busyAction === "split"}
                          onClick={() => act(key, "split",
                            () => splitEntityCandidate(candidate.left.uuid, candidate.right.uuid, "Not the same entity"),
                            () => showToast(`Kept ${candidate.left.name} and ${candidate.right.name} split.`))}
                        >
                          <Split />
                          {busy && busyAction === "split" ? "Saving..." : "Keep split"}
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          disabled={locked}
                          loading={busy && busyAction === "ignore"}
                          onClick={() => act(key, "ignore",
                            () => ignoreEntityCandidate(candidate.left.uuid, candidate.right.uuid, "Reviewed"),
                            () => showToast(`Ignored ${candidate.left.name} and ${candidate.right.name}.`))}
                        >
                          <X />
                          {busy && busyAction === "ignore" ? "Saving..." : "Ignore"}
                        </Button>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </section>

          <section
            id="recent-merges"
            aria-labelledby="recent-title"
            className="scroll-mt-4 rounded-[10px] border border-border bg-surface min-[1180px]:sticky min-[1180px]:top-0 min-[1180px]:max-h-[calc(100dvh-80px)] min-[1180px]:overflow-y-auto"
          >
            <h2 id="recent-title" className="heading-16 px-4 pt-3 pb-2">Recent merges</h2>
            {mergesError ? (
              <div role="alert" className="flex flex-wrap items-center gap-3 px-4 pb-4 copy-14 text-error">
                <span>{mergesError}</span>
                <Button variant="outline" size="sm" onClick={() => loadMerges()}>Retry</Button>
              </div>
            ) : merges.length === 0 ? (
              <p className="px-4 pb-4 copy-14 text-fg-muted">No merges yet.</p>
            ) : (
              <ul className="divide-y divide-border border-t border-border">
                {merges.map((row) => {
                  const duplicate = row.duplicate_name ?? "Unnamed entity";
                  const primary = row.primary_name ?? "Unnamed entity";
                  return (
                    <li key={row.merge_id} className="flex items-start gap-3 px-4 py-3">
                      <div className="min-w-0 flex-1 space-y-1">
                        <p className="label-13 break-words text-fg">
                          {/* The merged entity exists again only after a clean undo. */}
                          {row.status === "reverted" ? <EntityName uuid={row.duplicate_uuid} name={duplicate} /> : duplicate}
                          <span className="text-fg-muted"> into </span>
                          <EntityName uuid={row.primary_uuid} name={primary} />
                        </p>
                        <p className="label-12 text-fg-muted tabular-nums"><time dateTime={row.created_at}>{when(row.created_at)}</time></p>
                        {row.risk_flags.length > 0 && (
                          <ul aria-label="Risk flags" className="flex flex-wrap gap-x-3 gap-y-1 label-12 text-fg-muted">
                            {row.risk_flags.map((flag) => (
                              <li key={flag}>
                                <Dot color={flag === "conflicting_identifiers" ? "var(--error)" : "var(--partial)"}>{flagWord(flag)}</Dot>
                              </li>
                            ))}
                          </ul>
                        )}
                        {row.status === "conflict" && (
                          <p className="label-12 text-fg-muted">Fields were reset and the documents were sent for reindexing.</p>
                        )}
                      </div>
                      {row.status === "reverted" ? (
                        <span className="inline-flex items-center gap-1 label-12 font-semibold text-fg-muted">
                          <Undo2 aria-hidden className="size-3.5" strokeWidth={1.75} />Undone
                        </span>
                      ) : row.status === "conflict" ? (
                        <span className="inline-flex items-center gap-1 label-12 font-semibold text-partial">
                          <TriangleAlert aria-hidden className="size-3.5" strokeWidth={1.75} />Conflict
                        </span>
                      ) : (
                        <Button size="sm" variant="outline" disabled={locked} loading={busyKey === row.merge_id} onClick={() => undo(row)}>
                          <Undo2 />
                          {busyKey === row.merge_id ? "Undoing..." : "Undo"}
                        </Button>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
        </div>
      </div>

      <AlertDialog open={mergeTarget !== null} onOpenChange={(open) => { if (!open) setMergeTarget(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {mergeTarget && `Merge ${mergeTarget.right.name} into ${mergeTarget.left.name}?`}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {mergeTarget &&
                `${mergeTarget.left.name} keeps its name and takes over the relationships and documents of ${mergeTarget.right.name}. ${mergeTarget.right.name} is then removed. You can undo it at once or later from Recent merges.`}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={() => { if (mergeTarget) merge(mergeTarget); setMergeTarget(null); }}>
              Merge
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <Toaster toast={toast} onDismiss={() => setToast(null)} disabled={locked} />
    </div>
  );
}
