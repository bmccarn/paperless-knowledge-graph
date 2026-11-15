"use client";

import { useEffect, useLayoutEffect, useState, useCallback, useRef, type ReactNode } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
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
import { StatusLabel } from "@/components/status-label";
import { TypeDot } from "@/components/kind-shape";
import { errMsg } from "@/lib/utils";
import { answerState, type AnswerMessage, type AnswerState } from "@/lib/answer-state";
import {
  getStatus, postSync, postReindex, postRepairDrift, getTask, cancelTask, graphSearch,
  listConversations, getConversation, getEntityReviewCandidates, getDocuments,
} from "@/lib/api";
import type { FreshnessDocumentRef, FreshnessStatus, HealthyStatusResponse } from "@/lib/types";
import { ChevronDown, CircleAlert, CircleCheck, CircleX, Search, SkipForward, TriangleAlert } from "lucide-react";

interface TaskProgress {
  status: string;
  started: string;
  total_docs: number;
  processed: number;
  skipped: number;
  errors: number;
  current_doc: string;
  elapsed_seconds: number;
  docs_per_minute: number;
  estimated_remaining_seconds: number;
  recent_results: Array<{
    doc_id: number;
    title: string;
    status: string;
    entities?: number;
    relationships?: number;
    error?: string;
  }>;
  result?: unknown;
  error?: string;
}

interface RecentAnswer { id: string; question: string; state: AnswerState }
/** Each column is undefined while loading and null when its request failed. */
interface Overview { answers?: RecentAnswer[] | null; duplicates?: number | null; types?: [string, number][] | null }

const DUPLICATE_LIMIT = 50;

function formatTime(seconds: number): string {
  if (!seconds || seconds <= 0) return "0:00";
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

function formatRefs(refs: (FreshnessDocumentRef | number)[] = []): string {
  // Bare IDs are short, so show more of them than titled document refs.
  const limit = typeof refs[0] === "number" ? 8 : 5;
  const shown = refs.slice(0, limit).map((ref) =>
    typeof ref === "number" ? `#${ref}` : `#${ref.id}${ref.title ? ` ${ref.title}` : ""}`);
  const more = refs.length > limit ? ` +${refs.length - limit} more` : "";
  return `${shown.join(", ")}${more}`;
}

const DRIFT_ROWS: [string, keyof FreshnessStatus, Exclude<keyof NonNullable<FreshnessStatus["drift"]>, "sample_limit">][] = [
  ["Missing from the graph", "missing_documents", "missing_from_graph"],
  ["Extra in the graph", "extra_documents", "extra_in_graph"],
  ["Missing embeddings", "missing_embedding_documents", "missing_embeddings"],
  ["Extra embeddings", "extra_embedding_documents", "extra_embeddings"],
  ["Missing hashes", "missing_hash_documents", "missing_hashes"],
  ["Extra hashes", "extra_hash_documents", "extra_hashes"],
  ["Modified after sync", "modified_after_last_sync_documents", "modified_after_last_sync"],
  ["Changed since indexing", "changed_since_index_documents", "changed_since_index"],
];

const TASK_LABELS: Record<string, string> = {
  reindex: "Full reindex",
  sync: "Sync",
  "scheduled-sync": "Scheduled sync",
  "repair-drift": "Repair drift",
  "reindex-document": "Document reindex",
  entity_steward: "Entity steward",
};

function taskLabel(taskType: string): string {
  const words = taskType.replace(/[-_]/g, " ");
  return TASK_LABELS[taskType] || words.charAt(0).toUpperCase() + words.slice(1);
}

const TASK_STATES: Record<string, string> = {
  running: "Running", cancelling: "Cancelling", completed: "Completed", failed: "Failed", cancelled: "Cancelled",
};

/** "The index was updated at 12:00." Adds the date when the update was not today. */
function indexAge(lastSync: string | null): string {
  if (!lastSync) return "The index has not been updated yet.";
  const at = new Date(lastSync);
  const time = at.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
  if (at.toDateString() === new Date().toDateString()) return `The index was updated at ${time}.`;
  return `The index was updated on ${at.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" })} at ${time}.`;
}

type StoredMessage = AnswerMessage & { role: string };

/** The question and stored state of the last answer in each of the five newest conversations. */
async function recentAnswers(): Promise<RecentAnswer[]> {
  const list: { id: string }[] = (await listConversations(5)).slice(0, 5);
  const conversations = await Promise.all(list.map((row) => getConversation(row.id)));
  return conversations.flatMap((conversation): RecentAnswer[] => {
    const messages: StoredMessage[] = conversation.messages ?? [];
    const index = messages.findLastIndex((message) => message.role === "assistant");
    if (index < 0) return [];
    const question = messages.slice(0, index).findLast((message) => message.role === "user")?.content ?? conversation.title;
    return [{ id: conversation.id, question, state: answerState(messages[index]).state }];
  });
}

function Column({ title, value, error, onRetry, children }: {
  title: string; value: unknown; error: string; onRetry: () => void; children: ReactNode;
}) {
  const id = `${title.toLowerCase().replace(/\s+/g, "-")}-title`;
  return (
    <section aria-labelledby={id} aria-busy={value === undefined} className="min-w-0 space-y-3 border-t border-border pt-3">
      <h2 id={id} className="heading-16">{title}</h2>
      {value === undefined ? (
        <div className="space-y-2">{[0, 1, 2].map((row) => <Skeleton key={row} className="h-5" />)}</div>
      ) : value === null ? (
        <div role="alert" className="flex flex-wrap items-center gap-3 copy-14 text-error">
          {error}
          <Button size="sm" variant="outline" onClick={onRetry}>Retry</Button>
        </div>
      ) : children}
    </section>
  );
}

export default function DashboardPage() {
  const router = useRouter();
  const [status, setStatus] = useState<HealthyStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [activeTaskType, setActiveTaskType] = useState<string>("");
  const [taskProgress, setTaskProgress] = useState<TaskProgress | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searching, setSearching] = useState(false);
  const [confirm, setConfirm] = useState<"reindex" | "repair" | null>(null);
  const [toast, setToast] = useState<ToastMessage | null>(null);
  const [overview, setOverview] = useState<Overview>({});
  const logRef = useRef<HTMLDivElement>(null);
  const followLog = useRef(true);
  const opener = useRef<HTMLButtonElement | null>(null);

  const showToast = (text: string) => setToast({ id: Date.now(), text });

  const fetchStatus = useCallback(() => {
    getStatus()
      .then((s) => {
        if (s.status === "degraded") {
          setError(s.error || "Backend degraded");
          return;
        }
        setStatus(s);
        setError(null);
      })
      .catch((e) => setError(e.message));
  }, []);

  const loadOverview = useCallback(() => {
    setOverview({});
    Promise.allSettled([
      recentAnswers(),
      getEntityReviewCandidates(DUPLICATE_LIMIT),
      getDocuments("", "", 0, 1),
    ]).then(([answers, duplicates, documents]) => setOverview({
      answers: answers.status === "fulfilled" ? answers.value : null,
      duplicates: duplicates.status === "fulfilled" ? duplicates.value.candidates.length : null,
      types: documents.status === "fulfilled"
        ? (Object.entries(documents.value.doc_types ?? {}) as [string, number][]).sort((a, b) => b[1] - a[1])
        : null,
    }));
  }, []);

  useEffect(() => {
    fetchStatus();
    loadOverview();
    const i = setInterval(fetchStatus, 10000);
    return () => clearInterval(i);
  }, [fetchStatus, loadOverview]);

  // Auto-attach to running tasks on page load
  useEffect(() => {
    if (activeTaskId || !status) return;
    const runningTasks = Object.entries(status.active_tasks).filter(
      ([, info]) => {
        const s = typeof info === "object" && info !== null ? (info as Record<string, string>).status : info;
        return s === "running" || s === "cancelling";
      }
    );
    if (runningTasks.length > 0) {
      const [taskId, info] = runningTasks[0];
      setActiveTaskId(taskId);
      const taskType = typeof info === "object" && info !== null ? (info as Record<string, string>).type || "task" : "task";
      setActiveTaskType(taskLabel(taskType));
      // Immediately fetch task progress instead of waiting for poll interval
      getTask(taskId).then((t) => setTaskProgress(t as TaskProgress)).catch(() => {});
    }
  }, [status, activeTaskId]);

  // Poll task progress
  useEffect(() => {
    if (!activeTaskId) return;
    const interval = setInterval(async () => {
      try {
        const t = await getTask(activeTaskId);
        setTaskProgress(t as TaskProgress);
        if (t.status === "completed" || t.status === "failed" || t.status === "cancelled") {
          clearInterval(interval);
          fetchStatus();
          loadOverview();
        }
      } catch {
        clearInterval(interval);
        setActiveTaskId(null);
        setTaskProgress(null);
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [activeTaskId, fetchStatus, loadOverview]);

  // Follow new log lines only while the log is scrolled to the bottom. Scroll the log itself, never the page.
  useLayoutEffect(() => {
    const log = logRef.current;
    if (log && followLog.current) log.scrollTop = log.scrollHeight;
  }, [taskProgress?.recent_results]);

  const startTask = async (start: () => Promise<{ task_id?: string; status?: string }>, label: string, startedMsg: string, failMsg: string) => {
    try {
      const res = await start();
      if (res.status === "noop" || !res.task_id) {
        showToast("No drift to repair.");
        fetchStatus();
        return;
      }
      followLog.current = true;
      setActiveTaskId(res.task_id);
      setActiveTaskType(label);
      setTaskProgress(null);
      showToast(startedMsg);
    } catch (e: unknown) {
      const msg = errMsg(e, failMsg);
      showToast(msg.includes("409") ? "A task is already running." : msg);
    }
  };

  const handleSync = () => startTask(postSync, "Sync", "Sync started.", "Failed to start sync");

  const handleDismiss = () => {
    setActiveTaskId(null);
    setTaskProgress(null);
  };

  const handleCancel = async () => {
    if (!activeTaskId) return;
    try {
      const result = await cancelTask(activeTaskId);
      setTaskProgress((prev) => prev ? { ...prev, status: result.status } : null);
    } catch (e) {
      showToast(errMsg(e, "Failed to cancel task"));
    }
  };

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setSearching(true);
    try {
      const data = await graphSearch(searchQuery);
      if (data.results?.length > 0) {
        router.push(`/graph?q=${encodeURIComponent(searchQuery)}`);
      } else {
        router.push(`/query?q=${encodeURIComponent(searchQuery)}`);
      }
    } catch {
      router.push(`/query?q=${encodeURIComponent(searchQuery)}`);
    } finally {
      setSearching(false);
    }
  };

  const frame = (children: ReactNode) => (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-[1280px] space-y-6 p-4 md:px-12 md:py-10">{children}</div>
      <Toaster toast={toast} onDismiss={() => setToast(null)} />
    </div>
  );

  if (error && !status)
    return frame(<>
      <h1 className="page-title">Status</h1>
      <Alert variant="destructive">
        <CircleX aria-hidden strokeWidth={1.75} />
        <AlertTitle>The status could not be loaded.</AlertTitle>
        <AlertDescription>
          <p>{error}</p>
          <Button onClick={fetchStatus} size="sm" variant="outline" className="mt-1">Retry</Button>
        </AlertDescription>
      </Alert>
    </>);

  if (!status)
    return frame(<>
      <h1 className="page-title">Status</h1>
      <div role="status" aria-busy="true" aria-label="Loading status" className="space-y-6">
        <Skeleton className="h-6 max-w-xl" />
        <div className="flex gap-2"><Skeleton className="h-9 w-44" /><Skeleton className="h-9 w-28" /><Skeleton className="h-9 w-28" /></div>
        <div className="grid gap-6 md:grid-cols-3">{[0, 1, 2].map((column) => <Skeleton key={column} className="h-32" />)}</div>
      </div>
    </>);

  const tp = taskProgress;
  const isCancelling = tp?.status === "cancelling";
  const isRunning = !!activeTaskId && (!tp || tp.status === "running" || isCancelling);
  const isDone = tp?.status === "completed" || tp?.status === "failed" || tp?.status === "cancelled";
  const totalDone = (tp?.processed || 0) + (tp?.skipped || 0) + (tp?.errors || 0);
  const freshness = status.freshness;
  const lastFailedExtraction = freshness?.last_failed_extraction;
  const freshnessDriftDetails = freshness
    ? DRIFT_ROWS.map(([label, countKey, refsKey]) => ({
        label,
        count: Number(freshness[countKey]) || 0,
        detail: formatRefs(freshness.drift?.[refsKey]),
      })).filter((item) => item.count > 0)
    : [];
  const behind = !!freshness && (freshness.stale || freshnessDriftDetails.length > 0);
  const { answers, duplicates, types } = overview;

  return frame(<>
    <header className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
      <h1 className="page-title">Status</h1>
      <form
        role="search"
        onSubmit={(e) => { e.preventDefault(); handleSearch(); }}
        className="flex w-full gap-2 md:max-w-sm"
      >
        <div className="relative flex-1">
          <Search aria-hidden className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-fg-muted" strokeWidth={1.75} />
          <Input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search the knowledge graph..."
            aria-label="Search the knowledge graph"
            className="pl-9"
          />
        </div>
        <Button type="submit" variant="outline" size="icon" aria-label="Search" loading={searching} disabled={!searchQuery.trim()}>
          <Search strokeWidth={1.75} />
        </Button>
      </form>
    </header>

    <p className="heading-16 max-w-(--measure)">
      The archive holds {status.graph.documents.toLocaleString()} documents and {status.graph.entities.toLocaleString()} entities.{" "}
      {indexAge(status.last_sync)}
    </p>

    {error && (
      <Alert variant="destructive">
        <CircleX aria-hidden strokeWidth={1.75} />
        <AlertTitle>{error}</AlertTitle>
        <AlertDescription>Showing the last successful status. Counts may be out of date.</AlertDescription>
      </Alert>
    )}

    {behind && (
      <Alert variant="warning">
        <TriangleAlert aria-hidden strokeWidth={1.75} />
        <AlertTitle className="line-clamp-none">The index is behind Paperless.</AlertTitle>
        <AlertDescription className="text-fg">
          <p>
            Paperless has {freshness.paperless_documents.toLocaleString()} documents and the index has {freshness.indexed_documents.toLocaleString()}.
            {freshness.missing_documents > 0 && ` ${freshness.missing_documents.toLocaleString()} ${freshness.missing_documents === 1 ? "is" : "are"} missing from the index.`}
          </p>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <Button size="sm" onClick={handleSync} disabled={isRunning}>Sync now</Button>
            {(freshnessDriftDetails.length > 0 || freshness.latest_paperless_title) && (
              <Collapsible>
                <CollapsibleTrigger className="group inline-flex min-h-8 items-center gap-1 label-13 text-fg underline underline-offset-4">
                  Details<ChevronDown aria-hidden className="size-4 transition-transform duration-(--dur-3) group-data-[state=open]:rotate-180" strokeWidth={1.75} />
                </CollapsibleTrigger>
                <CollapsibleContent className="space-y-1 pt-2 label-13 text-fg-muted">
                  {freshnessDriftDetails.map((item) => (
                    <p key={item.label}>
                      <span className="font-medium text-fg">{item.label}:</span>{" "}
                      <span className="tabular-nums">{item.count.toLocaleString()}</span>
                      {item.detail && <span> ({item.detail})</span>}
                    </p>
                  ))}
                  {freshness.latest_paperless_title && (
                    <p>
                      Latest Paperless change: {freshness.latest_paperless_title}
                      {freshness.latest_paperless_modified && ` (${new Date(freshness.latest_paperless_modified).toLocaleString()})`}
                    </p>
                  )}
                </CollapsibleContent>
              </Collapsible>
            )}
          </div>
        </AlertDescription>
      </Alert>
    )}

    <div className="grid gap-2 sm:flex sm:flex-wrap">
      <Button onClick={handleSync} disabled={isRunning} loading={isRunning && activeTaskType === "Sync"}>Sync new documents</Button>
      <Button variant="outline" onClick={(e) => { opener.current = e.currentTarget; setConfirm("repair"); }} disabled={isRunning} loading={isRunning && activeTaskType === "Repair drift"}>Repair drift</Button>
      <Button variant="outline" onClick={(e) => { opener.current = e.currentTarget; setConfirm("reindex"); }} disabled={isRunning} loading={isRunning && activeTaskType === "Full reindex"}>Full reindex</Button>
    </div>

    {tp && (
      <section aria-labelledby="task-title" className="space-y-3 rounded-[10px] border border-border bg-surface p-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 id="task-title" className="heading-16">
            {activeTaskType} <span className="label-13 font-normal text-fg-muted">{TASK_STATES[tp.status] ?? tp.status}</span>
          </h2>
          {tp.status === "running" && <Button variant="destructive" size="sm" onClick={handleCancel}>Cancel</Button>}
          {isDone && <Button variant="ghost" size="sm" onClick={handleDismiss}>Dismiss</Button>}
        </div>
        <Progress value={totalDone} max={tp.total_docs || 1} aria-label={`${activeTaskType} progress`} />
        {isCancelling && (
          <p role="status" className="label-13 text-fg-muted">
            Cancelling. Finishing documents already in progress before another task can start.
          </p>
        )}
        <p className="label-13 text-fg-muted tabular-nums">
          {totalDone} of {tp.total_docs || "?"} documents
          {tp.skipped > 0 && `, ${tp.skipped} skipped`}
          {tp.errors > 0 && `, ${tp.errors} error${tp.errors !== 1 ? "s" : ""}`}.
          {tp.docs_per_minute > 0 && ` ${tp.docs_per_minute} per minute.`}
          {isRunning && tp.estimated_remaining_seconds > 0 && ` About ${formatTime(tp.estimated_remaining_seconds)} left.`}
          {isDone && ` ${TASK_STATES[tp.status]} in ${formatTime(tp.elapsed_seconds)}.`}
        </p>
        {isRunning && tp.current_doc && <p className="truncate label-13 text-fg-muted">Now: {tp.current_doc}</p>}
        {tp.recent_results?.length > 0 && (
          <div
            ref={logRef}
            role="log"
            aria-label={`${activeTaskType} log`}
            tabIndex={0}
            onScroll={(e) => {
              const log = e.currentTarget;
              followLog.current = log.scrollHeight - log.scrollTop - log.clientHeight < 8;
            }}
            className="max-h-40 overflow-y-auto rounded-md bg-sunken p-2"
          >
            <ol className="space-y-1">
              {tp.recent_results.map((r, i) => (
                <li key={`${r.doc_id}-${i}`} className="flex items-start gap-1.5 label-12 text-fg-muted">
                  {r.status === "processed" ? (
                    <CircleCheck aria-label="Processed" className="mt-0.5 size-3.5 shrink-0 text-verified" strokeWidth={1.75} />
                  ) : r.status === "skipped" ? (
                    <SkipForward aria-label="Skipped" className="mt-0.5 size-3.5 shrink-0" strokeWidth={1.75} />
                  ) : (
                    <CircleAlert aria-label="Error" className="mt-0.5 size-3.5 shrink-0 text-error" strokeWidth={1.75} />
                  )}
                  <span className="min-w-0 truncate">{r.title || `Document #${r.doc_id}`}</span>
                  {r.status === "processed" && r.entities !== undefined && (
                    <span className="ml-auto shrink-0 tabular-nums">{r.entities} entities</span>
                  )}
                  {r.status === "error" && r.error && (
                    <span className="ml-auto max-w-[160px] shrink-0 truncate text-error">{r.error}</span>
                  )}
                </li>
              ))}
            </ol>
          </div>
        )}
      </section>
    )}

    {lastFailedExtraction && (
      <Alert variant="destructive">
        <CircleX aria-hidden strokeWidth={1.75} />
        <AlertTitle className="line-clamp-none">
          The last extraction failed: {lastFailedExtraction.title || `Document #${lastFailedExtraction.doc_id}`}
        </AlertTitle>
        {lastFailedExtraction.error && <AlertDescription><p className="line-clamp-2">{lastFailedExtraction.error}</p></AlertDescription>}
      </Alert>
    )}

    <div className="grid gap-6 md:grid-cols-3">
      <Column title="Recent answers" value={answers} error="Could not load recent answers." onRetry={loadOverview}>
        {answers?.length ? (
          <ul className="space-y-2">
            {answers.map((answer) => (
              <li key={answer.id} className="flex items-start gap-3">
                <StatusLabel status={answer.state} size="sm" className="mt-0.5 w-24 shrink-0" />
                <span className="min-w-0 line-clamp-2 label-13 text-fg">{answer.question}</span>
              </li>
            ))}
          </ul>
        ) : (
          <p className="copy-14 text-fg-muted">
            No answers yet. <Link href="/query" className="text-fg underline underline-offset-4">Ask a question</Link>
          </p>
        )}
      </Column>
      <Column title="Possible duplicates" value={duplicates} error="Could not load possible duplicates." onRetry={loadOverview}>
        {duplicates ? (
          <p className="copy-14 text-fg-muted">
            <span className="heading-16 text-fg tabular-nums">{duplicates === DUPLICATE_LIMIT ? `${DUPLICATE_LIMIT}+` : duplicates}</span>{" "}
            {duplicates === 1 ? "pair" : "pairs"} to review.{" "}
            <Link href="/entities/review" className="text-fg underline underline-offset-4">Review on Entities</Link>
          </p>
        ) : (
          <p className="copy-14 text-fg-muted">No possible duplicates right now.</p>
        )}
      </Column>
      <Column title="Archive by type" value={types} error="Could not load document types." onRetry={loadOverview}>
        {types?.length ? (
          <ul className="space-y-1.5">
            {types.map(([type, count]) => (
              <li key={type} className="flex items-center justify-between gap-3">
                <TypeDot type={type} />
                <span className="label-13 text-fg tabular-nums">{count.toLocaleString()}</span>
              </li>
            ))}
          </ul>
        ) : (
          <p className="copy-14 text-fg-muted">No documents indexed yet.</p>
        )}
      </Column>
    </div>

    <AlertDialog open={confirm !== null} onOpenChange={(open) => { if (!open) setConfirm(null); }}>
      {/* Without a Trigger, Radix has nowhere to return focus; send it back to the button that opened the dialog. */}
      <AlertDialogContent onCloseAutoFocus={(e) => { e.preventDefault(); opener.current?.focus(); }}>
        <AlertDialogHeader>
          <AlertDialogTitle>{confirm === "reindex" ? "Reindex every document?" : "Repair drift?"}</AlertDialogTitle>
          <AlertDialogDescription>
            {confirm === "reindex"
              ? "This prepares and replaces the index for every document. Existing document data stays available while each replacement is prepared. It can take a long time; vector indexes and entity resolution run afterwards."
              : "This reindexes only the drifted documents that the freshness check reported, and removes graph, vector and hash data for documents no longer in Paperless."}
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction onClick={() => confirm === "reindex"
            ? startTask(postReindex, "Full reindex", "Full reindex started.", "Failed to start reindex")
            : startTask(postRepairDrift, "Repair drift", "Drift repair started.", "Failed to start drift repair")}>
            {confirm === "reindex" ? "Full reindex" : "Repair drift"}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  </>);
}
