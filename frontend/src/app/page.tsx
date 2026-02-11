"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { getStatus, postSync, postReindex, getTask, cancelTask, graphSearch } from "@/lib/api";
import type { StatusResponse } from "@/lib/types";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  FileText,
  GitBranch,
  Database,
  Binary,
  Clock,
  RefreshCw,
  Play,
  Loader2,
  Search,
  CheckCircle2,
  XCircle,
  Activity,
  TrendingUp,
  SkipForward,
  AlertCircle,
  AlertTriangle,
} from "lucide-react";

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

function formatTime(seconds: number): string {
  if (!seconds || seconds <= 0) return "0:00";
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

export default function DashboardPage() {
  const router = useRouter();
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [activeTaskType, setActiveTaskType] = useState<string>("");
  const [taskProgress, setTaskProgress] = useState<TaskProgress | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searching, setSearching] = useState(false);
  const [confirmDialog, setConfirmDialog] = useState<{
    open: boolean;
    title: string;
    description: string;
    action: () => Promise<void>;
    variant: "default" | "destructive";
  }>({ open: false, title: "", description: "", action: async () => {}, variant: "default" });
  const [toast, setToast] = useState<{ message: string; type: "success" | "error" } | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  // Auto-dismiss toast
  useEffect(() => {
    if (toast) {
      const t = setTimeout(() => setToast(null), 4000);
      return () => clearTimeout(t);
    }
  }, [toast]);

  const fetchStatus = useCallback(() => {
    getStatus()
      .then(setStatus)
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    fetchStatus();
    const i = setInterval(fetchStatus, 10000);
    return () => clearInterval(i);
  }, [fetchStatus]);

  // Auto-attach to running tasks on page load
  useEffect(() => {
    if (activeTaskId || !status) return;
    const runningTasks = Object.entries(status.active_tasks).filter(
      ([, info]) => {
        const s = typeof info === "object" && info !== null ? (info as Record<string, string>).status : info;
        return s === "running";
      }
    );
    if (runningTasks.length > 0) {
      const [taskId, info] = runningTasks[0];
      setActiveTaskId(taskId);
      const taskType = typeof info === "object" && info !== null ? (info as Record<string, string>).type || "task" : "task";
      const label = taskType === "reindex" ? "Reindex" : taskType === "sync" ? "Sync" : taskType.charAt(0).toUpperCase() + taskType.slice(1);
      setActiveTaskType(label);
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
        if (t.status === "completed" || t.status === "failed") {
          clearInterval(interval);
          fetchStatus();
        }
      } catch {
        clearInterval(interval);
        setActiveTaskId(null);
        setTaskProgress(null);
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [activeTaskId, fetchStatus]);

  // Auto-scroll log
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [taskProgress?.recent_results]);

  const handleSync = () => {
    setConfirmDialog({
      open: true,
      title: "Sync New Documents",
      description: "This will check for new or modified documents in Paperless and process them. Existing documents won't be re-processed.",
      variant: "default",
      action: async () => {
        try {
          const res = await postSync();
          setActiveTaskId(res.task_id);
          setActiveTaskType("Sync");
          setTaskProgress(null);
          setToast({ message: "Sync started successfully", type: "success" });
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : "Failed to start sync";
          if (msg.includes("409")) {
            setToast({ message: "A task is already running", type: "error" });
          } else {
            setToast({ message: msg, type: "error" });
          }
        }
      },
    });
  };

  const handleReindex = () => {
    setConfirmDialog({
      open: true,
      title: "Full Reindex",
      description: "This will clear all graph data and re-process every document from scratch. This can take a while depending on the number of documents. Vector indexes and entity resolution will run automatically after.",
      variant: "destructive",
      action: async () => {
        try {
          const res = await postReindex();
          setActiveTaskId(res.task_id);
          setActiveTaskType("Reindex");
          setTaskProgress(null);
          setToast({ message: "Full reindex started", type: "success" });
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : "Failed to start reindex";
          if (msg.includes("409")) {
            setToast({ message: "A task is already running", type: "error" });
          } else {
            setToast({ message: msg, type: "error" });
          }
        }
      },
    });
  };

  const handleDismiss = () => {
    setActiveTaskId(null);
    setTaskProgress(null);
  };

  const handleCancel = async () => {
    if (!activeTaskId) return;
    try {
      await cancelTask(activeTaskId);
      setTaskProgress((prev) => prev ? { ...prev, status: "cancelled" } : null);
    } catch (e) {
      setError((e as Error).message);
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

  if (error && !status)
    return (
      <div className="flex h-full items-center justify-center">
        <Card className="max-w-md">
          <CardContent className="pt-6 text-center">
            <XCircle className="h-12 w-12 text-destructive mx-auto mb-3" />
            <p className="font-medium">Connection Error</p>
            <p className="text-sm text-muted-foreground mt-1">{error}</p>
            <Button onClick={fetchStatus} className="mt-4" variant="outline">
              Retry
            </Button>
          </CardContent>
        </Card>
      </div>
    );

  if (!status)
    return (
      <div className="space-y-6 p-8">
        <Skeleton className="h-10 w-64" />
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {[...Array(4)].map((_, i) => (
            <Skeleton key={i} className="h-28 rounded-xl" />
          ))}
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          <Skeleton className="h-40 rounded-xl" />
          <Skeleton className="h-40 rounded-xl" />
        </div>
      </div>
    );

  const totalEmbeddings = status.embeddings.document_chunks + status.embeddings.entity_embeddings;

  const cards = [
    {
      title: "Documents",
      value: status.graph.documents,
      icon: FileText,
      color: "text-blue-400",
      bgColor: "bg-blue-500/10",
      description: "Indexed documents",
    },
    {
      title: "Entities",
      value: status.graph.entities,
      icon: Database,
      color: "text-emerald-400",
      bgColor: "bg-emerald-500/10",
      description: "Knowledge nodes",
    },
    {
      title: "Relationships",
      value: status.graph.relationships,
      icon: GitBranch,
      color: "text-violet-400",
      bgColor: "bg-violet-500/10",
      description: "Graph connections",
    },
    {
      title: "Embeddings",
      value: totalEmbeddings,
      icon: Binary,
      color: "text-amber-400",
      bgColor: "bg-amber-500/10",
      description: `${status.embeddings.document_chunks} doc chunks + ${status.embeddings.entity_embeddings} entities`,
    },
  ];

  const tp = taskProgress;
  const isRunning = tp?.status === "running";
  const isDone = tp?.status === "completed" || tp?.status === "failed" || tp?.status === "cancelled";
  const totalDone = (tp?.processed || 0) + (tp?.skipped || 0) + (tp?.errors || 0);
  const progressPct = tp?.total_docs ? Math.round((totalDone / tp.total_docs) * 100) : 0;

  return (
    <div className="space-y-6 p-6 lg:p-8">
      {/* Header with search */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-sm text-muted-foreground flex items-center gap-1.5 mt-1">
            <Clock className="h-3.5 w-3.5" />
            Last sync: {status.last_sync ? new Date(status.last_sync).toLocaleString() : "Never"}
          </p>
        </div>
        <form
          onSubmit={(e) => { e.preventDefault(); handleSearch(); }}
          className="flex gap-2 w-full sm:w-auto sm:max-w-sm"
        >
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search the knowledge graph..."
              className="pl-9"
            />
          </div>
          <Button type="submit" size="icon" disabled={searching || !searchQuery.trim()}>
            {searching ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
          </Button>
        </form>
      </div>

      {/* Stat cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {cards.map((card, idx) => (
          <Card
            key={card.title}
            className="group hover:shadow-lg hover:shadow-primary/5 transition-all duration-300 border-border/50"
          >
            <CardContent className="pt-5 pb-4">
              <div className="flex items-start justify-between">
                <div className="space-y-2">
                  <p className="text-sm text-muted-foreground">{card.title}</p>
                  <p
                    className="text-3xl font-bold tracking-tight animate-count-up"
                    style={{ animationDelay: `${idx * 100}ms` }}
                  >
                    {card.value.toLocaleString()}
                  </p>
                  <p className="text-xs text-muted-foreground/70">{card.description}</p>
                </div>
                <div className={`${card.bgColor} rounded-lg p-2.5`}>
                  <card.icon className={`h-5 w-5 ${card.color}`} />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {/* Quick Actions + Progress Panel */}
        <Card className="border-border/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-base font-semibold flex items-center gap-2">
              <Activity className="h-4 w-4 text-primary" />
              Quick Actions
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex gap-3">
              <Button
                onClick={handleSync}
                disabled={!!activeTaskId && isRunning}
                className="gap-2 flex-1"
              >
                {activeTaskType === "Sync" && isRunning ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Play className="h-4 w-4" />
                )}
                Sync New Docs
              </Button>
              <Button
                onClick={handleReindex}
                disabled={!!activeTaskId && isRunning}
                variant="secondary"
                className="gap-2 flex-1"
              >
                {activeTaskType === "Reindex" && isRunning ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4" />
                )}
                Full Reindex
              </Button>
            </div>

            {/* Rich Progress Panel */}
            {tp && (
              <div className="space-y-3 rounded-lg bg-accent/50 p-4">
                {/* Header */}
                <div className="flex items-center justify-between">
                  <span className="font-medium text-sm">{activeTaskType}</span>
                  <div className="flex items-center gap-2">
                    <Badge
                      variant={
                        tp.status === "completed" ? "default" :
                        tp.status === "failed" ? "destructive" : "secondary"
                      }
                      className="gap-1"
                    >
                      {tp.status === "completed" ? (
                        <CheckCircle2 className="h-3 w-3" />
                      ) : tp.status === "failed" ? (
                        <XCircle className="h-3 w-3" />
                      ) : (
                        <Loader2 className="h-3 w-3 animate-spin" />
                      )}
                      {tp.status}
                    </Badge>
                    {isRunning && (
                      <Button variant="destructive" size="sm" className="h-6 px-2 text-xs" onClick={handleCancel}>
                        Cancel
                      </Button>
                    )}
                    {isDone && (
                      <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={handleDismiss}>
                        Dismiss
                      </Button>
                    )}
                  </div>
                </div>

                {/* Progress bar */}
                <Progress value={progressPct} className="h-2" />

                {/* Stats line */}
                <p className="text-xs text-muted-foreground">
                  {totalDone}/{tp.total_docs || "?"} docs
                  {tp.skipped > 0 && <span> ({tp.skipped} skipped</span>}
                  {tp.errors > 0 && <span>{tp.skipped > 0 ? ", " : " ("}{tp.errors} error{tp.errors !== 1 ? "s" : ""}</span>}
                  {(tp.skipped > 0 || tp.errors > 0) && ")"}
                  {tp.docs_per_minute > 0 && <span> — {tp.docs_per_minute} docs/min</span>}
                  {isRunning && tp.estimated_remaining_seconds > 0 && (
                    <span> — ~{formatTime(tp.estimated_remaining_seconds)} remaining</span>
                  )}
                </p>

                {/* Current doc */}
                {isRunning && tp.current_doc && (
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <Loader2 className="h-3 w-3 animate-spin shrink-0" />
                    <span className="truncate">{tp.current_doc}</span>
                  </div>
                )}

                {/* Live log */}
                {tp.recent_results && tp.recent_results.length > 0 && (
                  <ScrollArea className="h-36 rounded-md border border-border/50 bg-background/50">
                    <div className="p-2 space-y-1">
                      {tp.recent_results.map((r, i) => (
                        <div key={`${r.doc_id}-${i}`} className="flex items-start gap-1.5 text-xs">
                          {r.status === "processed" ? (
                            <CheckCircle2 className="h-3.5 w-3.5 text-emerald-400 shrink-0 mt-0.5" />
                          ) : r.status === "skipped" ? (
                            <SkipForward className="h-3.5 w-3.5 text-muted-foreground shrink-0 mt-0.5" />
                          ) : (
                            <AlertCircle className="h-3.5 w-3.5 text-red-400 shrink-0 mt-0.5" />
                          )}
                          <span className="truncate text-muted-foreground">
                            {r.title || `Doc #${r.doc_id}`}
                          </span>
                          {r.status === "processed" && r.entities !== undefined && (
                            <span className="shrink-0 text-muted-foreground/60 ml-auto">
                              {r.entities}e
                            </span>
                          )}
                          {r.status === "error" && r.error && (
                            <span className="shrink-0 text-red-400/80 ml-auto truncate max-w-[120px]">
                              {r.error}
                            </span>
                          )}
                        </div>
                      ))}
                      <div ref={logEndRef} />
                    </div>
                  </ScrollArea>
                )}

                {/* Final summary */}
                {isDone && (
                  <div className="text-xs text-muted-foreground border-t border-border/50 pt-2">
                    {tp.status === "completed" ? "✅" : "❌"} {activeTaskType} {tp.status} in {formatTime(tp.elapsed_seconds)}
                    {" — "}{tp.processed} processed, {tp.skipped} skipped, {tp.errors} errors
                  </div>
                )}
              </div>
            )}

            {/* Existing active tasks (from other sessions) */}
            {!tp && Object.keys(status.active_tasks).length > 0 && (
              <div className="space-y-2">
                {Object.entries(status.active_tasks).map(([id, info]) => {
                  const taskStatus = typeof info === "object" ? info.status : info;
                  const taskType = typeof info === "object" ? info.type : "";
                  return (
                    <div key={id} className="flex items-center gap-2 rounded-lg bg-accent/50 p-2.5">
                      <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
                      <Badge variant="secondary" className="text-xs">{taskType || taskStatus}</Badge>
                      <span className="text-xs text-muted-foreground truncate flex-1">{id}</span>
                    </div>
                  );
                })}
              </div>
            )}

            {!tp && Object.keys(status.active_tasks).length === 0 && (
              <p className="text-sm text-muted-foreground text-center py-2">
                No active tasks — system is idle
              </p>
            )}
          </CardContent>
        </Card>

        {/* System Overview */}
        <Card className="border-border/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-base font-semibold flex items-center gap-2">
              <TrendingUp className="h-4 w-4 text-primary" />
              System Overview
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Status</span>
                <Badge variant={status.status === "healthy" ? "default" : "destructive"} className="gap-1">
                  {status.status === "healthy" ? (
                    <CheckCircle2 className="h-3 w-3" />
                  ) : (
                    <XCircle className="h-3 w-3" />
                  )}
                  {status.status}
                </Badge>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Graph Density</span>
                <span className="text-sm font-medium">
                  {status.graph.entities > 0
                    ? (status.graph.relationships / status.graph.entities).toFixed(1)
                    : "0"}{" "}
                  rel/entity
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Embedding Coverage</span>
                <span className="text-sm font-medium">
                  {status.graph.documents > 0
                    ? Math.round((status.embeddings.docs_with_embeddings / status.graph.documents) * 100)
                    : 0}%
                  <span className="text-muted-foreground text-xs ml-1">
                    ({status.embeddings.docs_with_embeddings}/{status.graph.documents} docs)
                  </span>
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Active Tasks</span>
                <span className="text-sm font-medium">
                  {Object.keys(status.active_tasks).length}
                </span>
              </div>
              {status.graph.documents > 0 && (
                <div className="pt-2 border-t">
                  <p className="text-xs text-muted-foreground mb-2">Embedding Coverage</p>
                  <Progress
                    value={Math.min(
                      (status.embeddings.docs_with_embeddings / Math.max(status.graph.documents, 1)) * 100,
                      100
                    )}
                    className="h-2"
                  />
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
      {/* Confirmation Dialog */}
      <Dialog open={confirmDialog.open} onOpenChange={(open) => setConfirmDialog((prev) => ({ ...prev, open }))}>
        <DialogContent showCloseButton={false}>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {confirmDialog.variant === "destructive" && (
                <AlertTriangle className="h-5 w-5 text-destructive" />
              )}
              {confirmDialog.title}
            </DialogTitle>
            <DialogDescription>{confirmDialog.description}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmDialog((prev) => ({ ...prev, open: false }))}>
              Cancel
            </Button>
            <Button
              variant={confirmDialog.variant}
              onClick={async () => {
                setConfirmDialog((prev) => ({ ...prev, open: false }));
                await confirmDialog.action();
              }}
            >
              {confirmDialog.variant === "destructive" ? "Yes, Reindex Everything" : "Start"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Toast notification */}
      {toast && (
        <div
          className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-lg px-4 py-3 text-sm font-medium shadow-lg transition-all animate-in slide-in-from-bottom-4 fade-in ${
            toast.type === "success"
              ? "bg-emerald-500/90 text-white"
              : "bg-destructive/90 text-destructive-foreground"
          }`}
        >
          {toast.type === "success" ? (
            <CheckCircle2 className="h-4 w-4" />
          ) : (
            <XCircle className="h-4 w-4" />
          )}
          {toast.message}
        </div>
      )}
    </div>
  );
}
