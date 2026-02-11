"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { getStatus, postSync, postReindex, getTask, graphSearch } from "@/lib/api";
import type { StatusResponse } from "@/lib/types";
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
} from "lucide-react";

export default function DashboardPage() {
  const router = useRouter();
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTask, setActiveTask] = useState<{
    id: string;
    type: string;
    status: string;
    progress: number;
  } | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searching, setSearching] = useState(false);

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

  const pollTask = useCallback(
    (taskId: string, type: string) => {
      setActiveTask({ id: taskId, type, status: "running", progress: 10 });
      let progressVal = 10;
      const interval = setInterval(async () => {
        try {
          progressVal = Math.min(progressVal + Math.random() * 15, 90);
          const t = await getTask(taskId);
          if (t.status !== "running") {
            setActiveTask({ id: taskId, type, status: t.status, progress: 100 });
            clearInterval(interval);
            fetchStatus();
            setTimeout(() => setActiveTask(null), 3000);
          } else {
            setActiveTask((prev) => prev ? { ...prev, progress: progressVal } : null);
          }
        } catch {
          clearInterval(interval);
          setActiveTask(null);
        }
      }, 2000);
    },
    [fetchStatus]
  );

  const handleSync = async () => {
    try {
      const res = await postSync();
      pollTask(res.task_id, "Sync");
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const handleReindex = async () => {
    try {
      const res = await postReindex();
      pollTask(res.task_id, "Reindex");
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
      value: status.graph.nodes,
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
      value: status.embeddings,
      icon: Binary,
      color: "text-amber-400",
      bgColor: "bg-amber-500/10",
      description: "Vector embeddings",
    },
  ];

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
        {/* Quick Actions */}
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
                disabled={!!activeTask}
                className="gap-2 flex-1"
              >
                {activeTask?.type === "Sync" && activeTask.status === "running" ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Play className="h-4 w-4" />
                )}
                Sync New Docs
              </Button>
              <Button
                onClick={handleReindex}
                disabled={!!activeTask}
                variant="secondary"
                className="gap-2 flex-1"
              >
                {activeTask?.type === "Reindex" && activeTask.status === "running" ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4" />
                )}
                Full Reindex
              </Button>
            </div>

            {/* Task progress */}
            {activeTask && (
              <div className="space-y-2 rounded-lg bg-accent/50 p-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="font-medium">{activeTask.type}</span>
                  <Badge
                    variant={
                      activeTask.status === "completed" ? "default" :
                      activeTask.status === "failed" ? "destructive" : "secondary"
                    }
                    className="gap-1"
                  >
                    {activeTask.status === "completed" ? (
                      <CheckCircle2 className="h-3 w-3" />
                    ) : activeTask.status === "failed" ? (
                      <XCircle className="h-3 w-3" />
                    ) : (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    )}
                    {activeTask.status}
                  </Badge>
                </div>
                <Progress value={activeTask.progress} className="h-1.5" />
                <p className="text-xs text-muted-foreground truncate">
                  Task: {activeTask.id}
                </p>
              </div>
            )}

            {/* Existing active tasks */}
            {!activeTask && Object.keys(status.active_tasks).length > 0 && (
              <div className="space-y-2">
                {Object.entries(status.active_tasks).map(([id, s]) => (
                  <div key={id} className="flex items-center gap-2 rounded-lg bg-accent/50 p-2.5">
                    <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
                    <Badge variant="secondary" className="text-xs">{s}</Badge>
                    <span className="text-xs text-muted-foreground truncate flex-1">{id}</span>
                  </div>
                ))}
              </div>
            )}

            {!activeTask && Object.keys(status.active_tasks).length === 0 && (
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
                  {status.graph.nodes > 0
                    ? (status.graph.relationships / status.graph.nodes).toFixed(1)
                    : "0"}{" "}
                  rel/node
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Embedding Coverage</span>
                <span className="text-sm font-medium">
                  {status.graph.nodes > 0
                    ? Math.round((status.embeddings / status.graph.nodes) * 100)
                    : 0}%
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Active Tasks</span>
                <span className="text-sm font-medium">
                  {Object.keys(status.active_tasks).length}
                </span>
              </div>
              {status.graph.nodes > 0 && (
                <div className="pt-2 border-t">
                  <p className="text-xs text-muted-foreground mb-2">Knowledge Coverage</p>
                  <Progress
                    value={Math.min(
                      (status.embeddings / Math.max(status.graph.nodes, 1)) * 100,
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
    </div>
  );
}
