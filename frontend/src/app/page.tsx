"use client";

import { useEffect, useState, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { getStatus, postSync, postReindex, getTask } from "@/lib/api";
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
} from "lucide-react";

export default function DashboardPage() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTask, setActiveTask] = useState<{
    id: string;
    type: string;
    status: string;
  } | null>(null);

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
      setActiveTask({ id: taskId, type, status: "running" });
      const interval = setInterval(async () => {
        try {
          const t = await getTask(taskId);
          if (t.status !== "running") {
            setActiveTask({ id: taskId, type, status: t.status });
            clearInterval(interval);
            fetchStatus();
            setTimeout(() => setActiveTask(null), 3000);
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

  if (error && !status)
    return (
      <div className="p-8 text-destructive">
        Failed to load status: {error}
      </div>
    );
  if (!status)
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );

  const cards = [
    {
      title: "Documents",
      value: status.graph.documents,
      icon: FileText,
      color: "text-blue-500",
    },
    {
      title: "Nodes",
      value: status.graph.nodes,
      icon: Database,
      color: "text-green-500",
    },
    {
      title: "Relationships",
      value: status.graph.relationships,
      icon: GitBranch,
      color: "text-purple-500",
    },
    {
      title: "Embeddings",
      value: status.embeddings,
      icon: Binary,
      color: "text-orange-500",
    },
  ];

  return (
    <div className="space-y-6 p-8">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Clock className="h-4 w-4" />
          Last sync:{" "}
          {status.last_sync
            ? new Date(status.last_sync).toLocaleString()
            : "Never"}
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {cards.map((card) => (
          <Card key={card.title}>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">
                {card.title}
              </CardTitle>
              <card.icon className={`h-4 w-4 ${card.color}`} />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {card.value.toLocaleString()}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Quick Actions</CardTitle>
          </CardHeader>
          <CardContent className="flex gap-3">
            <Button
              onClick={handleSync}
              disabled={!!activeTask}
              className="gap-2"
            >
              {activeTask?.type === "Sync" &&
              activeTask.status === "running" ? (
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
              className="gap-2"
            >
              {activeTask?.type === "Reindex" &&
              activeTask.status === "running" ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
              Full Reindex
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Active Tasks</CardTitle>
          </CardHeader>
          <CardContent>
            {activeTask ? (
              <div className="flex items-center gap-2">
                <Badge
                  variant={
                    activeTask.status === "completed"
                      ? "default"
                      : activeTask.status === "failed"
                      ? "destructive"
                      : "secondary"
                  }
                >
                  {activeTask.status}
                </Badge>
                <span className="text-sm">{activeTask.type}</span>
                <span className="text-xs text-muted-foreground truncate">
                  {activeTask.id}
                </span>
              </div>
            ) : Object.keys(status.active_tasks).length > 0 ? (
              <div className="space-y-2">
                {Object.entries(status.active_tasks).map(([id, s]) => (
                  <div key={id} className="flex items-center gap-2">
                    <Badge variant="secondary">{s}</Badge>
                    <span className="text-xs text-muted-foreground truncate">
                      {id}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                No active tasks
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
