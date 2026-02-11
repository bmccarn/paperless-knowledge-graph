"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import { Button } from "@/components/ui/button";

import { cn } from "@/lib/utils";

interface LogLine {
  timestamp: string;
  level: string;
  logger: string;
  message: string;
}

const LEVEL_COLORS: Record<string, string> = {
  DEBUG: "text-zinc-500",
  INFO: "text-zinc-200",
  WARNING: "text-amber-400",
  ERROR: "text-red-400",
};

const LEVEL_ORDER = ["DEBUG", "INFO", "WARNING", "ERROR"];

function formatTime(iso: string) {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return iso;
  }
}

function shortLogger(name: string) {
  const parts = name.split(".");
  return parts[parts.length - 1] || name;
}

export default function DebugPage() {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [level, setLevel] = useState("INFO");
  const [paused, setPaused] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [connected, setConnected] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const pausedRef = useRef(false);
  const linesRef = useRef<LogLine[]>([]);

  // Keep refs in sync
  useEffect(() => { pausedRef.current = paused; }, [paused]);
  useEffect(() => { linesRef.current = lines; }, [lines]);

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && !paused) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [lines, autoScroll, paused]);

  // Fetch initial logs + SSE
  useEffect(() => {
    const API_URL = process.env.NEXT_PUBLIC_API_URL || "/api";

    // Fetch history
    fetch(`${API_URL}/logs?limit=200`)
      .then((r) => r.json())
      .then((data) => {
        setLines(data.lines || []);
      })
      .catch(console.error);

    // SSE
    let es: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout>;

    function connect() {
      es = new EventSource(`${API_URL}/logs/stream`);
      es.onopen = () => setConnected(true);
      es.onmessage = (e) => {
        try {
          const line: LogLine = JSON.parse(e.data);
          if (!pausedRef.current) {
            setLines((prev) => {
              const next = [...prev, line];
              return next.length > 2000 ? next.slice(-1500) : next;
            });
          }
        } catch {}
      };
      es.onerror = () => {
        setConnected(false);
        es?.close();
        retryTimer = setTimeout(connect, 3000);
      };
    }

    connect();

    return () => {
      es?.close();
      clearTimeout(retryTimer);
    };
  }, []);

  // Filter lines by level
  const minIdx = LEVEL_ORDER.indexOf(level);
  const filtered = lines.filter(
    (l) => LEVEL_ORDER.indexOf(l.level) >= minIdx
  );

  return (
    <div className="flex flex-col h-full">
      {/* Controls */}
      <div className="flex items-center gap-3 px-4 py-2 border-b bg-card/50 backdrop-blur-sm shrink-0">
        <div className="flex items-center gap-2">
          <span
            className={cn(
              "h-2.5 w-2.5 rounded-full",
              connected ? "bg-emerald-500" : "bg-red-500 animate-pulse"
            )}
          />
          <span className="text-xs text-muted-foreground">
            {connected ? "Connected" : "Disconnected"}
          </span>
        </div>

        <select
          value={level}
          onChange={(e) => setLevel(e.target.value)}
          className="h-8 rounded-md border bg-background px-2 text-xs"
        >
          {LEVEL_ORDER.map((l) => (
            <option key={l} value={l}>{l}</option>
          ))}
        </select>

        <Button
          variant={paused ? "default" : "outline"}
          size="sm"
          className="h-8 text-xs"
          onClick={() => setPaused(!paused)}
        >
          {paused ? "Resume" : "Pause"}
        </Button>

        <Button
          variant="outline"
          size="sm"
          className="h-8 text-xs"
          onClick={() => setAutoScroll(!autoScroll)}
        >
          Auto-scroll: {autoScroll ? "On" : "Off"}
        </Button>

        <Button
          variant="outline"
          size="sm"
          className="h-8 text-xs"
          onClick={() => setLines([])}
        >
          Clear
        </Button>

        <span className="ml-auto text-xs text-muted-foreground">
          {filtered.length} lines
        </span>
      </div>

      {/* Log output */}
      <div className="flex-1 overflow-auto bg-zinc-950 p-3 font-mono text-[13px] leading-5">
        {filtered.map((line, i) => (
          <div key={i} className={cn("whitespace-pre-wrap", LEVEL_COLORS[line.level] || "text-zinc-200")}>
            <span className="text-zinc-500">{formatTime(line.timestamp)}</span>{" "}
            <span className={cn("inline-block w-[52px]", LEVEL_COLORS[line.level])}>
              {line.level.padEnd(7)}
            </span>{" "}
            <span className="text-zinc-400">{shortLogger(line.logger).padEnd(12)}</span>{" "}
            {line.message}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
