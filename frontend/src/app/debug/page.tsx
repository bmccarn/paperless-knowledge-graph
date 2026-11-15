"use client";

import { useEffect, useRef, useState } from "react";
import { AlertCircle, CheckCircle2 } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { API_URL } from "@/lib/api";
import { cn } from "@/lib/utils";

interface LogLine {
  timestamp: string;
  level: string;
  logger: string;
  message: string;
}

const LEVEL_COLORS: Record<string, string> = {
  DEBUG: "text-fg-muted",
  INFO: "text-fg",
  WARNING: "text-partial",
  ERROR: "text-error",
};
const LEVEL_ORDER = ["DEBUG", "INFO", "WARNING", "ERROR"];

function formatTime(iso: string) {
  return new Date(iso).toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export default function DebugPage() {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [level, setLevel] = useState("INFO");
  const [paused, setPaused] = useState(false);
  const [follow, setFollow] = useState(true);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [connection, setConnection] = useState(0);
  const outputRef = useRef<HTMLDivElement>(null);
  const pausedRef = useRef(false);
  const clearRef = useRef(() => {});

  useEffect(() => {
    if (follow && !paused && outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [lines, follow, paused]);

  useEffect(() => {
    const controller = new AbortController();
    let frame: number | null = null;
    let pending: LogLine[] = [];
    let streamed: LogLine[] = [];
    let history: LogLine[] | null = null;
    let historyLoaded = false;
    let cleared = false;
    let es: EventSource;
    let retryTimer: ReturnType<typeof setTimeout>;

    function schedule() {
      if (frame !== null) return;
      frame = requestAnimationFrame(() => {
        frame = null;
        const batch = pending;
        const snapshot = history;
        pending = [];
        history = null;
        setLines(previous => {
          const next = snapshot ?? [...previous, ...batch];
          return next.length > 2000 ? next.slice(-1500) : next;
        });
      });
    }

    clearRef.current = () => {
      cleared = true;
      streamed = [];
      pending = [];
      history = null;
      if (frame !== null) cancelAnimationFrame(frame);
      frame = null;
      setLines([]);
    };

    fetch(`${API_URL}/logs?limit=200`, { signal: controller.signal })
      .then(response => {
        if (!response.ok) throw new Error(`Could not load log history (HTTP ${response.status}).`);
        return response.json();
      })
      .then(data => {
        if (controller.signal.aborted) return;
        historyLoaded = true;
        if (!cleared) {
          history = [...(data.lines || []), ...streamed];
          pending = [];
          schedule();
        }
        streamed = [];
      })
      .catch(reason => {
        if (!controller.signal.aborted) {
          historyLoaded = true;
          streamed = [];
          setError(reason instanceof Error ? reason.message : "Could not load log history.");
        }
      });

    function connect() {
      es = new EventSource(`${API_URL}/logs/stream`);
      es.onopen = () => {
        setConnected(true);
        setError(current => current === "Live connection lost. Reconnecting automatically." ? null : current);
      };
      es.onmessage = event => {
        if (pausedRef.current) return;
        try {
          const line: LogLine = JSON.parse(event.data);
          pending.push(line);
          if (!historyLoaded) streamed.push(line);
          if (history !== null) history.push(line);
          schedule();
        } catch {}
      };
      es.onerror = () => {
        setConnected(false);
        setError(current => current || "Live connection lost. Reconnecting automatically.");
        es.close();
        retryTimer = setTimeout(connect, 3000);
      };
    }
    connect();
    return () => {
      controller.abort();
      es.close();
      clearTimeout(retryTimer);
      if (frame !== null) cancelAnimationFrame(frame);
    };
  }, [connection]);

  const filtered = lines.filter(line => LEVEL_ORDER.indexOf(line.level) >= LEVEL_ORDER.indexOf(level));
  const reconnect = () => {
    setError(null);
    setConnected(false);
    setConnection(value => value + 1);
  };

  return (
    <div className="flex h-full min-h-0 flex-col gap-4 p-4 md:px-12 md:py-10">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="page-title">Logs</h1>
        <span role="status" className="inline-flex items-center gap-2 label-13 text-fg-muted">
          {connected ? <CheckCircle2 className="size-4" aria-hidden="true" /> : <AlertCircle className="size-4" aria-hidden="true" />}
          {connected ? "Connected" : "Disconnected"}
        </span>
      </header>
      {error && <Alert variant="destructive">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>Logs unavailable</AlertTitle>
        <AlertDescription><p>{error}</p><Button variant="outline" onClick={reconnect}>Reconnect</Button></AlertDescription>
      </Alert>}
      <div className="flex shrink-0 flex-wrap items-center gap-2">
        <select aria-label="Minimum log level" value={level} onChange={event => setLevel(event.target.value)} className="min-h-11 rounded-md border border-border-strong bg-surface px-3 text-base md:min-h-9 md:text-sm">
          {LEVEL_ORDER.map(value => <option key={value} value={value}>{value.charAt(0) + value.slice(1).toLowerCase()}</option>)}
        </select>
        <Button variant={follow ? "secondary" : "outline"} aria-pressed={follow} onClick={() => setFollow(value => !value)}>Follow</Button>
        <Button variant={paused ? "secondary" : "outline"} aria-pressed={paused} onClick={() => { pausedRef.current = !paused; setPaused(!paused); }}>Pause</Button>
        <Button variant="ghost" onClick={() => clearRef.current()}>Clear</Button>
        <span className="ml-auto label-12 tabular-nums text-fg-muted">{filtered.length} lines</span>
      </div>
      <div ref={outputRef} role="region" aria-label="Log output" tabIndex={0} className="min-h-0 flex-1 overflow-auto rounded-lg border bg-sunken p-3 font-mono text-xs leading-[18px] tabular-nums">
        {filtered.length ? filtered.map((line, index) => <div key={index} className={cn("whitespace-pre-wrap break-words", LEVEL_COLORS[line.level] || "text-fg")}>
          <span className="text-fg-muted">{formatTime(line.timestamp)}</span>{" "}
          <span>{line.level.padEnd(7)}</span>{" "}
          <span className="text-fg-muted">{(line.logger.split(".").pop() || line.logger).padEnd(12)}</span>{" "}
          {line.message}
        </div>) : <p className="text-fg-muted">No log lines yet.</p>}
      </div>
    </div>
  );
}
