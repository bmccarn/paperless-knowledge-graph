"use client";

import { useState, useRef, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { postQuery } from "@/lib/api";
import { Send, Loader2, ExternalLink, History, Trash2, Network, FileText, Users } from "lucide-react";

interface EntityReport {
  name?: string;
  label?: string;
  description?: string;
  uuid?: string;
}

interface Message {
  role: "user" | "assistant";
  content: string;
  sources?: Array<{
    paperless_id?: number;
    doc_id?: number;
    title?: string;
    doc_type?: string;
    date?: string;
  }>;
  entities?: EntityReport[];
  graph_context?: {
    nodes?: Array<{ labels: string[]; props: Record<string, unknown> }>;
    relationships?: Array<{ type: string; start: string; end: string }>;
  };
  timestamp: number;
}

interface HistoryEntry {
  question: string;
  timestamp: number;
}

const HISTORY_KEY = "kg-query-history";

function loadHistory(): HistoryEntry[] {
  if (typeof window === "undefined") return [];
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
  } catch {
    return [];
  }
}

function saveHistory(h: HistoryEntry[]) {
  localStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, 50)));
}

export default function QueryPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [showHistory, setShowHistory] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setHistory(loadHistory());
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSubmit = async (question?: string) => {
    const q = question || input.trim();
    if (!q || loading) return;
    setInput("");

    const userMsg: Message = { role: "user", content: q, timestamp: Date.now() };
    setMessages((prev) => [...prev, userMsg]);
    setLoading(true);

    try {
      const result = await postQuery(q);
      // Extract entity descriptions from graph context nodes
      const contextNodes = result.graph_context?.nodes || result.context?.nodes || [];
      const entities: EntityReport[] = contextNodes
        .filter((n: Record<string, unknown>) => {
          const p = (n.props || n.properties || {}) as Record<string, unknown>;
          return p.description || p.name;
        })
        .map((n: Record<string, unknown>) => {
          const p = (n.props || n.properties || {}) as Record<string, unknown>;
          const labels = n.labels as string[] | undefined;
          return {
            name: (p.name as string) || (p.title as string) || "Unknown",
            label: labels?.[0] || "Entity",
            description: p.description as string | undefined,
            uuid: p.uuid as string | undefined,
          };
        });

      // Also include any explicitly provided entity reports
      const explicitEntities = result.entities || result.entity_reports || [];

      const assistantMsg: Message = {
        role: "assistant",
        content: result.answer || result.response || JSON.stringify(result),
        sources: result.sources || result.citations || [],
        entities: [...entities, ...explicitEntities],
        graph_context: result.graph_context || result.context,
        timestamp: Date.now(),
      };
      setMessages((prev) => [...prev, assistantMsg]);

      const newHistory = [{ question: q, timestamp: Date.now() }, ...history.filter((h) => h.question !== q)];
      setHistory(newHistory);
      saveHistory(newHistory);
    } catch (e) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Error: ${(e as Error).message}`,
          timestamp: Date.now(),
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const paperlessUrl = (id: number) => `http://10.10.10.20:8000/documents/${id}/`;

  return (
    <div className="flex h-full">
      {/* History sidebar */}
      {showHistory && (
        <div className="w-64 border-r flex flex-col">
          <div className="flex items-center justify-between border-b px-3 py-3">
            <span className="text-sm font-medium flex items-center gap-1">
              <History className="h-4 w-4" /> History
            </span>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                setHistory([]);
                saveHistory([]);
              }}
            >
              <Trash2 className="h-3 w-3" />
            </Button>
          </div>
          <ScrollArea className="flex-1">
            <div className="p-2 space-y-1">
              {history.map((h, i) => (
                <button
                  key={i}
                  className="w-full text-left rounded-md px-2 py-1.5 text-xs hover:bg-accent truncate"
                  onClick={() => handleSubmit(h.question)}
                >
                  {h.question}
                </button>
              ))}
              {history.length === 0 && (
                <p className="text-xs text-muted-foreground p-2">No history yet</p>
              )}
            </div>
          </ScrollArea>
        </div>
      )}

      {/* Chat area */}
      <div className="flex-1 flex flex-col">
        <div className="border-b px-4 py-3 flex items-center justify-between">
          <h1 className="text-lg font-semibold">Knowledge Query</h1>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setShowHistory(!showHistory)}
          >
            <History className="h-4 w-4" />
          </Button>
        </div>

        <ScrollArea className="flex-1 p-4">
          <div className="max-w-3xl mx-auto space-y-4">
            {messages.length === 0 && (
              <div className="text-center py-20 text-muted-foreground">
                <p className="text-lg">Ask a question about your documents</p>
                <p className="text-sm mt-1">
                  The knowledge graph will find relevant information and cite sources.
                </p>
              </div>
            )}
            {messages.map((msg, i) => (
              <div
                key={i}
                className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
              >
                <Card
                  className={`max-w-[85%] ${
                    msg.role === "user" ? "bg-primary text-primary-foreground" : ""
                  }`}
                >
                  <CardContent className="py-3 px-4">
                    <p className="text-sm whitespace-pre-wrap">{msg.content}</p>

                    {/* Entity descriptions used */}
                    {msg.entities && msg.entities.length > 0 && (
                      <>
                        <Separator className="my-3" />
                        <div className="space-y-2">
                          <p className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                            <Users className="h-3 w-3" />
                            Entities Referenced ({msg.entities.length})
                          </p>
                          {msg.entities.map((ent, j) => (
                            <div
                              key={j}
                              className="rounded-md bg-accent/30 p-2.5 text-xs"
                            >
                              <div className="flex items-center gap-2 mb-1">
                                <Badge
                                  variant="secondary"
                                  className="text-[9px] px-1.5 py-0"
                                >
                                  {ent.label}
                                </Badge>
                                <span className="font-medium">{ent.name}</span>
                              </div>
                              {ent.description && (
                                <p className="text-muted-foreground leading-relaxed mt-1">
                                  {ent.description}
                                </p>
                              )}
                            </div>
                          ))}
                        </div>
                      </>
                    )}

                    {/* Sources */}
                    {msg.sources && msg.sources.length > 0 && (
                      <>
                        <Separator className="my-3" />
                        <div className="space-y-1.5">
                          <p className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                            <FileText className="h-3 w-3" />
                            Source Documents ({msg.sources.length})
                          </p>
                          {msg.sources.map((s, j) => {
                            const docId = s.paperless_id || s.doc_id;
                            return (
                              <a
                                key={j}
                                href={docId ? paperlessUrl(docId) : "#"}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex items-center gap-2 rounded-md bg-accent/50 px-2 py-1.5 text-xs hover:bg-accent transition-colors"
                              >
                                <ExternalLink className="h-3 w-3 shrink-0" />
                                <span className="truncate">
                                  {s.title || `Document #${docId}`}
                                </span>
                                {s.doc_type && (
                                  <Badge variant="secondary" className="text-[10px] ml-auto shrink-0">
                                    {s.doc_type}
                                  </Badge>
                                )}
                                {s.date && (
                                  <span className="text-muted-foreground shrink-0">
                                    {s.date}
                                  </span>
                                )}
                              </a>
                            );
                          })}
                        </div>
                      </>
                    )}

                    {/* Graph context (raw) */}
                    {msg.graph_context && (
                      <>
                        <Separator className="my-3" />
                        <details className="text-xs">
                          <summary className="cursor-pointer text-muted-foreground hover:text-foreground flex items-center gap-1">
                            <Network className="h-3 w-3" />
                            Graph traversal details
                          </summary>
                          <pre className="mt-2 overflow-auto rounded bg-muted p-2 text-[10px] max-h-48">
                            {JSON.stringify(msg.graph_context, null, 2)}
                          </pre>
                        </details>
                      </>
                    )}
                  </CardContent>
                </Card>
              </div>
            ))}
            {loading && (
              <div className="flex justify-start">
                <Card>
                  <CardContent className="py-3 px-4">
                    <Loader2 className="h-4 w-4 animate-spin" />
                  </CardContent>
                </Card>
              </div>
            )}
            <div ref={scrollRef} />
          </div>
        </ScrollArea>

        <div className="border-t p-4">
          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleSubmit();
            }}
            className="flex gap-2 max-w-3xl mx-auto"
          >
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Ask a question about your documents..."
              disabled={loading}
              className="flex-1"
            />
            <Button type="submit" disabled={loading || !input.trim()}>
              <Send className="h-4 w-4" />
            </Button>
          </form>
        </div>
      </div>
    </div>
  );
}
