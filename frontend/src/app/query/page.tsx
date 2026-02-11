"use client";

import { useState, useRef, useEffect, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { postQuery } from "@/lib/api";
import {
  Send,
  Loader2,
  ExternalLink,
  History,
  Trash2,
  Network,
  FileText,
  Users,
  Plus,
  Clock,
  MessageSquare,
  ChevronDown,
} from "lucide-react";

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
  queryTime?: number;
}

interface Conversation {
  id: string;
  title: string;
  messages: Message[];
  createdAt: number;
}

const CONVERSATIONS_KEY = "kg-conversations";

function loadConversations(): Conversation[] {
  if (typeof window === "undefined") return [];
  try {
    return JSON.parse(localStorage.getItem(CONVERSATIONS_KEY) || "[]");
  } catch {
    return [];
  }
}

function saveConversations(c: Conversation[]) {
  localStorage.setItem(CONVERSATIONS_KEY, JSON.stringify(c.slice(0, 50)));
}

function renderMarkdownContent(text: string) {
  // Process markdown line by line
  const lines = text.split("\n");
  const elements: React.ReactNode[] = [];
  let inCodeBlock = false;
  let codeContent: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (line.startsWith("```")) {
      if (inCodeBlock) {
        elements.push(
          <pre key={`code-${i}`} className="bg-muted rounded-md p-3 text-xs overflow-x-auto my-2 font-mono">
            {codeContent.join("\n")}
          </pre>
        );
        codeContent = [];
        inCodeBlock = false;
      } else {
        inCodeBlock = true;
      }
      continue;
    }

    if (inCodeBlock) {
      codeContent.push(line);
      continue;
    }

    // Headers
    if (line.startsWith("### ")) {
      elements.push(<h3 key={i} className="text-sm font-semibold mt-3 mb-1">{line.slice(4)}</h3>);
    } else if (line.startsWith("## ")) {
      elements.push(<h2 key={i} className="text-base font-semibold mt-3 mb-1">{line.slice(3)}</h2>);
    } else if (line.startsWith("# ")) {
      elements.push(<h1 key={i} className="text-lg font-bold mt-3 mb-1">{line.slice(2)}</h1>);
    } else if (line.startsWith("- ") || line.startsWith("* ")) {
      elements.push(
        <li key={i} className="text-sm ml-4 list-disc" dangerouslySetInnerHTML={{
          __html: formatInline(line.slice(2))
        }} />
      );
    } else if (/^\d+\.\s/.test(line)) {
      elements.push(
        <li key={i} className="text-sm ml-4 list-decimal" dangerouslySetInnerHTML={{
          __html: formatInline(line.replace(/^\d+\.\s/, ""))
        }} />
      );
    } else if (line.trim() === "") {
      elements.push(<div key={i} className="h-2" />);
    } else {
      elements.push(
        <p key={i} className="text-sm leading-relaxed" dangerouslySetInnerHTML={{
          __html: formatInline(line)
        }} />
      );
    }
  }

  return elements;
}

function formatInline(text: string): string {
  return text
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.+?)\*/g, "<em>$1</em>")
    .replace(/`(.+?)`/g, '<code class="bg-muted px-1.5 py-0.5 rounded text-xs font-mono">$1</code>');
}

function QueryContent() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get("q") || "";

  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeConvId, setActiveConvId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState(initialQuery);
  const [loading, setLoading] = useState(false);
  const [showHistory, setShowHistory] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    const convs = loadConversations();
    setConversations(convs);
    if (convs.length > 0 && !initialQuery) {
      setActiveConvId(convs[0].id);
      setMessages(convs[0].messages);
    }
  }, [initialQuery]);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Auto-submit initial query from URL
  useEffect(() => {
    if (initialQuery) {
      handleSubmit(initialQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const saveCurrentConversation = (msgs: Message[], convId?: string) => {
    const id = convId || activeConvId || crypto.randomUUID();
    const firstUserMsg = msgs.find((m) => m.role === "user");
    const title = firstUserMsg?.content.slice(0, 60) || "New conversation";

    const existing = conversations.find((c) => c.id === id);
    let updated: Conversation[];
    if (existing) {
      updated = conversations.map((c) =>
        c.id === id ? { ...c, messages: msgs, title } : c
      );
    } else {
      updated = [{ id, title, messages: msgs, createdAt: Date.now() }, ...conversations];
    }

    setConversations(updated);
    saveConversations(updated);
    setActiveConvId(id);
    return id;
  };

  const handleNewConversation = () => {
    setActiveConvId(null);
    setMessages([]);
    setInput("");
    textareaRef.current?.focus();
  };

  const handleSubmit = async (question?: string) => {
    const q = question || input.trim();
    if (!q || loading) return;
    setInput("");

    const userMsg: Message = { role: "user", content: q, timestamp: Date.now() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setLoading(true);

    const startTime = Date.now();
    try {
      const result = await postQuery(q);
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

      const explicitEntities = result.entities || result.entity_reports || [];
      const queryTime = Date.now() - startTime;

      const assistantMsg: Message = {
        role: "assistant",
        content: result.answer || result.response || JSON.stringify(result),
        sources: result.sources || result.citations || [],
        entities: [...entities, ...explicitEntities],
        graph_context: result.graph_context || result.context,
        timestamp: Date.now(),
        queryTime,
      };

      const allMessages = [...newMessages, assistantMsg];
      setMessages(allMessages);
      saveCurrentConversation(allMessages);
    } catch (e) {
      const errMsg: Message = {
        role: "assistant",
        content: `Sorry, something went wrong. Please try again.\n\n*Error: ${(e as Error).message}*`,
        timestamp: Date.now(),
      };
      const allMessages = [...newMessages, errMsg];
      setMessages(allMessages);
      saveCurrentConversation(allMessages);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const loadConversation = (conv: Conversation) => {
    setActiveConvId(conv.id);
    setMessages(conv.messages);
  };

  const deleteConversation = (id: string) => {
    const updated = conversations.filter((c) => c.id !== id);
    setConversations(updated);
    saveConversations(updated);
    if (activeConvId === id) {
      setActiveConvId(null);
      setMessages([]);
    }
  };

  const paperlessUrl = (id: number) => `http://your-paperless-host:8000/documents/${id}/`;

  return (
    <div className="flex h-full">
      {/* History sidebar */}
      {showHistory && (
        <div className="w-64 border-r flex flex-col bg-card/30">
          <div className="flex items-center justify-between border-b px-3 py-2.5">
            <span className="text-xs font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
              <History className="h-3.5 w-3.5" /> Conversations
            </span>
          </div>
          <div className="p-2">
            <Button
              variant="outline"
              size="sm"
              className="w-full gap-2 text-xs"
              onClick={handleNewConversation}
            >
              <Plus className="h-3.5 w-3.5" /> New Conversation
            </Button>
          </div>
          <ScrollArea className="flex-1">
            <div className="px-2 pb-2 space-y-0.5">
              {conversations.map((conv) => (
                <div
                  key={conv.id}
                  className={`group flex items-center gap-1 rounded-md text-xs transition-colors ${
                    activeConvId === conv.id
                      ? "bg-accent text-accent-foreground"
                      : "hover:bg-accent/50 text-muted-foreground hover:text-foreground"
                  }`}
                >
                  <button
                    className="flex-1 text-left px-2.5 py-2 truncate"
                    onClick={() => loadConversation(conv)}
                  >
                    <MessageSquare className="h-3 w-3 inline mr-1.5 opacity-50" />
                    {conv.title}
                  </button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 opacity-0 group-hover:opacity-100 shrink-0 mr-1"
                    onClick={(e) => { e.stopPropagation(); deleteConversation(conv.id); }}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </div>
              ))}
              {conversations.length === 0 && (
                <p className="text-xs text-muted-foreground/60 text-center py-6">
                  No conversations yet
                </p>
              )}
            </div>
          </ScrollArea>
        </div>
      )}

      {/* Chat area */}
      <div className="flex-1 flex flex-col min-w-0">
        <div className="border-b px-4 py-2.5 flex items-center justify-between bg-card/50 backdrop-blur-sm">
          <h1 className="text-base font-semibold">Knowledge Query</h1>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                onClick={() => setShowHistory(!showHistory)}
              >
                <History className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Toggle history</TooltipContent>
          </Tooltip>
        </div>

        <ScrollArea className="flex-1">
          <div className="max-w-3xl mx-auto px-4 py-6 space-y-4">
            {messages.length === 0 && (
              <div className="text-center py-16 space-y-3">
                <div className="h-12 w-12 rounded-2xl bg-primary/10 flex items-center justify-center mx-auto">
                  <MessageSquare className="h-6 w-6 text-primary" />
                </div>
                <div>
                  <p className="text-lg font-medium">Ask anything about your documents</p>
                  <p className="text-sm text-muted-foreground mt-1">
                    The knowledge graph will find relevant entities, relationships, and sources.
                  </p>
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div
                key={i}
                className={`flex gap-3 ${msg.role === "user" ? "justify-end" : "justify-start"}`}
              >
                {msg.role === "assistant" && (
                  <div className="h-7 w-7 rounded-lg bg-primary/10 flex items-center justify-center shrink-0 mt-1">
                    <Network className="h-3.5 w-3.5 text-primary" />
                  </div>
                )}
                <div className={`max-w-[85%] space-y-2 ${msg.role === "user" ? "items-end" : ""}`}>
                  <div
                    className={`rounded-2xl px-4 py-3 ${
                      msg.role === "user"
                        ? "bg-primary text-primary-foreground rounded-br-md"
                        : "bg-card border rounded-bl-md"
                    }`}
                  >
                    {msg.role === "assistant" ? (
                      <div className="space-y-1">{renderMarkdownContent(msg.content)}</div>
                    ) : (
                      <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                    )}
                  </div>

                  {/* Query time */}
                  {msg.queryTime && (
                    <p className="text-[10px] text-muted-foreground flex items-center gap-1 px-1">
                      <Clock className="h-2.5 w-2.5" />
                      {(msg.queryTime / 1000).toFixed(1)}s
                    </p>
                  )}

                  {/* Entities */}
                  {msg.entities && msg.entities.length > 0 && (
                    <div className="space-y-1.5 px-1">
                      <p className="text-[10px] font-medium text-muted-foreground flex items-center gap-1 uppercase tracking-wider">
                        <Users className="h-3 w-3" />
                        Entities ({msg.entities.length})
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {msg.entities.map((ent, j) => (
                          <Badge
                            key={j}
                            variant="secondary"
                            className="text-xs gap-1 py-1 font-normal"
                          >
                            <span className="font-medium">{ent.name}</span>
                            <span className="text-muted-foreground">· {ent.label}</span>
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Sources */}
                  {msg.sources && msg.sources.length > 0 && (
                    <div className="space-y-1.5 px-1">
                      <p className="text-[10px] font-medium text-muted-foreground flex items-center gap-1 uppercase tracking-wider">
                        <FileText className="h-3 w-3" />
                        Sources ({msg.sources.length})
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {msg.sources.map((s, j) => {
                          const docId = s.paperless_id || s.doc_id;
                          return (
                            <a
                              key={j}
                              href={docId ? paperlessUrl(docId) : "#"}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center gap-1.5 rounded-lg border bg-card px-2.5 py-1.5 text-xs hover:bg-accent transition-colors"
                            >
                              <ExternalLink className="h-3 w-3 shrink-0 text-muted-foreground" />
                              <span className="truncate max-w-[200px]">
                                {s.title || `Doc #${docId}`}
                              </span>
                              {s.doc_type && (
                                <Badge variant="secondary" className="text-[9px] px-1 py-0">
                                  {s.doc_type}
                                </Badge>
                              )}
                            </a>
                          );
                        })}
                      </div>
                    </div>
                  )}

                  {/* Graph context collapsible */}
                  {msg.graph_context && (
                    <details className="px-1">
                      <summary className="text-[10px] text-muted-foreground hover:text-foreground cursor-pointer flex items-center gap-1 uppercase tracking-wider font-medium">
                        <ChevronDown className="h-3 w-3" />
                        Graph traversal details
                      </summary>
                      <pre className="mt-2 overflow-auto rounded-lg bg-muted/50 p-3 text-[10px] max-h-48 font-mono">
                        {JSON.stringify(msg.graph_context, null, 2)}
                      </pre>
                    </details>
                  )}
                </div>
                {msg.role === "user" && (
                  <div className="h-7 w-7 rounded-lg bg-primary flex items-center justify-center shrink-0 mt-1">
                    <span className="text-xs font-bold text-primary-foreground">U</span>
                  </div>
                )}
              </div>
            ))}

            {/* Typing indicator */}
            {loading && (
              <div className="flex gap-3">
                <div className="h-7 w-7 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
                  <Network className="h-3.5 w-3.5 text-primary" />
                </div>
                <div className="rounded-2xl rounded-bl-md bg-card border px-4 py-3">
                  <div className="flex gap-1.5">
                    <span className="typing-dot h-2 w-2 rounded-full bg-muted-foreground/50" />
                    <span className="typing-dot h-2 w-2 rounded-full bg-muted-foreground/50" />
                    <span className="typing-dot h-2 w-2 rounded-full bg-muted-foreground/50" />
                  </div>
                </div>
              </div>
            )}
            <div ref={scrollRef} />
          </div>
        </ScrollArea>

        {/* Input */}
        <div className="border-t p-4 bg-card/50 backdrop-blur-sm">
          <form
            onSubmit={(e) => { e.preventDefault(); handleSubmit(); }}
            className="flex gap-2 max-w-3xl mx-auto items-end"
          >
            <Textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask a question... (Enter to send, Shift+Enter for newline)"
              disabled={loading}
              className="flex-1 min-h-[42px] max-h-[160px] text-sm"
              rows={1}
            />
            <Button
              type="submit"
              disabled={loading || !input.trim()}
              size="icon"
              className="h-[42px] w-[42px] shrink-0"
            >
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            </Button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default function QueryPage() {
  return (
    <Suspense fallback={
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary/40" />
      </div>
    }>
      <QueryContent />
    </Suspense>
  );
}
