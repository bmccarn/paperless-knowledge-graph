"use client";

import { useState, useRef, useEffect, useCallback, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import {
  postQueryStream,
  listConversations,
  createConversation,
  getConversation,
  renameConversation,
  deleteConversation, getConfig, getModels, ModelInfo} from "@/lib/api";
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
  Zap,
  Copy,
  Check,
  Pencil,
  Search,
  Gauge,
  Bot,
} from "lucide-react";

interface Source {
  document_id?: number;
  title?: string;
  doc_type?: string;
  excerpt_count?: number;
  similarity?: number;
  paperless_url?: string;
}

interface Message {
  id?: string;
  role: "user" | "assistant";
  content: string;
  sources?: Source[];
  entities?: Array<{ name?: string; label?: string }>;
  timestamp?: number;
  queryTime?: number;
  cached?: boolean;
  confidence?: number;
  follow_ups?: string[];
}

interface Conversation {
  id: string;
  title: string;
  message_count: number;
  last_message_at?: string;
  messages?: Message[];
}

function renderMarkdownContent(text: string) {
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
    if (inCodeBlock) { codeContent.push(line); continue; }
    if (line.startsWith("### ")) {
      elements.push(<h3 key={i} className="text-sm font-semibold mt-3 mb-1">{line.slice(4)}</h3>);
    } else if (line.startsWith("## ")) {
      elements.push(<h2 key={i} className="text-base font-semibold mt-3 mb-1">{line.slice(3)}</h2>);
    } else if (line.startsWith("# ")) {
      elements.push(<h1 key={i} className="text-lg font-bold mt-3 mb-1">{line.slice(2)}</h1>);
    } else if (line.startsWith("- ") || line.startsWith("* ")) {
      elements.push(
        <li key={i} className="text-sm ml-4 list-disc" dangerouslySetInnerHTML={{ __html: formatInline(line.slice(2)) }} />
      );
    } else if (/^\d+\.\s/.test(line)) {
      elements.push(
        <li key={i} className="text-sm ml-4 list-decimal" dangerouslySetInnerHTML={{ __html: formatInline(line.replace(/^\d+\.\s/, "")) }} />
      );
    } else if (line.trim() === "") {
      elements.push(<div key={i} className="h-2" />);
    } else {
      elements.push(
        <p key={i} className="text-sm leading-relaxed" dangerouslySetInnerHTML={{ __html: formatInline(line) }} />
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

function ConfidenceBar({ value }: { value: number }) {
  const pct = Math.round(value * 100);
  const color = pct >= 80 ? "bg-green-500" : pct >= 50 ? "bg-yellow-500" : "bg-red-500";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className="flex items-center gap-1.5 px-1">
          <Gauge className="h-2.5 w-2.5 text-muted-foreground" />
          <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden">
            <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${pct}%` }} />
          </div>
          <span className="text-[10px] text-muted-foreground">{pct}%</span>
        </div>
      </TooltipTrigger>
      <TooltipContent>Confidence: {pct}%</TooltipContent>
    </Tooltip>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button onClick={handleCopy} className="text-muted-foreground hover:text-foreground transition-colors p-1 rounded hover:bg-accent">
          {copied ? <Check className="h-3 w-3 text-green-500" /> : <Copy className="h-3 w-3" />}
        </button>
      </TooltipTrigger>
      <TooltipContent>{copied ? "Copied!" : "Copy answer"}</TooltipContent>
    </Tooltip>
  );
}

function QueryContent() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get("q") || "";

  const [paperlessBaseUrl, setPaperlessBaseUrl] = useState("");
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeConvId, setActiveConvId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState(initialQuery);
  const [loading, setLoading] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [showHistoryDesktop, setShowHistoryDesktop] = useState(true);
  const [streamingContent, setStreamingContent] = useState("");
  const [statusMessage, setStatusMessage] = useState("");
  const [followUpSuggestions, setFollowUpSuggestions] = useState<string[]>([]);
  const [editingTitle, setEditingTitle] = useState<string | null>(null);
  const [editTitleValue, setEditTitleValue] = useState("");
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [defaultModel, setDefaultModel] = useState<string>("");
  const [showModelDropdown, setShowModelDropdown] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const loadConversations = useCallback(async () => {
    try {
      const convs = await listConversations();
      setConversations(convs);
    } catch (e) {
      console.error("Failed to load conversations:", e);
    }
  }, []);

  useEffect(() => { getConfig().then(c => setPaperlessBaseUrl(c.paperless_url)).catch(() => {}); }, []);
  useEffect(() => {
    getModels().then(data => {
      setModels(data.models);
      setDefaultModel(data.default);
      if (!selectedModel) setSelectedModel(data.default);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    loadConversations();
  }, [loadConversations]);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, streamingContent]);

  useEffect(() => {
    if (initialQuery) {
      handleSubmit(initialQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleNewConversation = () => {
    setActiveConvId(null);
    setMessages([]);
    setInput("");
    setFollowUpSuggestions([]);
    setShowHistory(false);
    textareaRef.current?.focus();
  };

  const handleSubmit = async (question?: string) => {
    const q = question || input.trim();
    if (!q || loading) return;
    setInput("");
    setStreamingContent("");
    setStatusMessage("");
    setFollowUpSuggestions([]);

    let convId = activeConvId;
    if (!convId) {
      try {
        const conv = await createConversation();
        convId = conv.id;
        setActiveConvId(convId);
      } catch (e) {
        console.error("Failed to create conversation:", e);
      }
    }

    const userMsg: Message = { role: "user", content: q, timestamp: Date.now() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setLoading(true);

    const startTime = Date.now();
    try {
      let fullAnswer = "";
      let sources: Source[] = [];
      let entitiesFound: Array<{ name?: string; label?: string }> = [];
      let cached = false;
      let confidence: number | undefined;
      let followUps: string[] = [];

      for await (const event of postQueryStream(q, convId || undefined, selectedModel || undefined)) {
        switch (event.type) {
          case "status":
            setStatusMessage(event.message || "");
            break;
          case "answer_chunk":
            fullAnswer += event.content;
            setStreamingContent(fullAnswer);
            setStatusMessage("");
            break;
          case "complete":
            sources = event.sources || [];
            entitiesFound = event.entities_found || [];
            cached = event.cached || false;
            confidence = event.confidence;
            followUps = event.follow_up_suggestions || [];
            break;
          case "error":
            throw new Error(event.message || "Stream error");
        }
      }

      const queryTime = Date.now() - startTime;
      const assistantMsg: Message = {
        role: "assistant",
        content: fullAnswer,
        sources,
        entities: entitiesFound,
        timestamp: Date.now(),
        queryTime,
        cached,
        confidence,
        follow_ups: followUps,
      };

      const allMessages = [...newMessages, assistantMsg];
      setMessages(allMessages);
      setStreamingContent("");
      setStatusMessage("");
      setFollowUpSuggestions(followUps);
      loadConversations();
    } catch (e) {
      const errMsg: Message = {
        role: "assistant",
        content: `Sorry, something went wrong.\n\n*Error: ${(e as Error).message}*`,
        timestamp: Date.now(),
      };
      setMessages([...newMessages, errMsg]);
      setStreamingContent("");
      setStatusMessage("");
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

  const loadConversation = async (conv: Conversation) => {
    try {
      const full = await getConversation(conv.id);
      setActiveConvId(conv.id);
      setMessages(full.messages || []);
      setFollowUpSuggestions([]);
      setShowHistory(false);
      const lastAssistant = [...(full.messages || [])].reverse().find((m: Message) => m.role === "assistant");
      if (lastAssistant?.follow_ups) {
        setFollowUpSuggestions(lastAssistant.follow_ups);
      }
    } catch (e) {
      console.error("Failed to load conversation:", e);
    }
  };

  const handleDeleteConversation = async (id: string) => {
    try {
      await deleteConversation(id);
      if (activeConvId === id) {
        setActiveConvId(null);
        setMessages([]);
        setFollowUpSuggestions([]);
      }
      loadConversations();
    } catch (e) {
      console.error("Failed to delete conversation:", e);
    }
  };

  const handleRename = async (id: string) => {
    if (!editTitleValue.trim()) return;
    try {
      await renameConversation(id, editTitleValue.trim());
      setEditingTitle(null);
      loadConversations();
    } catch (e) {
      console.error("Failed to rename:", e);
    }
  };

  // Conversation list component (shared between desktop sidebar and mobile sheet)
  const ConversationList = () => (
    <>
      <div className="p-2">
        <Button variant="outline" size="sm" className="w-full gap-2 text-xs min-h-[44px] md:min-h-0" onClick={handleNewConversation}>
          <Plus className="h-3.5 w-3.5" /> New Conversation
        </Button>
      </div>
      <ScrollArea className="flex-1">
        <div className="px-2 pb-2 space-y-0.5">
          {conversations.map((conv) => (
            <div
              key={conv.id}
              className={`group flex items-center gap-0.5 rounded-md text-xs transition-colors ${
                activeConvId === conv.id ? "bg-accent text-accent-foreground" : "hover:bg-accent/50 text-muted-foreground hover:text-foreground"
              }`}
            >
              {editingTitle === conv.id ? (
                <input
                  autoFocus
                  className="flex-1 px-2 py-1.5 text-xs bg-background border rounded"
                  value={editTitleValue}
                  onChange={(e) => setEditTitleValue(e.target.value)}
                  onKeyDown={(e) => { if (e.key === "Enter") handleRename(conv.id); if (e.key === "Escape") setEditingTitle(null); }}
                  onBlur={() => handleRename(conv.id)}
                />
              ) : (
                <button className="flex-1 text-left px-2.5 py-3 md:py-2 truncate min-h-[44px] md:min-h-0" onClick={() => loadConversation(conv)}>
                  <MessageSquare className="h-3 w-3 inline mr-1.5 opacity-50" />
                  {conv.title}
                </button>
              )}
              <Button
                variant="ghost" size="icon"
                className="h-8 w-8 md:h-6 md:w-6 md:opacity-0 md:group-hover:opacity-100 shrink-0"
                onClick={(e) => { e.stopPropagation(); setEditingTitle(conv.id); setEditTitleValue(conv.title); }}
              >
                <Pencil className="h-3 w-3 md:h-2.5 md:w-2.5" />
              </Button>
              <Button
                variant="ghost" size="icon"
                className="h-8 w-8 md:h-6 md:w-6 md:opacity-0 md:group-hover:opacity-100 shrink-0 mr-0.5"
                onClick={(e) => { e.stopPropagation(); handleDeleteConversation(conv.id); }}
              >
                <Trash2 className="h-3 w-3 md:h-2.5 md:w-2.5" />
              </Button>
            </div>
          ))}
          {conversations.length === 0 && (
            <p className="text-xs text-muted-foreground/60 text-center py-6">No conversations yet</p>
          )}
        </div>
      </ScrollArea>
    </>
  );

  return (
    <div className="flex h-full">
      {/* Desktop history sidebar */}
      {showHistoryDesktop && (
        <div className="hidden md:flex w-64 border-r flex-col bg-card/30">
          <div className="flex items-center justify-between border-b px-3 py-2.5">
            <span className="text-xs font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
              <History className="h-3.5 w-3.5" /> Conversations
            </span>
          </div>
          <ConversationList />
        </div>
      )}

      {/* Mobile history sheet */}
      <Sheet open={showHistory} onOpenChange={setShowHistory}>
        <SheetContent side="left" className="w-[300px] p-0 flex flex-col">
          <SheetHeader className="border-b px-4 py-3">
            <SheetTitle className="text-sm flex items-center gap-1.5">
              <History className="h-4 w-4" /> Conversations
            </SheetTitle>
          </SheetHeader>
          <ConversationList />
        </SheetContent>
      </Sheet>

      {/* Chat area */}
      <div className="flex-1 flex flex-col min-w-0">
        <div className="border-b px-3 md:px-4 py-2.5 flex items-center justify-between bg-card/50 backdrop-blur-sm">
          <div className="flex items-center gap-2">
            {/* Mobile: show history button */}
            <Button variant="ghost" size="icon" className="h-9 w-9 md:hidden" onClick={() => setShowHistory(true)}>
              <History className="h-4 w-4" />
            </Button>
            <h1 className="text-sm md:text-base font-semibold">Knowledge Query</h1>
            <Badge variant="secondary" className="text-[9px] gap-1 py-0 hidden sm:inline-flex">
              <Zap className="h-2.5 w-2.5" /> Streaming
            </Badge>
          </div>
          {/* Desktop: toggle history */}
          <Tooltip>
            <TooltipTrigger asChild>
              <Button variant="ghost" size="icon" className="h-8 w-8 hidden md:inline-flex" onClick={() => setShowHistoryDesktop(!showHistoryDesktop)}>
                <History className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Toggle history</TooltipContent>
          </Tooltip>
        </div>

        <div className="flex-1 overflow-y-auto">
          <div className="max-w-3xl mx-auto px-3 md:px-4 py-4 md:py-6 space-y-4">
            {messages.length === 0 && !streamingContent && (
              <div className="text-center py-12 md:py-16 space-y-3">
                <div className="h-12 w-12 rounded-2xl bg-primary/10 flex items-center justify-center mx-auto">
                  <MessageSquare className="h-6 w-6 text-primary" />
                </div>
                <div>
                  <p className="text-base md:text-lg font-medium">Ask anything about your documents</p>
                  <p className="text-sm text-muted-foreground mt-1">
                    Conversations are saved automatically.
                  </p>
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={`flex gap-2 md:gap-3 ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
                {msg.role === "assistant" && (
                  <div className="h-7 w-7 rounded-lg bg-primary/10 flex items-center justify-center shrink-0 mt-1">
                    <Network className="h-3.5 w-3.5 text-primary" />
                  </div>
                )}
                <div className={`max-w-[90%] md:max-w-[85%] space-y-2 ${msg.role === "user" ? "items-end" : ""}`}>
                  <div className={`rounded-2xl px-3 md:px-4 py-2.5 md:py-3 ${
                    msg.role === "user"
                      ? "bg-primary text-primary-foreground rounded-br-md"
                      : "bg-card border rounded-bl-md"
                  }`}>
                    {msg.role === "assistant" ? (
                      <div className="space-y-1">{renderMarkdownContent(msg.content)}</div>
                    ) : (
                      <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                    )}
                  </div>

                  {msg.role === "assistant" && (msg.queryTime || msg.confidence) && (
                    <div className="flex flex-wrap items-center gap-2 px-1">
                      {msg.queryTime && (
                        <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                          <Clock className="h-2.5 w-2.5" />
                          {(msg.queryTime / 1000).toFixed(1)}s
                        </span>
                      )}
                      {msg.cached && (
                        <Badge variant="outline" className="text-[8px] px-1 py-0">cached</Badge>
                      )}
                      {msg.confidence != null && <ConfidenceBar value={msg.confidence} />}
                      <CopyButton text={msg.content} />
                    </div>
                  )}

                  {msg.entities && msg.entities.length > 0 && (
                    <div className="space-y-1.5 px-1">
                      <p className="text-[10px] font-medium text-muted-foreground flex items-center gap-1 uppercase tracking-wider">
                        <Users className="h-3 w-3" /> Entities ({msg.entities.length})
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {msg.entities.map((ent, j) => (
                          <a
                            key={j}
                            href={`/graph?q=${encodeURIComponent(ent.name || "")}`}
                            className="inline-flex items-center gap-1 rounded-md border bg-card px-2 py-1.5 text-xs hover:bg-accent transition-colors cursor-pointer min-h-[36px]"
                          >
                            <Search className="h-2.5 w-2.5 text-muted-foreground" />
                            <span className="font-medium">{ent.name}</span>
                            {ent.label && <span className="text-muted-foreground text-[10px]">· {ent.label}</span>}
                          </a>
                        ))}
                      </div>
                    </div>
                  )}

                  {msg.sources && msg.sources.length > 0 && (
                    <div className="space-y-1.5 px-1">
                      <p className="text-[10px] font-medium text-muted-foreground flex items-center gap-1 uppercase tracking-wider">
                        <FileText className="h-3 w-3" /> Sources ({msg.sources.length})
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {msg.sources.map((s, j) => {
                          const docId = s.document_id;
                          const url = s.paperless_url || (docId ? `${paperlessBaseUrl}/documents/${docId}/details` : "#");
                          const title = s.title || `Document #${docId}`;
                          const excerpts = s.excerpt_count && s.excerpt_count > 1 ? ` (${s.excerpt_count} excerpts)` : "";
                          return (
                            <a key={j} href={url} target="_blank" rel="noopener noreferrer"
                              className="inline-flex items-center gap-1.5 rounded-lg border bg-card px-2.5 py-1.5 text-xs hover:bg-accent transition-colors min-h-[36px]">
                              <ExternalLink className="h-3 w-3 shrink-0 text-muted-foreground" />
                              <span className="truncate max-w-[180px] md:max-w-[200px]">{title}{excerpts}</span>
                              {s.doc_type && <Badge variant="secondary" className="text-[9px] px-1 py-0 hidden sm:inline-flex">{s.doc_type}</Badge>}
                            </a>
                          );
                        })}
                      </div>
                    </div>
                  )}
                </div>
                {msg.role === "user" && (
                  <div className="h-7 w-7 rounded-lg bg-primary flex items-center justify-center shrink-0 mt-1">
                    <span className="text-xs font-bold text-primary-foreground">B</span>
                  </div>
                )}
              </div>
            ))}

            {(streamingContent || statusMessage) && (
              <div className="flex gap-2 md:gap-3 justify-start">
                <div className="h-7 w-7 rounded-lg bg-primary/10 flex items-center justify-center shrink-0 mt-1">
                  <Network className="h-3.5 w-3.5 text-primary" />
                </div>
                <div className="max-w-[90%] md:max-w-[85%] space-y-2">
                  {statusMessage && !streamingContent && (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground animate-pulse">
                      <Loader2 className="h-3 w-3 animate-spin" /> {statusMessage}
                    </div>
                  )}
                  {streamingContent && (
                    <div className="rounded-2xl rounded-bl-md bg-card border px-3 md:px-4 py-2.5 md:py-3">
                      <div className="space-y-1">
                        {renderMarkdownContent(streamingContent)}
                        <span className="inline-block w-2 h-4 bg-primary/60 animate-pulse ml-0.5" />
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {loading && !streamingContent && !statusMessage && (
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

            {/* Follow-up suggestions */}
            {followUpSuggestions.length > 0 && !loading && (
              <div className="space-y-2 pt-2">
                <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider px-1">
                  Suggested follow-ups
                </p>
                <div className="flex flex-wrap gap-2">
                  {followUpSuggestions.map((suggestion, i) => (
                    <button
                      key={i}
                      onClick={() => handleSubmit(suggestion)}
                      className="text-left text-xs border rounded-lg px-3 py-2.5 hover:bg-accent transition-colors max-w-full md:max-w-[300px] break-words min-h-[44px] flex items-center"
                    >
                      {suggestion}
                    </button>
                  ))}
                </div>
              </div>
            )}

            <div ref={scrollRef} />
          </div>
        </div>

        {/* Input */}
        <div className="border-t p-3 md:p-4 bg-card/50 backdrop-blur-sm">
          <div className="max-w-3xl mx-auto space-y-2">
            {/* Model selector */}
            <div className="relative">
              <button
                type="button"
                onClick={() => setShowModelDropdown(!showModelDropdown)}
                className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors px-1 min-h-[36px] md:min-h-0"
              >
                <Bot className="h-3 w-3" />
                <span className="truncate max-w-[200px]">{models.find(m => m.id === selectedModel)?.name || selectedModel || 'Select model'}</span>
                <ChevronDown className={"h-3 w-3 transition-transform " + (showModelDropdown ? "rotate-180" : "")} />
              </button>
              {showModelDropdown && (
                <div className="absolute bottom-full left-0 mb-1 bg-popover border rounded-lg shadow-lg py-1 z-50 min-w-[200px] max-w-[calc(100vw-2rem)] max-h-[240px] overflow-y-auto">
                  {models.map((m) => (
                    <button
                      key={m.id}
                      onClick={() => { setSelectedModel(m.id); setShowModelDropdown(false); }}
                      className={"w-full text-left px-3 py-2.5 md:py-1.5 text-xs hover:bg-accent transition-colors flex items-center justify-between min-h-[44px] md:min-h-0 " + (selectedModel === m.id ? "bg-accent/50 font-medium" : "")}
                    >
                      <span>{m.name}</span>
                      {m.id === defaultModel && <Badge variant="secondary" className="text-[8px] px-1 py-0 ml-2">default</Badge>}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <form onSubmit={(e) => { e.preventDefault(); handleSubmit(); }} className="flex gap-2 items-end">
              <Textarea
                ref={textareaRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask a question..."
                disabled={loading}
                className="flex-1 min-h-[44px] max-h-[160px] text-sm resize-none"
                rows={1}
              />
              <Button type="submit" disabled={loading || !input.trim()} size="icon" className="h-[44px] w-[44px] shrink-0">
                {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              </Button>
            </form>
          </div>
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
