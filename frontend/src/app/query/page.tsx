"use client";

import { TimelineDates, TimelineEvent, TimelineReceipt } from "@/components/timeline-dates";
import { QuestionCoverage, QuestionCoverageReceipt } from "@/components/question-coverage";
import { ClaimProse } from "@/components/claim-prose";
import { ClaimTable, type OmittedUnit, type TableClaim } from "@/components/claim-table";
import { SourceLinks, SourceList } from "@/components/source-list";
import { TypeDot } from "@/components/kind-shape";
import { formatSourceDate } from "@/lib/answer-state";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { answerView } from "@/lib/answer-state";
import { StreamStatus, VerdictLine } from "@/components/answer-status";
import { Alert, AlertDescription } from "@/components/ui/alert";

import { useState, useRef, useEffect, useEffectEvent, useCallback, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { Button } from "@/components/ui/button";
import { ConversationList } from "@/components/conversation-list";
import { QueryModes, QueryModel, type QueryMode } from "@/components/query-controls";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from "@/components/ui/sheet";
import {
  postQueryStream,
  listConversations,
  createConversation,
  getConversation,
  renameConversation,
  deleteConversation, getModels, generateTitle, usePaperlessUrl, ModelInfo} from "@/lib/api";
import {
  Send,
  Loader2,
  History,
  Copy,
  Check,
  ChevronRight,
} from "lucide-react";

interface Source {
  document_id?: number;
  title?: string;
  doc_type?: string;
  excerpt_count?: number;
  similarity?: number;
  paperless_url?: string;
  chunk_index?: number;
  excerpt?: string;
  date?: string;
  date_signals?: Record<string, string[]>;
  source_quality?: { score?: number; tier?: string; reasons?: string[] };
}

interface SourceSummary {
  latest_source_date?: string | null;
  latest_check_used?: boolean;
  source_count?: number;
  newer_docs_may_exist?: boolean;
  trust_score?: number;
  trust_level?: string;
  trust_reasons?: string[];
  trust_penalties?: string[];
  trust_dimensions?: Record<string, number>;
  audit_status?: string;
  claim_summary?: Record<string, number>;
  evidence_coverage?: Record<string, unknown>;
  verification_status?: string;
  unsupported_claim_count?: number;
  stale_or_conflicting_claim_count?: number;
  timeline_event_count?: number;
}

interface QueryPlan {
  mode?: string;
  intent?: string;
  domain?: string;
  requires_current?: boolean;
  needs_timeline?: boolean;
  strategy?: string;
  planner?: string;
  subqueries?: Array<{ role?: string; query?: string }>;
}

interface TraceStep {
  step: string;
  status?: string;
  detail?: string;
  data?: Record<string, unknown>;
}

interface Verification {
  status?: string;
  partial?: { omitted_count: number; original_total: number; original_supported: number; omitted_units?: OmittedUnit[] };
  supported_claims?: string[];
  unsupported_claims?: string[];
  stale_or_conflicting_claims?: string[];
  missing_evidence?: string[];
  notes?: string[];
}

interface ClaimLedger {
  candidate_text?: string;
  summary?: Record<string, number>;
  claims?: Array<{
    claim?: string;
    support_status?: string;
    status?: string;
    document_id?: number;
    source_title?: string;
    evidence_id?: string;
    evidence_excerpt?: string;
    evidence_quote?: string;
    references?: Array<{ document_id: number; quote: string; source_title?: string }>;
    date?: string;
    source_quality?: string;
    notes?: string;
  }>;
}

interface EvidencePack {
  coverage?: Record<string, unknown>;
  source_documents?: Array<{
    document_id?: number;
    title?: string;
    doc_type?: string;
    chunks?: number;
    best_quality_score?: number;
    best_quality_tier?: string;
    structured_fact_count?: number;
    date_signals?: Record<string, string[]>;
  }>;
  items?: Array<{
    id?: string;
    document_id?: number;
    chunk_index?: number;
    title?: string;
    doc_type?: string;
    source_quality?: { score?: number; tier?: string; reasons?: string[] };
    date_signals?: Record<string, string[]>;
    structured_fact_count?: number;
    exact_term_hits?: number;
    excerpt?: string;
  }>;
}

interface Message {
  id?: string;
  role: "user" | "assistant";
  content: string;
  sources?: Source[];
  timestamp?: number;
  queryTime?: number;
  query_time_ms?: number;
  /** No complete event arrived: a stream error, an HTTP error or a dropped stream. */
  failed?: boolean;
  cached?: boolean;
  confidence?: number;
  evidence?: { score?: number; level?: string };
  source_dates?: Record<string, string>;
  follow_ups?: string[];
  source_summary?: SourceSummary;
  query_plan?: QueryPlan;
  trace?: TraceStep[];
  verification?: Verification;
  claim_ledger?: ClaimLedger;
  evidence_pack?: EvidencePack;
  timeline_events?: TimelineEvent[];
  finalization?: { disposition?: string; timeline?: TimelineReceipt; pipeline_version?: string; answer_verified?: boolean;
    question_coverage?: QuestionCoverageReceipt };
}

interface Conversation {
  id: string;
  title: string;
  message_count: number;
  last_message_at?: string;
  messages?: Message[];
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
        <button aria-label={copied ? "Answer copied" : "Copy answer"} onClick={handleCopy} className="text-muted-foreground hover:text-foreground transition-colors p-1 rounded hover:bg-accent">
          {copied ? <Check className="h-3 w-3 text-verified" /> : <Copy className="h-3 w-3" />}
        </button>
      </TooltipTrigger>
      <TooltipContent>{copied ? "Copied!" : "Copy answer"}</TooltipContent>
    </Tooltip>
  );
}

/**
 * The query plan, the trace and the evidence summary, closed by default.
 * Claim text from the verifier only shows with answer text, so a withheld draft never reaches the page.
 */
function HowChecked({ msg, showClaims }: { msg: Message; showClaims: boolean }) {
  const plan = msg.query_plan, summary = msg.source_summary, verification = msg.verification;
  const planLine = [plan?.intent && `Intent ${plan.intent}`, plan?.domain && `Domain ${plan.domain}`, plan?.planner && `${plan.planner} planner`]
    .filter(Boolean).join(" · ");
  const flagged = (summary?.unsupported_claim_count || 0) + (summary?.stale_or_conflicting_claim_count || 0);
  const notes = [
    ...(summary?.newer_docs_may_exist ? ["Retrieval does not establish completeness of the archive; newer or conflicting records may exist."] : []),
    ...(flagged ? [`The verifier flagged ${summary?.unsupported_claim_count || 0} unsupported and ${summary?.stale_or_conflicting_claim_count || 0} stale or conflicting claims.`] : []),
    ...(summary?.trust_penalties ?? []),
    ...(summary?.trust_reasons ?? []),
    ...(showClaims ? [
      ...(verification?.unsupported_claims ?? []).map(claim => `Unsupported: ${claim}`),
      ...(verification?.stale_or_conflicting_claims ?? []).map(claim => `Stale or conflicting: ${claim}`),
      ...(verification?.missing_evidence ?? []).map(gap => `Missing evidence: ${gap}`),
    ] : []),
  ];
  const trace = msg.trace ?? [];
  if (!planLine && !plan?.strategy && !plan?.subqueries?.length && !trace.length && !notes.length) return null;
  const heading = "mb-1 label-12 font-medium text-fg";
  return (
    <Collapsible>
      <CollapsibleTrigger className="group inline-flex min-h-11 items-center gap-1.5 label-14 text-fg md:min-h-8">
        <ChevronRight aria-hidden className="size-4 text-fg-muted transition-transform duration-(--dur-2) group-aria-expanded:rotate-90" strokeWidth={1.75} />
        How this was checked
      </CollapsibleTrigger>
      <CollapsibleContent className="space-y-4 pt-2 pl-[22px] label-13 text-fg-muted">
        {(planLine || plan?.strategy || plan?.subqueries?.length) && (
          <div>
            <h3 className={heading}>Query plan</h3>
            {planLine && <p>{planLine}</p>}
            {plan?.strategy && <p>{plan.strategy}</p>}
            {plan?.subqueries?.length ? (
              <ul className="mt-1 space-y-0.5">
                {plan.subqueries.map((q, idx) => <li key={idx}><span className="text-fg-subtle">{q.role || "query"}</span> {q.query}</li>)}
              </ul>
            ) : null}
          </div>
        )}
        {trace.length > 0 && (
          <div>
            <h3 className={heading}>Trace</h3>
            <ol className="space-y-0.5">
              {trace.map((step, idx) => <li key={idx}><span className="text-fg">{step.step}</span>: {step.detail || step.status}</li>)}
            </ol>
          </div>
        )}
        {notes.length > 0 && (
          <div>
            <h3 className={heading}>Evidence summary</h3>
            <ul className="space-y-0.5">{notes.map((note, idx) => <li key={idx}>{note}</li>)}</ul>
          </div>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
}

function QueryContent() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get("q") || "";
  const selectedConversation = searchParams.get("conversation") || "";
  const initialDraft = searchParams.get("draft") || "";

  const paperlessBaseUrl = usePaperlessUrl();
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeConvId, setActiveConvId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState(initialQuery);
  const [loading, setLoading] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [showHistoryDesktop, setShowHistoryDesktop] = useState(true);
  const [statusMessage, setStatusMessage] = useState("");
  const [elapsed, setElapsed] = useState(0);
  const [followUpSuggestions, setFollowUpSuggestions] = useState<string[]>([]);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [defaultModel, setDefaultModel] = useState<string>("");
  const [selectedSource, setSelectedSource] = useState<Source | null>(null);
  // The selected claim of each answer, by message index; shared by its prose, margin notes, claim table and sources.
  const [activeClaims, setActiveClaims] = useState<Record<number, number>>({});
  const [queryMode, setQueryMode] = useState<QueryMode>("strict");
  const composerRef = useRef<HTMLDivElement>(null);
  const messagesRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const viewVersion = useRef(0);
  const activeConversation = useRef<string | null>(null);
  const busy = useRef(false);
  const clearStream = () => {
    setStatusMessage("");
    setElapsed(0);
  };
  const invalidateView = () => {
    viewVersion.current++;
    busy.current = false;
    setLoading(false);
    clearStream();
    setFollowUpSuggestions([]);
    setSelectedSource(null);
    setActiveClaims({});
  };
  useEffect(() => () => { viewVersion.current++; busy.current = false; }, []);
  useEffect(() => {
    const composer = composerRef.current;
    const messages = messagesRef.current;
    if (!composer || !messages) return;
    const observer = new ResizeObserver(() => {
      messages.style.setProperty("--composer-height", `${composer.getBoundingClientRect().height}px`);
    });
    observer.observe(composer);
    return () => observer.disconnect();
  }, []);


  const loadConversations = useCallback(() => {
    return listConversations().then(setConversations).catch((e) => {
      console.error("Failed to load conversations:", e);
    });
  }, []);

  useEffect(() => {
    getModels().then(data => {
      setModels(data.models);
      setDefaultModel(data.default);
      setSelectedModel((current) => current || data.default);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    loadConversations();
  }, [loadConversations]);

  useEffect(() => {
    // Keep the new verdict visible instead of jumping to the end of a long answer.
    const latest = messagesRef.current?.querySelectorAll("article[aria-label=Answer], h2.question");
    latest?.[latest.length - 1]?.scrollIntoView({ block: "start", behavior: "instant" });
  }, [messages, loading]);

  // Whole seconds since the question or conversation load started, for the status line.
  useEffect(() => {
    if (!loading) return;
    const start = Date.now();
    const timer = setInterval(() => setElapsed(Math.floor((Date.now() - start) / 1000)), 1000);
    return () => clearInterval(timer);
  }, [loading]);

  const handleNewConversation = () => {
    invalidateView();
    activeConversation.current = null;
    setActiveConvId(null);
    setMessages([]);
    setInput("");
    setFollowUpSuggestions([]);
    setShowHistory(false);
    textareaRef.current?.focus();
  };

  const handleSubmit = async (question?: string, mode: QueryMode = queryMode) => {
    const q = question || input.trim();
    if (!q || busy.current) return;
    const version = ++viewVersion.current;
    const isCurrent = () => version === viewVersion.current;
    busy.current = true;
    setLoading(true);
    setInput("");
    clearStream();
    setFollowUpSuggestions([]);

    let convId = activeConversation.current;
    if (!convId) {
      try {
        const conv = await createConversation();
        if (!isCurrent()) return;
        convId = conv.id;
        activeConversation.current = convId;
        setActiveConvId(convId);
      } catch (e) {
        console.error("Failed to create conversation:", e);
      }
    }
    if (!isCurrent()) return;

    const userMsg: Message = { role: "user", content: q, timestamp: Date.now() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setLoading(true);

    const startTime = Date.now();
    let completed = false;
    // Accumulates the final answer and its trust artifacts; nothing shows until the complete event.
    const acc: Message = {
      role: "assistant", content: "", sources: [], cached: false,
      follow_ups: [], trace: [], timeline_events: [],
    };
    const snapshot = (): Message => ({ ...acc, timestamp: Date.now(), queryTime: Date.now() - startTime });

    try {
      for await (const event of postQueryStream(q, convId || undefined, selectedModel || undefined, mode)) {
        if (!isCurrent()) return;
        switch (event.type) {
          case "status":
            setStatusMessage(event.message || "");
            break;
          case "complete":
            completed = true;
            if (event.answer) acc.content = event.answer;
            acc.sources = event.sources || [];
            acc.cached = event.cached || false;
            acc.confidence = event.confidence;
            acc.follow_ups = event.follow_up_suggestions || [];
            acc.source_summary = event.source_summary || undefined;
            acc.query_plan = event.query_plan || undefined;
            acc.trace = event.trace || acc.trace;
            acc.verification = event.verification || undefined;
            acc.claim_ledger = event.claim_ledger || undefined;
            acc.evidence_pack = event.evidence_pack || undefined;
            acc.timeline_events = event.timeline_events || [];
            acc.finalization = event.finalization;
            acc.evidence = event.evidence;
            acc.source_dates = event.source_dates;
            break;
          case "error":
            throw new Error(event.message || "Stream error");
        }
      }

      if (!isCurrent()) return;
      if (!completed) throw new Error("Stream ended before final verification");
      const followUps = acc.follow_ups || [];
      const allMessages = [...newMessages, snapshot()];
      setMessages(allMessages);
      clearStream();
      setFollowUpSuggestions(followUps);
      loadConversations();

      // Generate AI title after first assistant response
      const userMsgCount = allMessages.filter((m: Message) => m.role === "user").length;
      const assistantMsgCount = allMessages.filter((m: Message) => m.role === "assistant").length;
      if (userMsgCount === 1 && assistantMsgCount === 1 && convId) {
        const firstUserMsg = allMessages.find((m: Message) => m.role === "user")?.content;
        if (firstUserMsg) {
          generateTitle(firstUserMsg).then(async (title) => {
            try {
              await renameConversation(convId, title);
              // Update sidebar immediately
              setConversations(prev => prev.map(c => c.id === convId ? { ...c, title } : c));
            } catch (e) {
              console.error("Failed to set AI title:", e);
            }
          }).catch(() => {});
        }
      }
    } catch {
      if (!isCurrent()) return;
      // Stream broke - try recovering from conversation history
      if (convId) {
        try {
          const full = await getConversation(convId);
          if (!isCurrent()) return;
          if (full.messages && full.messages.length > newMessages.length) {
            setMessages(full.messages);
            const lastA = full.messages.findLast((m: Message) => m.role === "assistant");
            if (lastA?.follow_ups) setFollowUpSuggestions(lastA.follow_ups);
            clearStream();
            loadConversations();
            setLoading(false);
            return;
          }
        } catch { /* recovery failed */ }
      }
      if (!isCurrent()) return;
      setMessages([...newMessages, { role: "assistant", content: "", failed: true, timestamp: Date.now(), queryTime: Date.now() - startTime }]);
      clearStream();
    } finally {
      if (isCurrent()) { busy.current = false; setLoading(false); }
    }
  };

  const submitInitialQuery = useEffectEvent((question: string) => handleSubmit(question));

  useEffect(() => {
    // Let Strict Mode cancel its rehearsal before consuming the question.
    const timer = setTimeout(() => {
      const url = new URL(window.location.href);
      const draft = url.searchParams.get("draft");
      if (draft) {
        url.searchParams.delete("draft");
        window.history.replaceState(null, "", url);
        setInput(draft);
        textareaRef.current?.focus();
        return;
      }
      const question = url.searchParams.get("q");
      if (!question) return;
      url.searchParams.delete("q");
      // Consume the question before any request so a reload cannot submit it again.
      window.history.replaceState(null, "", url);
      submitInitialQuery(question);
    }, 0);
    return () => clearTimeout(timer);
  }, [initialQuery, initialDraft]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const loadConversation = async (conv: Pick<Conversation, "id" | "title">) => {
    invalidateView();
    const version = viewVersion.current;
    activeConversation.current = conv.id;
    setActiveConvId(conv.id);
    setMessages([]);
    busy.current = true;
    setLoading(true);
    setStatusMessage("Loading conversation...");
    try {
      const full = await getConversation(conv.id);
      if (version !== viewVersion.current) return;
      setMessages(full.messages || []);
      setFollowUpSuggestions([]);
      setShowHistory(false);
      const lastAssistant = (full.messages || []).findLast((m: Message) => m.role === "assistant");
      if (lastAssistant?.follow_ups) {
        setFollowUpSuggestions(lastAssistant.follow_ups);
      }
    } catch (e) {
      if (version !== viewVersion.current) return;
      console.error("Failed to load conversation:", e);
    } finally {
      if (version === viewVersion.current) {
        busy.current = false;
        setLoading(false);
        setStatusMessage("");
      }
    }
  };

  const loadSelectedConversation = useEffectEvent((id: string) => loadConversation({ id, title: "" }));
  useEffect(() => {
    if (!selectedConversation) return;
    const timer = setTimeout(() => {
      const url = new URL(window.location.href);
      url.searchParams.delete("conversation");
      window.history.replaceState(null, "", url);
      loadSelectedConversation(selectedConversation);
    }, 0);
    return () => clearTimeout(timer);
  }, [selectedConversation]);

  const handleDeleteConversation = async (id: string) => {
    await deleteConversation(id);
    if (activeConversation.current === id) handleNewConversation();
    await loadConversations();
  };

  const handleRename = async (id: string, title: string) => {
    await renameConversation(id, title);
    await loadConversations();
  };

  // The sheet shows a source's date only from the answer's source_dates, like the source list.
  const openSource = (source: Source, dates?: Record<string, string>) =>
    setSelectedSource({ ...source, date: dates?.[String(source.document_id)] });

  const renderConversationList = () => <ConversationList conversations={conversations} activeId={activeConvId}
    onNew={handleNewConversation} onOpen={loadConversation} onRename={handleRename} onDelete={handleDeleteConversation} />;

  return (
    <div className="flex h-full overflow-hidden">
      {/* Desktop history sidebar */}
      {showHistoryDesktop && (
        <div className="hidden md:flex w-64 min-w-[180px] max-w-[400px] resize-x overflow-hidden border-r flex-col bg-surface shrink-0">
          <div className="flex items-center justify-between border-b px-3 py-2.5 flex-none">
            <span className="text-xs font-medium flex items-center gap-1.5 text-muted-foreground">
              <History className="h-3.5 w-3.5" /> Conversations
            </span>
          </div>
          {renderConversationList()}
        </div>
      )}

      {/* Mobile history sheet */}
      <Sheet open={showHistory} onOpenChange={setShowHistory}>
        <SheetContent side="left" className="w-[300px] p-0 flex flex-col" onEscapeKeyDown={event => {
          if ((event.target as HTMLElement)?.getAttribute("aria-label") === "Conversation title") event.preventDefault();
        }}>
          <SheetHeader className="border-b px-4 py-3">
            <SheetTitle className="text-sm flex items-center gap-1.5">
              <History className="h-4 w-4" /> Conversations
            </SheetTitle>
            <SheetDescription className="sr-only">Open, rename, or delete a saved conversation.</SheetDescription>
          </SheetHeader>
          {renderConversationList()}
        </SheetContent>
      </Sheet>

      <Sheet open={!!selectedSource} onOpenChange={(open) => !open && setSelectedSource(null)}>
        <SheetContent side="right" className="w-[380px] sm:w-[460px] p-0 flex flex-col">
          {selectedSource && (() => {
            const title = selectedSource.title || `Document ${selectedSource.document_id}`;
            return <>
              <SheetHeader className="border-b px-4 py-3 pr-12">
                <SheetTitle className="doc-title">{title}</SheetTitle>
                <SheetDescription className="flex flex-wrap items-center gap-x-3 label-12 text-fg-subtle tabular-nums">
                  {selectedSource.doc_type && <TypeDot type={selectedSource.doc_type} className="label-12" />}
                  {selectedSource.date && <span>{formatSourceDate(selectedSource.date)}</span>}
                  <span className="sr-only">The retrieved excerpt from this source.</span>
                </SheetDescription>
              </SheetHeader>
              <div className="flex-1 overflow-y-auto p-4 space-y-4">
                <div>
                  <h3 className="mb-1.5 label-12 font-medium text-fg-muted">Retrieved excerpt</h3>
                  <p className="ocr whitespace-pre-wrap">{selectedSource.excerpt || "No excerpt was returned for this source."}</p>
                </div>
                <SourceLinks source={selectedSource} paperlessBaseUrl={paperlessBaseUrl} />
              </div>
            </>;
          })()}
        </SheetContent>
      </Sheet>

      {/* Chat area */}
      <div className="flex-1 flex flex-col min-w-0">
        <div className="flex-none border-b px-3 md:px-4 py-2.5 flex items-center justify-between bg-card/50 backdrop-blur-sm">
          <div className="flex items-center gap-2">
            {/* Mobile: show history button */}
            <Button variant="ghost" size="icon" className="md:hidden" aria-label="Open conversations" onClick={() => setShowHistory(true)}>
              <History className="h-4 w-4" />
            </Button>
            <h1 className="page-title">Ask</h1>
          </div>
          {/* Desktop: toggle history */}
          <Tooltip>
            <TooltipTrigger asChild>
              <Button variant="ghost" size="icon" className="h-8 w-8 hidden md:inline-flex" aria-label={showHistoryDesktop ? "Hide conversations" : "Show conversations"} onClick={() => setShowHistoryDesktop(!showHistoryDesktop)}>
                <History className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Toggle history</TooltipContent>
          </Tooltip>
        </div>

        {/* Positioned, so absolute descendants (sr-only text) overflow this column and not the app frame, which scrollIntoView would then shift. */}
        <div ref={messagesRef} className="relative flex-1 overflow-y-auto app-scroll">
          <div className="@container max-w-3xl min-[1180px]:max-w-[1028px] mx-auto px-3 md:px-4 py-4 md:py-6 space-y-4">
            <QueryModes value={queryMode} onChange={setQueryMode} />
            {messages.length === 0 && !loading && (
              <div className="py-8 md:py-12 space-y-6">
                <p className="page-title">Ask a question about your documents.</p>
                <div className="flex flex-col items-start gap-2">
                  {["What are my current insurance policies?", "What tax documents do I have for this year?", "Show home-related documents by date."].map(question =>
                    <Button key={question} variant="outline" className="h-auto min-h-11 whitespace-normal text-left justify-start" onClick={() => { setInput(question); textareaRef.current?.focus(); }}>{question}</Button>)}
                </div>
              </div>
            )}

            {messages.map((msg, i) => {
              if (msg.role === "user") return <h2 key={i} className={`question ${i === 0 ? "pt-0" : "pt-6"}`}>{msg.content}</h2>;
              const view = answerView(msg);
              const question = messages[i - 1]?.role === "user" ? messages[i - 1].content : "";
              const seconds = msg.queryTime ?? msg.query_time_ms;
              const verdict = <VerdictLine state={view.state} reason={view.reason} level={view.level}
                seconds={seconds != null ? seconds / 1000 : undefined} />;
              const sourceCount = msg.sources?.length ?? 0;
              // Only audited states have claim results to show; Fast and withheld ledgers are not shown.
              const audited = ["verified", "documented", "partial"].includes(view.state);
              const claims: TableClaim[] = audited ? msg.claim_ledger?.claims ?? [] : [];
              const omitted = audited ? msg.verification?.partial?.omitted_units ?? [] : [];
              const select = (n: number) => setActiveClaims(current => ({ ...current, [i]: n }));
              // Audited sources carry no type; the evidence pack has it for the same document.
              const sources = (msg.sources ?? []).map(s => s.doc_type ? s
                : { ...s, doc_type: msg.evidence_pack?.items?.find(item => item.document_id === s.document_id && item.doc_type)?.doc_type });
              return (
              <article key={i} aria-label="Answer" className="space-y-4 @min-[58rem]:pr-[356px]">
                {view.state === "failed" ? (
                  <Alert variant="destructive" className="[&_[data-testid=verdict]]:border-0 [&_[data-testid=verdict]]:p-0">
                    <AlertDescription className="gap-3">
                      {verdict}
                      {question && <Button size="sm" onClick={() => handleSubmit(question)} disabled={loading}>Retry</Button>}
                    </AlertDescription>
                  </Alert>
                ) : verdict}
                {view.prose !== null && (
                  <ClaimProse id={`answer-${i}`} text={view.prose} state={view.state}
                    claims={msg.claim_ledger?.candidate_text ? msg.claim_ledger.claims : undefined} sourceDates={msg.source_dates}
                    active={activeClaims[i] ?? null} onSelect={select} />
                )}
                {view.state === "withheld" && (
                  <div className="flex flex-wrap gap-2">
                    {question && <Button onClick={() => { setQueryMode("deep"); handleSubmit(question, "deep"); }} disabled={loading}>Ask again in Deep mode</Button>}
                    {sourceCount > 0 && (
                      <Button variant="outline" onClick={() => document.getElementById(`sources-${i}`)?.focus()}>
                        Review the {sourceCount} {sourceCount === 1 ? "document" : "documents"}
                      </Button>
                    )}
                  </div>
                )}
                {view.prose !== null && (
                  <div className="flex flex-wrap items-center gap-2">
                    {msg.cached && <Badge variant="outline">cached</Badge>}
                    <CopyButton text={view.prose} />
                  </div>
                )}

                  <QuestionCoverage receipt={msg.finalization?.question_coverage} />

                  {(claims.length > 0 || omitted.length > 0) && (
                    <ClaimTable id={`claims-${i}`} claims={claims} omitted={omitted} sourceDates={msg.source_dates}
                      active={activeClaims[i] ?? null} onSelect={n => {
                        select(n);
                        // After the render that shows the note on a phone, as a claim number does.
                        requestAnimationFrame(() => document.getElementById(`note-answer-${i}-${n}`)?.scrollIntoView({ block: "nearest" }));
                      }} />
                  )}

                  <TimelineDates events={msg.timeline_events}
                    receipt={msg.finalization?.timeline} answerId={`answer-${i}`} onSource={source => openSource(source, msg.source_dates)} />

                  {sources.length > 0 && (
                    <SourceList id={`sources-${i}`} sources={sources} claims={claims} sourceDates={msg.source_dates}
                      active={activeClaims[i]} paperlessBaseUrl={paperlessBaseUrl} onOpen={source => openSource(source, msg.source_dates)} />
                  )}

                  <HowChecked msg={msg} showClaims={view.prose !== null} />
              </article>
              );
            })}

            {loading && <StreamStatus step={statusMessage || "Starting"} seconds={elapsed} />}

            {/* Follow-up suggestions */}
            {followUpSuggestions.length > 0 && !loading && (
              <div className="space-y-2 pt-2">
                <p className="label-12 font-medium text-fg-muted">Suggested follow-ups</p>
                <div className="flex flex-col items-start gap-2">
                  {followUpSuggestions.map((suggestion, i) => (
                    <Button key={i} variant="outline" className="h-auto min-h-11 whitespace-normal text-left justify-start" onClick={() => handleSubmit(suggestion)}>{suggestion}</Button>
                  ))}
                </div>
              </div>
            )}

          </div>
        </div>

        {/* Input */}
        <div ref={composerRef} className="flex-none border-t p-3 md:p-4 bg-card/50 backdrop-blur-sm">
          <div className="max-w-3xl min-[1180px]:max-w-[996px] mx-auto space-y-2">
            <div className="flex items-center justify-between gap-2">
              <label htmlFor="question" className="label-14 shrink-0">Ask a question</label>
              <div className="min-w-0"><QueryModel models={models} value={selectedModel} defaultModel={defaultModel} onChange={setSelectedModel} /></div>
            </div>
            <form onSubmit={(e) => { e.preventDefault(); handleSubmit(); }} className="flex gap-2 items-end">
              <Textarea
                ref={textareaRef}
                id="question"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask a question..."
                disabled={loading}
                className="flex-1 min-h-[44px] max-h-[160px] text-base md:text-sm resize-none"
                rows={1}
              />
              <Button type="submit" aria-label="Send question" disabled={loading || !input.trim()} size="icon" className="h-[44px] w-[44px] shrink-0">
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
      <div className="flex h-full flex-col items-center justify-center gap-4">
        <h1 className="page-title">Ask</h1>
        <Loader2 className="h-8 w-8 animate-spin text-primary/40" />
      </div>
    }>
      <QueryContent />
    </Suspense>
  );
}
