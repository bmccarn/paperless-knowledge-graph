"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TypeDot } from "@/components/kind-shape";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Pager } from "@/components/pager";
import { errMsg } from "@/lib/utils";
import { graphSearch, getPaperlessDocUrl, usePaperlessUrl } from "@/lib/api";
import type { GraphNode } from "@/lib/types";
import { Building, Car, FileText, HeartPulse, Home, Loader2, Shield, Search, ExternalLink } from "lucide-react";

const DOMAINS = [
  {
    id: "insurance",
    title: "Insurance",
    icon: Shield,
    query: "insurance policy coverage premium deductible homeowners auto",
    questions: [
      "What are my current insurance policies?",
      "Compare my current auto insurance coverage.",
      "What homeowners coverage do I have right now?",
    ],
  },
  {
    id: "tax",
    title: "Taxes",
    icon: Building,
    query: "tax return W-2 1099 K-1 business IRS estimated payment",
    questions: [
      "What tax documents do I have for this year?",
      "What business tax documents do I have?",
      "What estimated payments or IRS notices are in the archive?",
    ],
  },
  {
    id: "medical",
    title: "Medical",
    icon: HeartPulse,
    query: "medical health diagnosis prescription lab result doctor VA disability",
    questions: [
      "Summarize my recent medical documents.",
      "What medications are referenced in my documents?",
      "What VA or disability records are in the archive?",
    ],
  },
  {
    id: "vehicles",
    title: "Vehicles",
    icon: Car,
    query: "vehicle auto car truck registration title insurance VIN",
    questions: [
      "What vehicles are referenced in my documents?",
      "Which vehicle documents look current?",
      "What vehicle insurance or registration records exist?",
    ],
  },
  {
    id: "home",
    title: "Home & Mortgage",
    icon: Home,
    query: "home mortgage escrow homeowners property deed loan utility",
    questions: [
      "What mortgage or home loan documents are in the archive?",
      "What home insurance and property documents are current?",
      "Show home-related documents by date.",
    ],
  },
];

const PAGE_SIZE = 12;

export default function HubsPage() {
  const [activeId, setActiveId] = useState(DOMAINS[0].id);
  const [pages, setPages] = useState<Record<string, number>>({});
  const [docs, setDocs] = useState<GraphNode[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const requestVersion = useRef(0);
  const paperlessBaseUrl = usePaperlessUrl();
  const active = DOMAINS.find((d) => d.id === activeId) || DOMAINS[0];
  const page = pages[active.id] || 0;

  const requestKey = `${active.id}:${page}`;
  const [previousRequest, setPreviousRequest] = useState(requestKey);
  if (previousRequest !== requestKey) {
    setPreviousRequest(requestKey);
    setLoading(true);
    setError(null);
    setDocs([]);
    setTotal(null);
  }

  const loadDomain = useCallback(() => {
    const version = ++requestVersion.current;
    return graphSearch(active.query, "Document", PAGE_SIZE, page * PAGE_SIZE).then((data) => {
      if (version !== requestVersion.current) return;
      setDocs(data.results || []);
      setTotal(data.total);
      if (page > 0 && page * PAGE_SIZE >= data.total) {
        setPages((previous) => ({ ...previous, [active.id]: Math.max(0, Math.ceil(data.total / PAGE_SIZE) - 1) }));
      }
    }).catch((e) => {
      if (version === requestVersion.current) setError(errMsg(e, "Failed to load hub documents"));
    }).finally(() => {
      if (version === requestVersion.current) setLoading(false);
    });
  }, [active, page]);

  const invalidateRequests = useCallback(() => { requestVersion.current++; }, []);
  useEffect(() => {
    void loadDomain();
    return invalidateRequests;
  }, [loadDomain, invalidateRequests]);

  function resetDomain() {
    setLoading(true);
    setError(null);
    setDocs([]);
    setTotal(null);
  }
  const selectDomain = (id: string) => {
    if (id === activeId) return;
    invalidateRequests();
    setActiveId(id);
  };
  const selectPage = (next: number) => {
    invalidateRequests();
    setPages((previous) => ({ ...previous, [active.id]: next }));
  };

  const ActiveIcon = active.icon;
  const pageCount = total === null ? 1 : Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div className="h-full overflow-y-auto p-4 md:px-12 md:py-10 space-y-6">
      <div>
        <h1 className="page-title">Hubs</h1>
        <p className="text-sm text-muted-foreground mt-1">Saved views for recurring document workflows.</p>
      </div>

      <Tabs value={activeId} onValueChange={selectDomain} activationMode="manual">
        <TabsList aria-label="Document domains">
          {DOMAINS.map(domain => {
            const Icon = domain.icon;
            return <TabsTrigger key={domain.id} value={domain.id}><Icon aria-hidden="true" />{domain.title}</TabsTrigger>;
          })}
        </TabsList>
        {DOMAINS.map(domain => <TabsContent key={domain.id} value={domain.id}>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_300px]">
        <Card>
          <CardHeader>
            <CardTitle className="heading-16 flex items-center gap-2"><ActiveIcon className="h-4 w-4" aria-hidden="true" /> {active.title} documents</CardTitle>
            {total !== null && !loading && !error && <p className="text-sm text-muted-foreground">{total} matching indexed documents</p>}
          </CardHeader>
          <CardContent className="space-y-2">
            {loading ? (
              <div role="status" className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading documents...</div>
            ) : error ? (
              <Alert variant="destructive"><AlertTitle>Could not load documents</AlertTitle><AlertDescription><p>{error}</p><Button variant="outline" onClick={() => { resetDomain(); void loadDomain(); }}>Retry hub search</Button></AlertDescription></Alert>
            ) : docs.length ? docs.map((doc) => {
              const p = doc.properties || {};
              const docId = p.paperless_id as number;
              const content = <>
                    <FileText className="mt-1 size-4 shrink-0 text-fg-muted" aria-hidden="true" />
                    <div className="min-w-0 space-y-1">
                      <p className="doc-title break-words">{(p.title as string) || `Document #${docId}`}</p>
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 label-12 text-fg-muted tabular-nums">
                        {typeof p.doc_type === "string" && p.doc_type && <TypeDot type={p.doc_type} />}
                        {typeof p.date === "string" && p.date && <span>{p.date}</span>}
                      </div>
                    </div>
              </>;
              return (
                <div key={String(docId || p.uuid)} className="flex items-center gap-2 border-b last:border-0">
                  {docId ? <Link href={`/documents/${docId}`} className="flex min-w-0 flex-1 items-start gap-3 rounded-md px-1 py-3 hover:bg-raised">{content}</Link> : <div className="flex min-w-0 flex-1 items-start gap-3 px-1 py-3">{content}</div>}
                  {docId && <Button asChild variant="ghost" size="sm">
                    <a href={getPaperlessDocUrl(docId, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer" aria-label={`Open ${(p.title as string) || `Document #${docId}`} in Paperless`}>
                      <ExternalLink aria-hidden="true" /><span>Paperless</span>
                    </a>
                  </Button>}
                </div>
              );
            }) : (
              <div className="space-y-2"><p className="text-sm text-fg-muted">No documents found for this hub search.</p><Button variant="outline" onClick={() => { resetDomain(); void loadDomain(); }}>Retry hub search</Button></div>
            )}
            {total !== null && !loading && !error && (
              <Pager page={page} pageCount={pageCount} pageSize={PAGE_SIZE} total={total} onPage={selectPage} noun="hub page" />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="heading-16 flex items-center gap-2"><Search className="h-4 w-4" aria-hidden="true" /> Common questions</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {active.questions.map((question) => (
              <Button asChild key={question} variant="outline" className="w-full justify-start text-left h-auto py-2.5 whitespace-normal">
                <Link href={`/query?draft=${encodeURIComponent(question)}`}>{question}</Link>
              </Button>
            ))}
          </CardContent>
        </Card>
      </div>
        </TabsContent>)}
      </Tabs>
    </div>
  );
}
