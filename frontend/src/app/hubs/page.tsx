"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { graphSearch, getConfig, getPaperlessDocUrl } from "@/lib/api";
import { Building, Car, FileText, HeartPulse, Home, Loader2, Shield, Search, ExternalLink } from "lucide-react";

const DOMAINS = [
  {
    id: "insurance",
    title: "Insurance",
    icon: Shield,
    query: "insurance policy coverage premium deductible Progressive USAA homeowners auto",
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
    query: "tax return W-2 1099 K-1 RapidRoute IRS estimated payment",
    questions: [
      "What tax documents do I have for this year?",
      "What documents mention RapidRoute Solutions taxes?",
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

interface SearchResult {
  labels: string[];
  properties: Record<string, unknown>;
}

export default function HubsPage() {
  const [activeId, setActiveId] = useState(DOMAINS[0].id);
  const [results, setResults] = useState<Record<string, SearchResult[]>>({});
  const [loading, setLoading] = useState("");
  const [paperlessBaseUrl, setPaperlessBaseUrl] = useState("");
  const active = DOMAINS.find((d) => d.id === activeId) || DOMAINS[0];

  const loadDomain = useCallback(async (domain = active) => {
    if (results[domain.id]) return;
    setLoading(domain.id);
    try {
      const data = await graphSearch(domain.query, "Document", 12);
      setResults((prev) => ({ ...prev, [domain.id]: data.results || [] }));
    } finally {
      setLoading("");
    }
  }, [active, results]);

  useEffect(() => { getConfig().then((c) => setPaperlessBaseUrl(c.paperless_url)).catch(() => {}); }, []);
  useEffect(() => { loadDomain(active); }, [active, loadDomain]);

  const ActiveIcon = active.icon;
  const docs = results[active.id] || [];

  return (
    <div className="h-full overflow-y-auto p-4 md:p-6 lg:p-8 space-y-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Domain Hubs</h1>
        <p className="text-sm text-muted-foreground mt-1">Saved views for recurring document workflows.</p>
      </div>

      <div className="flex flex-wrap gap-2">
        {DOMAINS.map((domain) => {
          const Icon = domain.icon;
          return (
            <button
              key={domain.id}
              onClick={() => setActiveId(domain.id)}
              className={
                "inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-sm transition-colors " +
                (activeId === domain.id ? "bg-primary text-primary-foreground" : "bg-card hover:bg-accent")
              }
            >
              <Icon className="h-4 w-4" /> {domain.title}
            </button>
          );
        })}
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_340px]">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2"><ActiveIcon className="h-4 w-4" /> {active.title} Documents</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {loading === active.id ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading documents...</div>
            ) : docs.length ? docs.map((doc) => {
              const p = doc.properties || {};
              const docId = p.paperless_id as number;
              return (
                <div key={String(docId || p.uuid)} className="flex items-center gap-3 rounded-lg border p-3">
                  <FileText className="h-4 w-4 text-muted-foreground shrink-0" />
                  <div className="min-w-0 flex-1">
                    <p className="text-sm font-medium truncate">{(p.title as string) || `Document #${docId}`}</p>
                    <div className="flex gap-1.5 mt-1">
                      {typeof p.doc_type === "string" && p.doc_type && <Badge variant="secondary" className="text-[10px]">{p.doc_type}</Badge>}
                      {typeof p.date === "string" && p.date && <Badge variant="outline" className="text-[10px]">{p.date}</Badge>}
                    </div>
                  </div>
                  {docId && (
                    <a href={getPaperlessDocUrl(docId, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer">
                      <Button variant="ghost" size="icon" className="h-8 w-8"><ExternalLink className="h-3.5 w-3.5" /></Button>
                    </a>
                  )}
                </div>
              );
            }) : (
              <p className="text-sm text-muted-foreground">No documents found for this hub search.</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2"><Search className="h-4 w-4" /> Common Questions</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {active.questions.map((question) => (
              <Link key={question} href={`/query?q=${encodeURIComponent(question)}`}>
                <Button variant="outline" className="w-full justify-start text-left h-auto py-2.5 whitespace-normal">
                  {question}
                </Button>
              </Link>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
