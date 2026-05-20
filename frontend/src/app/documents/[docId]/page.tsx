"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";
import { getDocumentDetail, postDocumentFeedback, postReindexDoc, getPaperlessDocUrl, getConfig } from "@/lib/api";
import { ArrowLeft, ExternalLink, FileText, Loader2, RefreshCw, ThumbsDown, Network } from "lucide-react";

interface DetailPayload {
  paperless: Record<string, unknown>;
  graph: {
    document: Record<string, unknown> | null;
    entities: Array<{ labels: string[]; properties: Record<string, unknown> }>;
    relationships: Array<Record<string, unknown>>;
  };
  chunks: Array<{ chunk_index: number; title?: string; doc_type?: string; content: string; created_at?: string }>;
  processing: {
    processed: boolean;
    processed_at?: string | null;
    chunk_count: number;
    feedback_count: number;
  };
}

export default function DocumentDetailPage() {
  const params = useParams<{ docId: string }>();
  const docId = Number(params.docId);
  const [detail, setDetail] = useState<DetailPayload | null>(null);
  const [paperlessBaseUrl, setPaperlessBaseUrl] = useState("");
  const [loading, setLoading] = useState(true);
  const [reindexing, setReindexing] = useState(false);
  const [feedbackNote, setFeedbackNote] = useState("");
  const [feedbackSent, setFeedbackSent] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      setDetail(await getDocumentDetail(docId));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    getConfig().then((c) => setPaperlessBaseUrl(c.paperless_url)).catch(() => {});
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [docId]);

  const handleReindex = async () => {
    setReindexing(true);
    try {
      await postReindexDoc(docId);
      await load();
    } finally {
      setReindexing(false);
    }
  };

  const handleFeedback = async () => {
    await postDocumentFeedback(docId, "extraction_wrong", feedbackNote);
    setFeedbackNote("");
    setFeedbackSent(true);
    await load();
  };

  if (loading && !detail) {
    return <div className="flex h-full items-center justify-center"><Loader2 className="h-8 w-8 animate-spin text-primary/50" /></div>;
  }

  const title = (detail?.paperless?.title as string) || `Document #${docId}`;
  const docType = (detail?.graph?.document?.doc_type as string) || (detail?.chunks?.[0]?.doc_type as string) || "unknown";

  return (
    <div className="h-full overflow-y-auto p-4 md:p-6 lg:p-8 space-y-4">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <Link href="/documents" className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground mb-2">
            <ArrowLeft className="h-3 w-3" /> Back to documents
          </Link>
          <h1 className="text-xl md:text-2xl font-bold tracking-tight">{title}</h1>
          <div className="flex flex-wrap gap-2 mt-2">
            <Badge variant="secondary">{docType}</Badge>
            <Badge variant={detail?.processing?.processed ? "default" : "destructive"}>
              {detail?.processing?.processed ? "processed" : "not processed"}
            </Badge>
            <Badge variant="outline">{detail?.processing?.chunk_count || 0} chunks</Badge>
            {detail?.processing?.feedback_count ? <Badge variant="outline">{detail.processing.feedback_count} review flags</Badge> : null}
          </div>
        </div>
        <div className="flex gap-2">
          <Button onClick={handleReindex} disabled={reindexing} size="sm" className="gap-2">
            {reindexing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
            Reindex
          </Button>
          <a href={getPaperlessDocUrl(docId, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer">
            <Button variant="outline" size="sm" className="gap-2">
              <ExternalLink className="h-3.5 w-3.5" /> Paperless
            </Button>
          </a>
          <Link href={`/query?q=${encodeURIComponent(`Why would document ${docId} be used as a source?`)}`}>
            <Button variant="outline" size="sm">Why sourced?</Button>
          </Link>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_360px]">
        <div className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="text-base">Raw OCR</CardTitle></CardHeader>
            <CardContent>
              <pre className="max-h-[420px] overflow-auto whitespace-pre-wrap rounded-lg bg-muted p-3 text-xs leading-relaxed">
                {(detail?.paperless?.content as string) || "No OCR content returned by Paperless."}
              </pre>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-base">Indexed Chunks</CardTitle></CardHeader>
            <CardContent className="space-y-3">
              {detail?.chunks?.length ? detail.chunks.map((chunk) => (
                <div key={chunk.chunk_index} className="rounded-lg border p-3">
                  <p className="text-xs font-medium text-muted-foreground mb-2">Chunk {chunk.chunk_index}</p>
                  <p className="text-xs leading-relaxed whitespace-pre-wrap">{chunk.content}</p>
                </div>
              )) : <p className="text-sm text-muted-foreground">No chunks indexed for this document.</p>}
            </CardContent>
          </Card>
        </div>

        <div className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="text-base flex items-center gap-2"><Network className="h-4 w-4" /> Extraction</CardTitle></CardHeader>
            <CardContent className="space-y-3">
              <div>
                <p className="text-xs text-muted-foreground">Processed at</p>
                <p className="text-sm">{detail?.processing?.processed_at ? new Date(detail.processing.processed_at).toLocaleString() : "Never"}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground mb-1">Entities</p>
                <div className="flex flex-wrap gap-1.5">
                  {detail?.graph?.entities?.length ? detail.graph.entities.slice(0, 80).map((entity, idx) => (
                    <Badge key={idx} variant="outline" className="text-[10px]">
                      {(entity.properties.name as string) || (entity.properties.title as string) || entity.labels?.[0]}
                    </Badge>
                  )) : <span className="text-sm text-muted-foreground">No entities extracted.</span>}
                </div>
              </div>
              <div>
                <p className="text-xs text-muted-foreground mb-1">Relationships</p>
                <div className="space-y-1 max-h-64 overflow-auto">
                  {detail?.graph?.relationships?.length ? detail.graph.relationships.map((rel, idx) => (
                    <div key={idx} className="rounded border px-2 py-1 text-xs">
                      {String(rel.rel_type)} to {String((rel.props as Record<string, unknown>)?.name || (rel.props as Record<string, unknown>)?.title || rel.labels)}
                    </div>
                  )) : <span className="text-sm text-muted-foreground">No relationships extracted.</span>}
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-base flex items-center gap-2"><ThumbsDown className="h-4 w-4" /> Extraction Review</CardTitle></CardHeader>
            <CardContent className="space-y-3">
              <Textarea
                value={feedbackNote}
                onChange={(e) => setFeedbackNote(e.target.value)}
                placeholder="What looks wrong?"
                rows={3}
              />
              <Button onClick={handleFeedback} disabled={!feedbackNote.trim()} variant="secondary" className="w-full gap-2">
                <FileText className="h-4 w-4" /> Mark extraction wrong
              </Button>
              {feedbackSent && <p className="text-xs text-emerald-400">Review flag recorded.</p>}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
