"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";
import { getDocumentDetail, postDocumentFeedback, resolveDocumentFeedback, postReindexDoc, getPaperlessDocUrl, getConfig } from "@/lib/api";
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
    open_feedback_count?: number;
  };
  feedback?: Array<{ id: number; reason: string; note: string; status: "open" | "resolved"; created_at: string; resolution?: string; resolution_note?: string; resolved_at?: string }>;
}

export default function DocumentDetailPage() {
  const params = useParams<{ docId: string }>();
  const docId = Number(params.docId);
  const [loadedDetail, setDetail] = useState<DetailPayload | null>(null);
  const [loadedDocumentId, setLoadedDocumentId] = useState<number | null>(null);
  const requestVersion = useRef(0);
  const detail = loadedDocumentId === docId ? loadedDetail : null;
  const [paperlessBaseUrl, setPaperlessBaseUrl] = useState("");
  const [loading, setLoading] = useState(true);
  const [reindexing, setReindexing] = useState(false);
  const [feedbackNote, setFeedbackNote] = useState("");
  const [feedbackSending, setFeedbackSending] = useState(false);
  const [resolving, setResolving] = useState<number | null>(null);
  const [resolutionNotes, setResolutionNotes] = useState<Record<number, string>>({});
  const [resolutionKinds, setResolutionKinds] = useState<Record<number, string>>({});
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const load = useCallback(async () => {
    const version = ++requestVersion.current;
    setLoading(true);
    setError("");
    try {
      const result = await getDocumentDetail(docId);
      if (version !== requestVersion.current) return;
      setDetail(result);
      setLoadedDocumentId(docId);
    } catch (error) {
      if (version === requestVersion.current) setError(error instanceof Error ? error.message : "Could not load document.");
    } finally {
      if (version === requestVersion.current) setLoading(false);
    }
  }, [docId]);

  useEffect(() => {
    getConfig().then((c) => setPaperlessBaseUrl(c.paperless_url)).catch(() => {});
    load();
  }, [load]);

  const handleReindex = async () => {
    setReindexing(true);
    setError("");
    setNotice("");
    try {
      await postReindexDoc(docId);
      await load();
      setNotice("Reindex completed. Inspect the extracted facts, then resolve any open reports.");
    } catch (error) {
      setError(error instanceof Error ? error.message : "Reindex failed. Review reports remain open.");
    } finally {
      setReindexing(false);
    }
  };

  const handleFeedback = async () => {
    setFeedbackSending(true);
    setError("");
    setNotice("");
    try {
      await postDocumentFeedback(docId, "extraction_wrong", feedbackNote);
      setFeedbackNote("");
      setNotice("Open review report recorded. The extraction remains disputed until reviewed.");
      await load();
    } catch (error) {
      setError(error instanceof Error ? error.message : "Could not record review report.");
    } finally {
      setFeedbackSending(false);
    }
  };

  const handleResolve = async (feedbackId: number) => {
    setResolving(feedbackId);
    setError("");
    setNotice("");
    try {
      await resolveDocumentFeedback(docId, feedbackId, resolutionKinds[feedbackId] || "reindexed_and_reviewed", resolutionNotes[feedbackId] || "");
      setNotice("Review resolution recorded and cached answers invalidated.");
      await load();
    } catch (error) {
      setError(error instanceof Error ? error.message : "Could not resolve report. Complete reindex and review first.");
    } finally {
      setResolving(null);
    }
  };

  if (loading && !detail) {
    return <div className="flex h-full items-center justify-center"><Loader2 className="h-8 w-8 animate-spin text-primary/50" /></div>;
  }
  if (!detail) {
    return <div className="space-y-3 p-6"><p role="alert">{error || "Document details are unavailable."}</p><Button onClick={load}>Retry loading document</Button></div>;
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
            {detail?.processing?.open_feedback_count ? <Badge variant="destructive">{detail.processing.open_feedback_count} open review reports</Badge> : null}
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

      {error && <p role="alert" className="rounded border border-destructive p-3 text-sm text-destructive">{error} <button className="underline" onClick={load}>Reload document</button></p>}
      {notice && <p role="status" className="rounded border p-3 text-sm">{notice}</p>}

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
              <p className="text-xs text-muted-foreground">Open reports mark derived extraction as disputed. Original OCR remains available. Reindexing keeps reports open until you inspect and resolve them.</p>
              <Textarea
                value={feedbackNote}
                onChange={(e) => setFeedbackNote(e.target.value)}
                placeholder="What looks wrong?"
                aria-label="Extraction review report"
                rows={3}
              />
              <Button onClick={handleFeedback} disabled={!feedbackNote.trim() || feedbackSending} variant="secondary" className="w-full gap-2">
                <FileText className="h-4 w-4" /> Mark extraction wrong
              </Button>
              {(detail?.feedback || []).map((report) => (
                <div key={report.id} className="space-y-2 rounded border p-3 text-xs">
                  <div className="flex items-center justify-between"><Badge variant={report.status === "open" ? "destructive" : "outline"}>{report.status}</Badge><span>{new Date(report.created_at).toLocaleDateString()}</span></div>
                  <p className="whitespace-pre-wrap">{report.note || report.reason}</p>
                  {report.status === "resolved" ? <p className="text-muted-foreground">{report.resolution === "reindexed_and_reviewed" ? "Reindexed and reviewed" : "Dismissed after review"}: {report.resolution_note}</p> : <>
                    <label htmlFor={`resolution-${report.id}`} className="block">Resolution</label>
                    <select id={`resolution-${report.id}`} className="w-full rounded border bg-background p-2" value={resolutionKinds[report.id] || "reindexed_and_reviewed"} onChange={(event) => setResolutionKinds({ ...resolutionKinds, [report.id]: event.target.value })}>
                      <option value="reindexed_and_reviewed">Reindexed and checked correction</option>
                      <option value="dismissed_after_review">Checked original extraction; no correction needed</option>
                    </select>
                    <Textarea aria-label={`Review note for report ${report.id}`} placeholder="What did you check against the original source?" value={resolutionNotes[report.id] || ""} onChange={(event) => setResolutionNotes({ ...resolutionNotes, [report.id]: event.target.value })} rows={2} />
                    <Button variant="outline" size="sm" disabled={!resolutionNotes[report.id]?.trim() || resolving !== null || reindexing} onClick={() => handleResolve(report.id)}>{resolving === report.id ? "Resolving…" : "Resolve reviewed report"}</Button>
                  </>}
                </div>
              ))}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
