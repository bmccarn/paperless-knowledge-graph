"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { KindShape, TypeDot } from "@/components/kind-shape";
import { nodeFromPayload, relationshipSupport } from "@/lib/graph-data";
import { nodeLabelText } from "@/lib/categories";
import { displayName } from "@/lib/graph-encoding";
import { Textarea } from "@/components/ui/textarea";
import { errMsg } from "@/lib/utils";
import { getDocumentDetail, postDocumentFeedback, resolveDocumentFeedback, postReindexDoc, getPaperlessDocUrl, usePaperlessUrl } from "@/lib/api";
import { ArrowLeft, ExternalLink, FileText, RefreshCw, ThumbsDown, ChevronDown } from "lucide-react";

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
  const paperlessBaseUrl = usePaperlessUrl();
  const [loading, setLoading] = useState(true);
  const [reindexing, setReindexing] = useState(false);
  const [feedbackNote, setFeedbackNote] = useState("");
  const [feedbackSending, setFeedbackSending] = useState(false);
  const [resolving, setResolving] = useState<number | null>(null);
  const [resolutionNotes, setResolutionNotes] = useState<Record<number, string>>({});
  const [resolutionKinds, setResolutionKinds] = useState<Record<number, string>>({});
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const [previousDocId, setPreviousDocId] = useState(docId);
  if (!Object.is(previousDocId, docId)) {
    setPreviousDocId(docId);
    setLoading(true);
    setError("");
  }

  const load = useCallback(() => {
    const version = ++requestVersion.current;
    return getDocumentDetail(docId).then((result) => {
      if (version !== requestVersion.current) return;
      setDetail(result);
      setLoadedDocumentId(docId);
    }).catch((error) => {
      if (version === requestVersion.current) setError(errMsg(error, "Could not load document."));
    }).finally(() => {
      if (version === requestVersion.current) setLoading(false);
    });
  }, [docId]);

  useEffect(() => {
    load();
  }, [load]);

  const reload = () => {
    setLoading(true);
    setError("");
    return load();
  };

  const handleReindex = async () => {
    setReindexing(true);
    setError("");
    setNotice("");
    try {
      await postReindexDoc(docId);
      await reload();
      setNotice("Reindex completed. Inspect the extracted facts, then resolve any open reports.");
    } catch (error) {
      setError(errMsg(error, "Reindex failed. Review reports remain open."));
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
      await reload();
    } catch (error) {
      setError(errMsg(error, "Could not record review report."));
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
      await reload();
    } catch (error) {
      setError(errMsg(error, "Could not resolve report. Complete reindex and review first."));
    } finally {
      setResolving(null);
    }
  };

  if (loading && !detail) {
    return <div className="space-y-6 p-4 md:px-12 md:py-10" aria-busy="true" aria-label="Loading document">
      <h1 className="page-title">Documents</h1>
      <Skeleton className="h-8 w-2/3" />
      <Skeleton className="h-36 w-full" />
      <Skeleton className="h-48 w-full" />
    </div>;
  }
  if (!detail) {
    return <div className="space-y-4 p-4 md:px-12 md:py-10">
      <h1 className="page-title">Document not found</h1>
      <p role="alert" className="copy-14 text-fg-muted">{error || "We could not load this document."}</p>
      <div className="flex flex-wrap items-center gap-4">
        <Link href="/documents" className="inline-flex min-h-11 items-center underline underline-offset-4">Back to Documents</Link>
        <Button onClick={reload} variant="outline">Retry loading document</Button>
      </div>
    </div>;
  }

  const title = (detail.paperless.title as string) || `Document #${docId}`;
  const docType = (detail.graph.document?.doc_type as string) || detail.chunks[0]?.doc_type || "unknown";
  const date = detail.paperless.created || detail.graph.document?.date;
  const correspondent = detail.graph.document?.correspondent || detail.paperless.correspondent;
  const entities = detail.graph.entities.map(nodeFromPayload).filter(entity => entity !== null);
  const kinds = [...new Set(entities.map(entity => entity.label))];

  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-6xl space-y-8 p-4 md:px-12 md:py-10">
        <header className="space-y-4">
          <Link href="/documents" className="inline-flex min-h-11 items-center gap-2 label-13 text-fg-muted underline underline-offset-4 md:min-h-0">
            <ArrowLeft className="size-4" aria-hidden /> Back to Documents
          </Link>
          <h1 className="doc-title text-[22px] leading-7 text-balance">{title}</h1>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2 label-13 text-fg-muted tabular-nums">
            <TypeDot type={docType} />
            {date ? <span>Date: {String(date).slice(0, 10)}</span> : null}
            {correspondent ? <span>{typeof correspondent === "number" ? "Correspondent ID" : "Correspondent"}: {String(correspondent)}</span> : null}
            <span>{detail.processing.processed ? "Processed" : "Not processed"}</span>
            <span>{detail.processing.chunk_count || 0} chunks</span>
            {detail.processing.open_feedback_count ? <Badge variant="destructive">{detail.processing.open_feedback_count} open review reports</Badge> : null}
          </div>
          <div className="flex flex-wrap gap-2">
            <Button asChild variant="outline"><a href={getPaperlessDocUrl(docId, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer"><ExternalLink aria-hidden />Open in Paperless</a></Button>
            <Button onClick={handleReindex} loading={reindexing}><RefreshCw aria-hidden />Reindex</Button>
            <Button asChild variant="ghost"><Link href={`/query?q=${encodeURIComponent(`Why would document ${docId} be used as a source?`)}`}>Why sourced?</Link></Button>
          </div>
        </header>

        {error && <Alert variant="destructive"><AlertDescription>{error}<Button variant="link" onClick={reload}>Reload document</Button></AlertDescription></Alert>}
        {notice && <Alert variant="success" role="status"><AlertDescription>{notice}</AlertDescription></Alert>}

        <section aria-labelledby="extraction-heading" className="space-y-5 border-t pt-6">
          <h2 id="extraction-heading" className="heading-16">Extracted from this document</h2>
          {entities.length ? <div className="grid gap-6 md:grid-cols-2">
            {kinds.map(kind => <div key={kind} className="space-y-2">
              <h3><KindShape label={kind} /></h3>
              <ul className="space-y-1">
                {entities.filter(entity => entity.label === kind).map(entity => <li key={entity.id}>
                  <Link href={`/graph?focus=${encodeURIComponent(entity.id)}`} className="inline-flex min-h-11 items-center label-14 underline underline-offset-4 md:min-h-8">{displayName(entity.name, entity.id, entity.label)}</Link>
                </li>)}
              </ul>
            </div>)}
          </div> : <p className="copy-14 text-fg-muted">No entities extracted. Reindex this document to extract its entities.</p>}
          <div className="space-y-2">
            <h3 className="label-14">Relationships</h3>
            {detail.graph.relationships.length ? <ul className="divide-y rounded-lg border bg-surface px-4">
              {detail.graph.relationships.map((rel, idx) => {
                const props = (rel.rel_props || {}) as Record<string, unknown>;
                const target = (rel.props || {}) as Record<string, unknown>;
                const support = relationshipSupport(props);
                const stated = new Set(support.filter(record => !record.inferred).map(record => record.documentId)).size;
                const inferred = new Set(support.filter(record => record.inferred).map(record => record.documentId)).size;
                const state = !support.length && props.inferred !== true && props.implied !== true ? "No support record" : stated && inferred ? `Stated in ${stated}, inferred in ${inferred}` : inferred || props.inferred === true || props.implied === true ? "Inferred" : "Supported";
                return <li key={idx} className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 py-3 copy-14">
                  <span>{nodeLabelText(String(rel.rel_type).replace(/_/g, " "))} {String(target.name || target.title || "Unknown entity")}</span>
                  <span className="label-12 text-fg-muted tabular-nums">{state}</span>
                </li>;
              })}
            </ul> : <p className="copy-14 text-fg-muted">No relationships extracted.</p>}
          </div>
        </section>

        <section aria-labelledby="chunks-heading" className="space-y-4 border-t pt-6">
          <h2 id="chunks-heading" className="heading-16">Indexed chunks</h2>
          {detail.chunks.length ? detail.chunks.map(chunk => <article key={chunk.chunk_index} className="space-y-2 rounded-lg border bg-surface p-4">
            <h3 className="label-12 text-fg-muted tabular-nums">Chunk {chunk.chunk_index}</h3>
            <p className="ocr max-w-(--measure) whitespace-pre-wrap break-words">{chunk.content}</p>
          </article>) : <p className="copy-14 text-fg-muted">No chunks indexed for this document.</p>}
        </section>

        <Collapsible key={docId} className="border-y py-2">
          <h2><CollapsibleTrigger className="group flex min-h-11 w-full items-center justify-between gap-4 text-left heading-16">
            Original OCR text<ChevronDown aria-hidden className="size-4 transition-transform group-data-[state=open]:rotate-180" />
          </CollapsibleTrigger></h2>
          <CollapsibleContent className="pt-2 pb-4">
            <div role="region" aria-label="Original OCR text" tabIndex={0} className="max-h-[420px] overflow-auto rounded-md bg-sunken p-4">
              <p className="ocr max-w-(--measure) whitespace-pre-wrap break-words">{(detail.paperless.content as string) || "No OCR content returned by Paperless."}</p>
            </div>
          </CollapsibleContent>
        </Collapsible>

        <section aria-labelledby="review-heading" className="space-y-4">
          <h2 id="review-heading" className="heading-16 flex items-center gap-2"><ThumbsDown className="size-4" aria-hidden />Extraction review</h2>
          <Alert role="note"><AlertDescription>Open reports mark derived extraction as disputed. Original OCR remains available. Reindexing keeps reports open until you inspect and resolve them.</AlertDescription></Alert>
          <div className="max-w-2xl space-y-3">
            <Textarea value={feedbackNote} onChange={event => setFeedbackNote(event.target.value)} placeholder="What looks wrong?" aria-label="Extraction review report" rows={3} />
            <Button onClick={handleFeedback} disabled={!feedbackNote.trim()} loading={feedbackSending} variant="secondary"><FileText aria-hidden />Mark extraction wrong</Button>
            {(detail.feedback || []).map(report => <div key={report.id} className="space-y-3 rounded-lg border bg-surface p-4 copy-14">
              <div className="flex flex-wrap items-center justify-between gap-2"><Badge variant={report.status === "open" ? "destructive" : "outline"}>{report.status === "open" ? "Open" : "Resolved"}</Badge><span className="label-12 text-fg-muted tabular-nums">{new Date(report.created_at).toLocaleDateString()}</span></div>
              <p className="whitespace-pre-wrap break-words">{report.note || report.reason}</p>
              {report.status === "resolved" ? <p className="text-fg-muted">{report.resolution === "reindexed_and_reviewed" ? "Reindexed and reviewed" : "Dismissed after review"}: {report.resolution_note}</p> : <>
                <label htmlFor={`resolution-${report.id}`} className="label-12 block">Resolution</label>
                <Select value={resolutionKinds[report.id] || "reindexed_and_reviewed"} onValueChange={value => setResolutionKinds({ ...resolutionKinds, [report.id]: value })}>
                  <SelectTrigger id={`resolution-${report.id}`}><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="reindexed_and_reviewed">Reindexed and checked correction</SelectItem>
                    <SelectItem value="dismissed_after_review">Checked original extraction; no correction needed</SelectItem>
                  </SelectContent>
                </Select>
                <Textarea aria-label={`Review note for report ${report.id}`} placeholder="What did you check against the original source?" value={resolutionNotes[report.id] || ""} onChange={event => setResolutionNotes({ ...resolutionNotes, [report.id]: event.target.value })} rows={2} />
                <Button variant="outline" disabled={!resolutionNotes[report.id]?.trim() || resolving !== null || reindexing} onClick={() => handleResolve(report.id)}>{resolving === report.id ? "Resolving…" : "Resolve reviewed report"}</Button>
              </>}
            </div>)}
          </div>
        </section>
      </div>
    </div>
  );
}
