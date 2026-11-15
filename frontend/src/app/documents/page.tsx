"use client";

import { Fragment, Suspense, useState, useEffect } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Skeleton } from "@/components/ui/skeleton";
import { Progress } from "@/components/ui/progress";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { KindShape, TypeDot } from "@/components/kind-shape";
import { cn, errMsg } from "@/lib/utils";
import { getDocuments, getGraphNode, postReindexDoc, getPaperlessDocUrl, usePaperlessUrl } from "@/lib/api";
import { Pager } from "@/components/pager";
import { Search, RefreshCw, Loader2, ExternalLink, ChevronRight, ArrowUp, ArrowDown, X } from "lucide-react";
import type { GraphNode } from "@/lib/types";
import { displayName } from "@/lib/graph-encoding";
import { parseDocumentDate } from "@/lib/answer-state";

interface DocumentRow extends GraphNode {
  entity_count?: number;
}

interface DocumentsPage {
  results?: DocumentRow[];
  total?: number;
  doc_types?: Record<string, number>;
  entity?: { uuid: string; name?: string; labels?: string[] } | null;
}

interface LinkedEntity {
  uuid: string;
  name: string;
  label: string;
}

interface ExpandedDoc {
  entities: LinkedEntity[];
  loading: boolean;
}

interface NodeDetail {
  relationships?: { neighbor_labels?: string[]; neighbor_props?: Record<string, unknown> }[];
}

const PAGE_SIZE = 25;
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "2026-01-31" and "January 31, 2026" become "31 Jan 2026"; the stored date has no time zone to shift. */
function formatDate(value: unknown): string {
  const date = parseDocumentDate(String(value || ""));
  return date ? `${String(date[2]).padStart(2, "0")} ${MONTHS[date[1] - 1]} ${date[0]}` : String(value || "—");
}

/** The document's entities, once each, in name order. */
function linkedEntities(node: NodeDetail): LinkedEntity[] {
  const seen = new Map<string, LinkedEntity>();
  for (const rel of node.relationships || []) {
    const labels = rel.neighbor_labels || [];
    const uuid = rel.neighbor_props?.uuid as string | undefined;
    if (!uuid || labels.includes("Document") || seen.has(uuid)) continue;
    seen.set(uuid, { uuid, name: displayName(String(rel.neighbor_props?.name ?? ""), uuid, labels[0] || ""), label: labels[0] || "" });
  }
  return [...seen.values()].sort((a, b) => a.name.localeCompare(b.name));
}

const SORTS = [
  { field: "title", label: "Title" },
  { field: "doc_type", label: "Type" },
  { field: "date", label: "Date" },
] as const;

function SortHeader({ field, label, sortField, sortDir, onSort, className }: {
  field: string; label: string; sortField: string; sortDir: "asc" | "desc";
  onSort: (field: string) => void; className?: string;
}) {
  const active = sortField === field;
  const Arrow = active && sortDir === "desc" ? ArrowDown : ArrowUp;
  return (
    <TableHead aria-sort={active ? (sortDir === "asc" ? "ascending" : "descending") : undefined} className={className}>
      <Button variant="ghost" size="xs" className={cn("-ml-2 label-12", active ? "text-fg" : "text-fg-muted")} onClick={() => onSort(field)}>
        {label}
        {/* The hidden arrow keeps the column width when the sort moves. */}
        <Arrow aria-hidden className={cn("size-3.5", !active && "invisible")} strokeWidth={1.75} />
      </Button>
    </TableHead>
  );
}

export default function DocumentsPage() {
  return (
    <Suspense fallback={null}>
      <DocumentsTable />
    </Suspense>
  );
}

function DocumentsTable() {
  const router = useRouter();
  const entity = useSearchParams().get("entity") || "";
  const [searchQuery, setSearchQuery] = useState("");
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [sortField, setSortField] = useState<string>("title");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [page, setPage] = useState(0);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [result, setResult] = useState<{ key: string; data?: DocumentsPage; error?: string }>({ key: "" });
  const [actionError, setActionError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Record<string, ExpandedDoc>>({});
  const [reindexing, setReindexing] = useState<Set<number>>(new Set());
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [confirmBatch, setConfirmBatch] = useState(false);
  const [batchReindexing, setBatchReindexing] = useState(false);
  const paperlessBaseUrl = usePaperlessUrl();

  const requestKey = JSON.stringify([query, typeFilter, entity, page, sortField, sortDir, refreshVersion]);
  // A new request clears the selection and expansions of the rows it replaces.
  const [rowsKey, setRowsKey] = useState(requestKey);
  if (rowsKey !== requestKey) {
    setRowsKey(requestKey);
    setSelected(new Set());
    setExpanded({});
  }

  useEffect(() => {
    let active = true;
    getDocuments(query, typeFilter, page * PAGE_SIZE, PAGE_SIZE, sortField, sortDir, entity).then(
      (data: DocumentsPage) => {
        if (!active) return;
        setResult({ key: requestKey, data });
        const total = data.total || 0;
        if (page > 0 && page * PAGE_SIZE >= total) setPage(Math.max(0, Math.ceil(total / PAGE_SIZE) - 1));
      },
      // Keep the last rows, so Retry reloads them with a progress bar instead of skeletons.
      (e) => { if (active) setResult(prev => ({ key: requestKey, data: prev.data, error: errMsg(e, "Failed to load documents") })); },
    );
    return () => { active = false; };
  }, [requestKey, query, typeFilter, page, sortField, sortDir, entity]);

  const loading = result.key !== requestKey;
  const error = loading ? null : result.error || null;
  // Reloads keep the previous rows; only the first load has none to show.
  const data = result.data;
  const documents = data?.results || [];
  const total = data?.total || 0;
  const typeCounts = data?.doc_types || {};
  const filtered = Boolean(query || typeFilter || entity);
  const entityName = data?.entity?.name || (data && !loading ? "Unknown entity" : "Entity");

  const refresh = () => setRefreshVersion(version => version + 1);

  const submitSearch = () => {
    if (query === searchQuery.trim() && page === 0) refresh();
    else { setQuery(searchQuery.trim()); setPage(0); }
  };

  const clearFilters = () => {
    setSearchQuery(""); setQuery(""); setTypeFilter(""); setPage(0);
    if (entity) router.replace("/documents");
  };

  const toggleExpand = async (docId: number) => {
    const key = String(docId);
    if (expanded[key]) {
      setExpanded((prev) => { const next = { ...prev }; delete next[key]; return next; });
      return;
    }
    setExpanded((prev) => ({ ...prev, [key]: { entities: [], loading: true } }));
    try {
      const node = await getGraphNode(`doc-${docId}`);
      // A newer request may have cleared this expansion while the details loaded.
      setExpanded((prev) => prev[key] ? { ...prev, [key]: { entities: linkedEntities(node), loading: false } } : prev);
    } catch (e) {
      setExpanded((prev) => { if (!prev[key]) return prev; const next = { ...prev }; delete next[key]; return next; });
      setActionError(errMsg(e, "Failed to load document details"));
    }
  };

  const handleReindex = async (docId: number) => {
    setActionError(null);
    setReindexing((prev) => new Set([...prev, docId]));
    try {
      await postReindexDoc(docId);
      refresh();
    } catch (e) {
      setActionError(errMsg(e, "Reindex failed"));
    } finally {
      setReindexing((prev) => { const next = new Set(prev); next.delete(docId); return next; });
    }
  };

  const handleBatchReindex = async () => {
    setConfirmBatch(false);
    setActionError(null);
    setBatchReindexing(true);
    const failures: number[] = [];
    for (const docId of selected) {
      try {
        await postReindexDoc(docId);
      } catch {
        failures.push(docId);
      }
    }
    setBatchReindexing(false);
    setSelected(new Set());
    refresh();
    if (failures.length) setActionError(`Reindex failed for documents: ${failures.join(", ")}`);
  };

  const toggleSelect = (docId: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(docId)) next.delete(docId);
      else next.add(docId);
      return next;
    });
  };

  const toggleSelectAll = () => {
    if (selected.size === documents.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(documents.map((d) => d.properties?.paperless_id as number)));
    }
  };

  const totalPages = Math.ceil(total / PAGE_SIZE);

  const handleSort = (field: string) => {
    setPage(0);
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  };

  const reindexButton = (docId: number, className?: string) => (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          variant="ghost"
          size="icon-sm"
          className={className}
          onClick={() => handleReindex(docId)}
          aria-label={`Reindex document ${docId}`}
          disabled={reindexing.has(docId)}
        >
          {reindexing.has(docId) ? <Loader2 className="animate-spin" /> : <RefreshCw strokeWidth={1.75} />}
        </Button>
      </TooltipTrigger>
      <TooltipContent>Reindex</TooltipContent>
    </Tooltip>
  );

  return (
    <div className="flex flex-col h-full">
      <div className="px-4 pt-4 md:px-12 md:pt-10 space-y-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="page-title">Documents</h1>
            <p className="label-13 text-fg-muted mt-1 tabular-nums" aria-live="polite">
              {loading ? "Loading documents…" : error ? "Document results unavailable" : `${total} ${filtered ? "matching indexed documents" : "indexed documents"}`}
            </p>
          </div>
          {selected.size > 0 && (
            <Button onClick={() => setConfirmBatch(true)} disabled={batchReindexing || loading} size="sm" loading={batchReindexing}>
              <RefreshCw strokeWidth={1.75} />
              Reindex {selected.size}
            </Button>
          )}
        </div>

        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search aria-hidden className="absolute left-3 top-1/2 -translate-y-1/2 size-4 text-fg-muted" strokeWidth={1.75} />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && submitSearch()}
              placeholder="Search documents..."
              aria-label="Search indexed documents"
              className="pl-9"
            />
          </div>
          <Button onClick={submitSearch} disabled={loading} variant="secondary" size="icon" aria-label="Search documents">
            {loading ? <Loader2 className="animate-spin" /> : <Search strokeWidth={1.75} />}
          </Button>
        </div>

        {(entity || (!error && Object.keys(typeCounts).length > 0)) && (
          <div className="flex gap-2 overflow-x-auto max-md:-mx-4 max-md:px-4 md:flex-wrap" aria-label="Filters" role="group">
            {entity && (
              <Button variant="outline" size="xs" className="shrink-0 rounded-full max-md:min-h-11" onClick={() => router.replace("/documents")} aria-label={`Remove entity filter ${entityName}`}>
                <span className="text-fg-muted">Entity</span>
                <span className="max-w-48 truncate">{entityName}</span>
                <X aria-hidden strokeWidth={1.75} />
              </Button>
            )}
            {!error && Object.entries(typeCounts)
              .sort((a, b) => b[1] - a[1])
              .map(([type, count]) => (
                <Button
                  key={type}
                  variant="outline"
                  size="xs"
                  aria-pressed={typeFilter === type}
                  onClick={() => { setTypeFilter(typeFilter === type ? "" : type); setPage(0); }}
                  className="shrink-0 rounded-full px-2.5 max-md:min-h-11 aria-pressed:border-fg aria-pressed:bg-raised [&[aria-pressed=true]_[data-type]]:text-fg"
                >
                  <TypeDot type={type} className="label-12" />{" "}
                  <span className="label-12 text-fg-muted tabular-nums">{count}</span>
                </Button>
              ))}
            {typeFilter && (
              <Button variant="ghost" size="xs" className="shrink-0 max-md:min-h-11" onClick={() => { setTypeFilter(""); setPage(0); }}>
                Clear
              </Button>
            )}
          </div>
        )}

        {error && (
          <div role="alert" className="flex items-center justify-between gap-3 rounded-md border border-error-line bg-error-bg p-3 label-13 text-fg">
            <span>{error}</span><Button variant="outline" size="sm" onClick={refresh}>Retry loading</Button>
          </div>
        )}
        {actionError && <div role="alert" className="rounded-md border border-error-line bg-error-bg p-3 label-13 text-fg">{actionError}</div>}
      </div>

      <div className="flex flex-1 flex-col px-4 pb-4 pt-4 md:px-12 md:pb-10 min-h-0">
        {error ? null : (
          <Card className="relative flex h-full flex-col gap-0 overflow-hidden p-0 min-h-0">
            {loading && data && <Progress indeterminate aria-label="Loading documents" className="absolute inset-x-0 top-0 z-10 h-0.5 rounded-none" />}
            <div className="flex-1 overflow-auto min-h-0">
              <Table className="table-fixed">
                {/* Phones hide type, date and entities, so their actions cell fills the fourth column.
                    The last three columns are empty there; zero widths keep full-width rows from widening them. */}
                <colgroup>
                  <col className="w-11 md:w-10" />
                  <col className="w-11 md:w-9" />
                  <col />
                  <col className="w-15 md:w-40" />
                  <col className="w-0 md:w-32" />
                  <col className="w-0 md:w-20" />
                  <col className="w-0 md:w-20" />
                </colgroup>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="pl-3">
                      <Checkbox
                        checked={selected.size > 0 && selected.size === documents.length}
                        aria-label="Select documents on this page"
                        onCheckedChange={toggleSelectAll}
                        disabled={loading || !documents.length}
                      />
                    </TableHead>
                    <TableHead><span className="sr-only">Details</span></TableHead>
                    <SortHeader {...SORTS[0]} sortField={sortField} sortDir={sortDir} onSort={handleSort} />
                    <SortHeader {...SORTS[1]} sortField={sortField} sortDir={sortDir} onSort={handleSort} className="hidden md:table-cell" />
                    <SortHeader {...SORTS[2]} sortField={sortField} sortDir={sortDir} onSort={handleSort} className="hidden md:table-cell" />
                    <TableHead className="hidden md:table-cell text-right label-12">Entities</TableHead>
                    <TableHead><span className="sr-only">Actions</span></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {!data && loading && [...Array(8)].map((_, i) => (
                    <TableRow key={i} aria-hidden className="hover:bg-transparent">
                      <TableCell colSpan={7}><Skeleton className="h-4 w-full" /></TableCell>
                    </TableRow>
                  ))}
                  {documents.map((doc) => {
                    const p = doc.properties || {};
                    const docId = p.paperless_id as number;
                    const key = String(docId);
                    const detail = expanded[key];
                    const docType = (p.doc_type as string) || "unknown";
                    const title = (p.title as string) || `Document #${docId}`;
                    const detailId = `document-details-${docId}`;

                    return (
                      <Fragment key={key}>
                        <TableRow className="max-md:h-auto">
                          <TableCell className="pl-3">
                            <Checkbox
                              aria-label={`Select document ${docId}`}
                              checked={selected.has(docId)}
                              onCheckedChange={() => toggleSelect(docId)}
                              disabled={loading}
                            />
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon-xs"
                              aria-expanded={!!detail}
                              aria-controls={detail ? detailId : undefined}
                              aria-label={`Show details for ${title}`}
                              onClick={() => toggleExpand(docId)}
                            >
                              <ChevronRight aria-hidden strokeWidth={1.75} className={cn("text-fg-muted transition-transform duration-(--dur-3)", detail && "rotate-90")} />
                            </Button>
                          </TableCell>
                          <TableCell className="max-md:py-2">
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Link href={`/documents/${docId}`} className="doc-title text-[15px] block truncate hover:underline underline-offset-4">
                                  {title}
                                </Link>
                              </TooltipTrigger>
                              <TooltipContent>{title}</TooltipContent>
                            </Tooltip>
                            <div className="md:hidden flex items-center gap-2 label-12 text-fg-muted tabular-nums">
                              <TypeDot type={docType} className="label-12" />
                              <span aria-hidden>·</span>
                              <span>{formatDate(p.date)}</span>
                            </div>
                          </TableCell>
                          <TableCell className="hidden md:table-cell"><TypeDot type={docType} /></TableCell>
                          <TableCell className="hidden md:table-cell label-13 text-fg-muted">{formatDate(p.date)}</TableCell>
                          <TableCell className="hidden md:table-cell label-13 text-right">{doc.entity_count ?? "—"}</TableCell>
                          <TableCell>
                            <div className="flex justify-end">
                              <Button asChild variant="ghost" size="icon-sm" className="hidden md:inline-flex">
                                <a href={getPaperlessDocUrl(docId, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer" aria-label={`Open document ${docId} in Paperless`}>
                                  <ExternalLink strokeWidth={1.75} />
                                </a>
                              </Button>
                              {reindexButton(docId)}
                            </div>
                          </TableCell>
                        </TableRow>
                        {detail && (
                          <TableRow id={detailId} className="hover:bg-transparent">
                            <TableCell colSpan={7} className="bg-sunken p-0 whitespace-normal">
                              <div className="px-4 py-4 md:pl-[5.25rem] md:pr-6">
                                <dl className="grid grid-cols-2 gap-x-6 gap-y-3 md:grid-cols-[auto_minmax(0,2fr)_auto_auto]">
                                  {([
                                    ["Paperless ID", (
                                      <a key="id" href={getPaperlessDocUrl(docId, paperlessBaseUrl)} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 underline underline-offset-4">
                                        {docId}<ExternalLink aria-hidden className="size-3.5" strokeWidth={1.75} /><span className="sr-only">(opens Paperless)</span>
                                      </a>
                                    )],
                                    ["Title", <span key="title" className="doc-title text-[15px]">{title}</span>],
                                    ["Date", formatDate(p.date)],
                                    ["Type", <TypeDot key="type" type={docType} />],
                                  ] as const).map(([label, value]) => (
                                    <div key={label} className="min-w-0 space-y-0.5">
                                      <dt className="label-12 text-fg-muted">{label}</dt>
                                      <dd className="label-13 tabular-nums break-words">{value}</dd>
                                    </div>
                                  ))}
                                </dl>
                                <h2 className="label-12 text-fg-muted mt-4 mb-1.5">Entities</h2>
                                {detail.loading ? (
                                  <p className="label-13 text-fg-muted" role="status">Loading entities…</p>
                                ) : detail.entities.length ? (
                                  <ul className="flex flex-wrap gap-x-5 gap-y-2">
                                    {detail.entities.map((item) => (
                                      <li key={item.uuid} className="flex items-center gap-2 label-13">
                                        <Link href={`/graph?focus=${encodeURIComponent(item.uuid)}`} className="underline underline-offset-4">{item.name}</Link>
                                        <KindShape label={item.label} className="label-12 text-fg-muted" />
                                      </li>
                                    ))}
                                  </ul>
                                ) : (
                                  <p className="label-13 text-fg-muted">No entities were extracted from this document.</p>
                                )}
                              </div>
                            </TableCell>
                          </TableRow>
                        )}
                      </Fragment>
                    );
                  })}
                  {data && documents.length === 0 && (
                    <TableRow className="hover:bg-transparent">
                      <TableCell colSpan={7} className="py-12 text-center whitespace-normal">
                        <p className="copy-14 text-fg-muted">{filtered ? "No documents match these filters." : "No documents are indexed yet."}</p>
                        {filtered && <Button variant="outline" size="sm" className="mt-3" onClick={clearFilters}>Clear filters</Button>}
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>

            {totalPages > 1 && (
              <Pager page={page} pageCount={totalPages} pageSize={PAGE_SIZE} total={total} onPage={setPage} disabled={loading} />
            )}
          </Card>
        )}
      </div>

      <Dialog open={confirmBatch} onOpenChange={setConfirmBatch}>
        <DialogContent role="alertdialog">
          <DialogHeader>
            <DialogTitle>Reindex {selected.size} {selected.size === 1 ? "document" : "documents"}?</DialogTitle>
            <DialogDescription>
              Each document is read again from Paperless and its entities are extracted again. This can take several minutes.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <DialogClose asChild><Button variant="outline">Cancel</Button></DialogClose>
            <Button onClick={handleBatchReindex}>Reindex</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
