"use client";

import { Fragment, useState, useEffect, useCallback, useRef } from "react";
import Link from "next/link";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { getDocuments, getGraphNode, postReindexDoc, getConfig } from "@/lib/api";
import {
  Search,
  RefreshCw,
  Loader2,
  ExternalLink,
  ChevronDown,
  ChevronRight,
  FileText,
  ArrowUpDown,
  ChevronLeft,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";

interface DocResult {
  labels: string[];
  properties: Record<string, unknown>;
}

interface ExpandedDoc {
  node: unknown;
  loading: boolean;
}

const DOC_TYPE_COLORS: Record<string, string> = {
  invoice: "bg-amber-500/15 text-amber-400 border-amber-500/20",
  receipt: "bg-green-500/15 text-green-400 border-green-500/20",
  letter: "bg-blue-500/15 text-blue-400 border-blue-500/20",
  contract: "bg-violet-500/15 text-violet-400 border-violet-500/20",
  medical: "bg-red-500/15 text-red-400 border-red-500/20",
  financial: "bg-emerald-500/15 text-emerald-400 border-emerald-500/20",
  statement: "bg-cyan-500/15 text-cyan-400 border-cyan-500/20",
  tax: "bg-orange-500/15 text-orange-400 border-orange-500/20",
};

function getDocTypeClass(type: string): string {
  const lower = type?.toLowerCase() || "";
  for (const [key, cls] of Object.entries(DOC_TYPE_COLORS)) {
    if (lower.includes(key)) return cls;
  }
  return "bg-muted text-muted-foreground";
}

const PAGE_SIZE = 25;

export default function DocumentsPage() {
  const [paperlessBaseUrl, setPaperlessBaseUrl] = useState("");
  const [documents, setDocuments] = useState<DocResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [query, setQuery] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [total, setTotal] = useState(0);
  const [typeCounts, setTypeCounts] = useState<Record<string, number>>({});
  const [typeFilter, setTypeFilter] = useState("");
  const [expanded, setExpanded] = useState<Record<string, ExpandedDoc>>({});
  const [reindexing, setReindexing] = useState<Set<number>>(new Set());
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [batchReindexing, setBatchReindexing] = useState(false);
  const [sortField, setSortField] = useState<string>("title");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [page, setPage] = useState(0);
  const requestVersion = useRef(0);

  const fetchDocs = useCallback(async () => {
    const version = ++requestVersion.current;
    setLoading(true);
    setError(null);
    setDocuments([]);
    setSelected(new Set());
    setExpanded({});
    try {
      const data = await getDocuments(query, typeFilter, page * PAGE_SIZE, PAGE_SIZE, sortField, sortDir);
      if (version !== requestVersion.current) return;
      setDocuments(data.results || []);
      setTotal(data.total || 0);
      setTypeCounts(data.doc_types || {});
      if (page > 0 && page * PAGE_SIZE >= data.total) setPage(Math.max(0, Math.ceil(data.total / PAGE_SIZE) - 1));
    } catch (e) {
      if (version === requestVersion.current) setError(e instanceof Error ? e.message : "Failed to load documents");
    } finally {
      if (version === requestVersion.current) setLoading(false);
    }
  }, [query, typeFilter, page, sortField, sortDir]);

  useEffect(() => { getConfig().then(c => setPaperlessBaseUrl(c.paperless_url)).catch(() => {}); }, []);

  const invalidateRequests = useCallback(() => { requestVersion.current++; }, []);
  useEffect(() => {
    void fetchDocs();
    return invalidateRequests;
  }, [fetchDocs, invalidateRequests]);

  const submitSearch = () => {
    if (query === searchQuery.trim() && page === 0) void fetchDocs();
    else { setQuery(searchQuery.trim()); setPage(0); }
  };

  const toggleExpand = async (docId: number, uuid?: string) => {
    const version = requestVersion.current;
    const key = String(docId);
    if (expanded[key]) {
      setExpanded((prev) => { const next = { ...prev }; delete next[key]; return next; });
      return;
    }
    setExpanded((prev) => ({ ...prev, [key]: { node: null, loading: true } }));
    try {
      const nodeId = uuid || `doc-${docId}`;
      const node = await getGraphNode(nodeId);
      if (version !== requestVersion.current) return;
      setExpanded((prev) => ({ ...prev, [key]: { node, loading: false } }));
    } catch (e) {
      if (version !== requestVersion.current) return;
      setError(e instanceof Error ? e.message : "Failed to load document details");
      setExpanded((prev) => { const next = { ...prev }; delete next[key]; return next; });
    }
  };

  const handleReindex = async (docId: number) => {
    setReindexing((prev) => new Set([...prev, docId]));
    try {
      await postReindexDoc(docId);
      await fetchDocs();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reindex failed");
    } finally {
      setReindexing((prev) => { const next = new Set(prev); next.delete(docId); return next; });
    }
  };

  const handleBatchReindex = async () => {
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
    await fetchDocs();
    if (failures.length) setError(`Reindex failed for documents: ${failures.join(", ")}`);
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
      setSelected(new Set(documents.map((d) => d.properties.paperless_id as number)));
    }
  };

  const totalPages = Math.ceil(total / PAGE_SIZE);
  const paginated = documents;

  const handleSort = (field: string) => {
    setPage(0);
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="p-4 md:p-6 pb-0 space-y-3 md:space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between gap-2">
          <div>
            <h1 className="text-xl md:text-2xl font-bold tracking-tight">Documents</h1>
            <p className="text-sm text-muted-foreground mt-0.5">
              {total} {query || typeFilter ? "matching indexed documents" : "indexed documents"}
            </p>
          </div>
          {selected.size > 0 && (
            <Button
              onClick={handleBatchReindex}
              disabled={batchReindexing || loading}
              size="sm"
              className="gap-2"
            >
              {batchReindexing ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <RefreshCw className="h-3.5 w-3.5" />
              )}
              <span className="hidden sm:inline">Reindex</span> {selected.size}
            </Button>
          )}
        </div>

        {/* Stats bar */}
        {!loading && Object.keys(typeCounts).length > 0 && (
          <div className="flex flex-wrap gap-1.5 md:gap-2 overflow-x-auto">
            {Object.entries(typeCounts)
              .sort((a, b) => b[1] - a[1])
              .map(([type, count]) => (
                <button
                  key={type}
                  onClick={() => { setTypeFilter(typeFilter === type ? "" : type); setPage(0); }}
                  className="transition-all"
                >
                  <Badge
                    variant="outline"
                    className={`text-xs gap-1.5 cursor-pointer transition-all whitespace-nowrap ${
                      typeFilter === type ? "ring-1 ring-primary" : "hover:bg-accent"
                    } ${getDocTypeClass(type)}`}
                  >
                    <FileText className="h-3 w-3" />
                    {type}
                    <span className="font-mono">{count}</span>
                  </Badge>
                </button>
              ))}
            {typeFilter && (
              <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={() => { setTypeFilter(""); setPage(0); }}>
                Clear
              </Button>
            )}
          </div>
        )}

        {/* Search */}
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && submitSearch()}
              placeholder="Search documents..."
              aria-label="Search indexed documents"
              className="pl-9"
            />
          </div>
          <Button onClick={submitSearch} disabled={loading} variant="secondary" className="min-w-[44px]" aria-label="Search documents">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
          </Button>
        </div>
        {error && (
          <div role="alert" className="flex items-center justify-between gap-3 rounded border border-destructive/40 p-3 text-sm">
            <span>{error}</span><Button variant="outline" size="sm" onClick={() => void fetchDocs()}>Retry loading</Button>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="flex flex-1 flex-col px-4 md:px-6 pb-4 md:pb-6 pt-3 md:pt-4 min-h-0">
        {loading && documents.length === 0 ? (
          <div className="space-y-2">
            {[...Array(8)].map((_, i) => (
              <Skeleton key={i} className="h-12 rounded-lg" />
            ))}
          </div>
        ) : (
          <>
            {/* Mobile card layout */}
            <div className="md:hidden min-h-0 flex-1 space-y-2 overflow-y-auto">
              {paginated.map((doc) => {
                const p = doc.properties;
                const docId = p.paperless_id as number;
                const docType = (p.doc_type as string) || "unknown";
                return (
                  <Card key={String(docId)} className="border-border/50">
                    <CardContent className="p-3">
                      <div className="flex items-start gap-3">
                        <Checkbox
                          checked={selected.has(docId)}
                          onCheckedChange={() => toggleSelect(docId)}
                          className="mt-1"
                        />
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-sm truncate">
                            <Link href={`/documents/${docId}`} className="hover:underline">
                              {(p.title as string) || `Document #${docId}`}
                            </Link>
                          </p>
                          <div className="flex items-center gap-2 mt-1.5 flex-wrap">
                            <Badge
                              variant="outline"
                              className={`text-[10px] ${getDocTypeClass(docType)}`}
                            >
                              {docType}
                            </Badge>
                            <span className="text-xs text-muted-foreground">
                              {(p.date as string) || "—"}
                            </span>
                            <a
                              href={`${paperlessBaseUrl}/documents/${docId}/`}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="flex items-center gap-1 text-xs text-primary ml-auto"
                            >
                              #{docId} <ExternalLink className="h-2.5 w-2.5" />
                            </a>
                          </div>
                        </div>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-9 w-9 shrink-0"
                          onClick={() => handleReindex(docId)}
                          disabled={reindexing.has(docId)}
                        >
                          {reindexing.has(docId) ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          ) : (
                            <RefreshCw className="h-3.5 w-3.5" />
                          )}
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
              {paginated.length === 0 && !loading && (
                <div className="text-center py-12 text-muted-foreground">
                  <FileText className="h-8 w-8 mx-auto mb-2 opacity-20" />
                  <p>No documents found</p>
                </div>
              )}
            </div>

            {/* Desktop table layout */}
            <Card className="hidden md:flex h-full flex-col border-border/50 min-h-0">
              <div className="flex-1 overflow-auto min-h-0">
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead className="w-10">
                        <Checkbox
                          checked={selected.size > 0 && selected.size === documents.length}
                          aria-label="Select documents on this page"
                          onCheckedChange={toggleSelectAll}
                          className="h-3.5 w-3.5"
                        />
                      </TableHead>
                      <TableHead className="w-8" />
                      <TableHead>
                        <button
                          className="flex items-center gap-1 hover:text-foreground transition-colors"
                          onClick={() => handleSort("title")}
                        >
                          Title
                          <ArrowUpDown className="h-3 w-3" />
                        </button>
                      </TableHead>
                      <TableHead>
                        <button
                          className="flex items-center gap-1 hover:text-foreground transition-colors"
                          onClick={() => handleSort("doc_type")}
                        >
                          Type
                          <ArrowUpDown className="h-3 w-3" />
                        </button>
                      </TableHead>
                      <TableHead>
                        <button
                          className="flex items-center gap-1 hover:text-foreground transition-colors"
                          onClick={() => handleSort("date")}
                        >
                          Date
                          <ArrowUpDown className="h-3 w-3" />
                        </button>
                      </TableHead>
                      <TableHead className="w-16">ID</TableHead>
                      <TableHead className="w-16" />
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {paginated.map((doc) => {
                      const p = doc.properties;
                      const docId = p.paperless_id as number;
                      const key = String(docId);
                      const isExpanded = !!expanded[key];
                      const docType = (p.doc_type as string) || "unknown";

                      return (
                        <Fragment key={key}>
                          <TableRow
                            key={key}
                            className="cursor-pointer group"
                            onClick={() => toggleExpand(docId, p.uuid as string)}
                          >
                            <TableCell onClick={(e) => e.stopPropagation()}>
                              <Checkbox
                                checked={selected.has(docId)}
                                onCheckedChange={() => toggleSelect(docId)}
                                className="h-3.5 w-3.5"
                              />
                            </TableCell>
                            <TableCell>
                              {isExpanded ? (
                                <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                              ) : (
                                <ChevronRight className="h-3.5 w-3.5 text-muted-foreground group-hover:text-foreground transition-colors" />
                              )}
                            </TableCell>
                            <TableCell className="font-medium max-w-md">
                              <span className="truncate block">
                                <Link
                                  href={`/documents/${docId}`}
                                  className="hover:underline"
                                  onClick={(e) => e.stopPropagation()}
                                >
                                  {(p.title as string) || `Document #${docId}`}
                                </Link>
                              </span>
                            </TableCell>
                            <TableCell>
                              <Badge
                                variant="outline"
                                className={`text-[10px] ${getDocTypeClass(docType)}`}
                              >
                                {docType}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground whitespace-nowrap">
                              {(p.date as string) || "—"}
                            </TableCell>
                            <TableCell>
                              <a
                                href={`${paperlessBaseUrl}/documents/${docId}/`}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex items-center gap-1 text-xs text-primary hover:underline"
                                onClick={(e) => e.stopPropagation()}
                              >
                                #{docId}
                                <ExternalLink className="h-2.5 w-2.5" />
                              </a>
                            </TableCell>
                            <TableCell>
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7"
                                    onClick={(e) => { e.stopPropagation(); handleReindex(docId); }}
                                    disabled={reindexing.has(docId)}
                                  >
                                    {reindexing.has(docId) ? (
                                      <Loader2 className="h-3 w-3 animate-spin" />
                                    ) : (
                                      <RefreshCw className="h-3 w-3" />
                                    )}
                                  </Button>
                                </TooltipTrigger>
                                <TooltipContent>Reindex</TooltipContent>
                              </Tooltip>
                            </TableCell>
                          </TableRow>
                          {isExpanded && (
                            <TableRow key={`${key}-detail`}>
                              <TableCell colSpan={7} className="bg-accent/20 p-0">
                                <div className="p-4">
                                  {expanded[key]?.loading ? (
                                    <div className="flex items-center gap-2 py-4 justify-center">
                                      <Loader2 className="h-4 w-4 animate-spin text-primary" />
                                      <span className="text-sm text-muted-foreground">Loading details...</span>
                                    </div>
                                  ) : expanded[key]?.node ? (
                                    <div className="grid gap-3 md:grid-cols-2">
                                      {Object.entries(
                                        (expanded[key].node as { properties?: Record<string, unknown> })?.properties || {}
                                      )
                                        .filter(([k]) => !["uuid"].includes(k))
                                        .map(([k, v]) => (
                                          <div key={k} className="space-y-0.5">
                                            <p className="text-[10px] uppercase text-muted-foreground tracking-wider">{k}</p>
                                            <p className="text-sm break-all">
                                              {typeof v === "object" ? JSON.stringify(v) : String(v || "—")}
                                            </p>
                                          </div>
                                        ))}
                                    </div>
                                  ) : (
                                    <p className="text-sm text-muted-foreground text-center py-2">No details available</p>
                                  )}
                                </div>
                              </TableCell>
                            </TableRow>
                          )}
                        </Fragment>
                      );
                    })}
                    {paginated.length === 0 && !loading && (
                      <TableRow>
                        <TableCell colSpan={7} className="text-center py-12 text-muted-foreground">
                          <FileText className="h-8 w-8 mx-auto mb-2 opacity-20" />
                          <p>No documents found</p>
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between border-t px-4 py-2">
                  <p className="text-xs text-muted-foreground">
                    {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, total)} of {total}
                  </p>
                  <div className="flex items-center gap-1">
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage(0)} aria-label="First page" disabled={loading || page === 0}>
                      <ChevronsLeft className="h-3.5 w-3.5" />
                    </Button>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage((p) => p - 1)} aria-label="Previous page" disabled={loading || page === 0}>
                      <ChevronLeft className="h-3.5 w-3.5" />
                    </Button>
                    <span className="text-xs text-muted-foreground px-2">
                      {page + 1} / {totalPages}
                    </span>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage((p) => p + 1)} aria-label="Next page" disabled={loading || page >= totalPages - 1}>
                      <ChevronRight className="h-3.5 w-3.5" />
                    </Button>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage(totalPages - 1)} aria-label="Last page" disabled={loading || page >= totalPages - 1}>
                      <ChevronsRight className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </div>
              )}
            </Card>

            {/* Mobile pagination */}
            {totalPages > 1 && (
              <div className="md:hidden flex items-center justify-between pt-3">
                <p className="text-xs text-muted-foreground">
                  {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, total)} of {total}
                </p>
                <div className="flex items-center gap-1">
                  <Button variant="ghost" size="icon" className="h-9 w-9" onClick={() => setPage((p) => p - 1)} aria-label="Previous page" disabled={loading || page === 0}>
                    <ChevronLeft className="h-4 w-4" />
                  </Button>
                  <span className="text-xs text-muted-foreground px-2">
                    {page + 1}/{totalPages}
                  </span>
                  <Button variant="ghost" size="icon" className="h-9 w-9" onClick={() => setPage((p) => p + 1)} aria-label="Next page" disabled={loading || page >= totalPages - 1}>
                    <ChevronRight className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
