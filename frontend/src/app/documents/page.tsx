"use client";

import { useState, useEffect, useCallback } from "react";
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
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { graphSearch, getGraphNode, postReindexDoc } from "@/lib/api";
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
  const [documents, setDocuments] = useState<DocResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [expanded, setExpanded] = useState<Record<string, ExpandedDoc>>({});
  const [reindexing, setReindexing] = useState<Set<number>>(new Set());
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [batchReindexing, setBatchReindexing] = useState(false);
  const [docTypes, setDocTypes] = useState<string[]>([]);
  const [sortField, setSortField] = useState<string>("title");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [page, setPage] = useState(0);

  const fetchDocs = useCallback(async () => {
    setLoading(true);
    try {
      const data = await graphSearch(searchQuery || " ", "Document", 200);
      const docs = (data.results || []).filter((r: DocResult) =>
        r.labels?.includes("Document")
      );
      setDocuments(docs);
      const types = [
        ...new Set(docs.map((d: DocResult) => d.properties?.doc_type as string).filter(Boolean)),
      ] as string[];
      setDocTypes(types.sort());
      setPage(0);
    } catch (e) {
      console.error("Failed to load documents:", e);
    } finally {
      setLoading(false);
    }
  }, [searchQuery]);

  useEffect(() => {
    fetchDocs();
  }, [fetchDocs]);

  const toggleExpand = async (docId: number, uuid?: string) => {
    const key = String(docId);
    if (expanded[key]) {
      setExpanded((prev) => { const next = { ...prev }; delete next[key]; return next; });
      return;
    }
    setExpanded((prev) => ({ ...prev, [key]: { node: null, loading: true } }));
    try {
      const nodeId = uuid || `doc-${docId}`;
      const node = await getGraphNode(nodeId);
      setExpanded((prev) => ({ ...prev, [key]: { node, loading: false } }));
    } catch {
      setExpanded((prev) => { const next = { ...prev }; delete next[key]; return next; });
    }
  };

  const handleReindex = async (docId: number) => {
    setReindexing((prev) => new Set([...prev, docId]));
    try {
      await postReindexDoc(docId);
    } catch (e) {
      console.error("Reindex failed:", e);
    } finally {
      setReindexing((prev) => { const next = new Set(prev); next.delete(docId); return next; });
    }
  };

  const handleBatchReindex = async () => {
    setBatchReindexing(true);
    for (const docId of selected) {
      try {
        await postReindexDoc(docId);
      } catch (e) {
        console.error(`Reindex failed for ${docId}:`, e);
      }
    }
    setBatchReindexing(false);
    setSelected(new Set());
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
    if (selected.size === filtered.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filtered.map((d) => d.properties.paperless_id as number)));
    }
  };

  // Filter and sort
  const filtered = documents
    .filter((d) => !typeFilter || d.properties?.doc_type === typeFilter)
    .sort((a, b) => {
      const aVal = (a.properties?.[sortField] as string) || "";
      const bVal = (b.properties?.[sortField] as string) || "";
      return sortDir === "asc" ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
    });

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const paginated = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const handleSort = (field: string) => {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  };

  // Doc type stats
  const typeCounts: Record<string, number> = {};
  documents.forEach((d) => {
    const t = (d.properties?.doc_type as string) || "unknown";
    typeCounts[t] = (typeCounts[t] || 0) + 1;
  });

  return (
    <div className="flex flex-col h-full">
      <div className="p-6 pb-0 space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Documents</h1>
            <p className="text-sm text-muted-foreground mt-0.5">
              {documents.length} documents indexed
            </p>
          </div>
          {selected.size > 0 && (
            <Button
              onClick={handleBatchReindex}
              disabled={batchReindexing}
              size="sm"
              className="gap-2"
            >
              {batchReindexing ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <RefreshCw className="h-3.5 w-3.5" />
              )}
              Reindex {selected.size} selected
            </Button>
          )}
        </div>

        {/* Stats bar */}
        {!loading && documents.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {Object.entries(typeCounts)
              .sort((a, b) => b[1] - a[1])
              .map(([type, count]) => (
                <button
                  key={type}
                  onClick={() => setTypeFilter(typeFilter === type ? "" : type)}
                  className="transition-all"
                >
                  <Badge
                    variant="outline"
                    className={`text-xs gap-1.5 cursor-pointer transition-all ${
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
              <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={() => setTypeFilter("")}>
                Clear filter
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
              onKeyDown={(e) => e.key === "Enter" && fetchDocs()}
              placeholder="Search documents..."
              className="pl-9"
            />
          </div>
          <Button onClick={fetchDocs} disabled={loading} variant="secondary">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
          </Button>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 px-6 pb-6 pt-4 min-h-0">
        {loading && documents.length === 0 ? (
          <div className="space-y-2">
            {[...Array(8)].map((_, i) => (
              <Skeleton key={i} className="h-12 rounded-lg" />
            ))}
          </div>
        ) : (
          <Card className="h-full flex flex-col border-border/50">
            <ScrollArea className="flex-1">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="w-10">
                      <Checkbox
                        checked={selected.size > 0 && selected.size === filtered.length}
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
                      <>
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
                              {(p.title as string) || `Document #${docId}`}
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
                              href={`http://your-paperless-host:8000/documents/${docId}/`}
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
                      </>
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
            </ScrollArea>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between border-t px-4 py-2">
                <p className="text-xs text-muted-foreground">
                  {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, filtered.length)} of {filtered.length}
                </p>
                <div className="flex items-center gap-1">
                  <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage(0)} disabled={page === 0}>
                    <ChevronsLeft className="h-3.5 w-3.5" />
                  </Button>
                  <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage((p) => p - 1)} disabled={page === 0}>
                    <ChevronLeft className="h-3.5 w-3.5" />
                  </Button>
                  <span className="text-xs text-muted-foreground px-2">
                    {page + 1} / {totalPages}
                  </span>
                  <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage((p) => p + 1)} disabled={page >= totalPages - 1}>
                    <ChevronRight className="h-3.5 w-3.5" />
                  </Button>
                  <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage(totalPages - 1)} disabled={page >= totalPages - 1}>
                    <ChevronsRight className="h-3.5 w-3.5" />
                  </Button>
                </div>
              </div>
            )}
          </Card>
        )}
      </div>
    </div>
  );
}
