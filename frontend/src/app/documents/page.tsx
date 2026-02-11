"use client";

import { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { graphSearch, getGraphNode, postReindexDoc } from "@/lib/api";
import {
  Search,
  RefreshCw,
  Loader2,
  ExternalLink,
  ChevronDown,
  ChevronRight,
} from "lucide-react";

interface DocResult {
  labels: string[];
  properties: Record<string, unknown>;
}

interface ExpandedDoc {
  node: unknown;
  loading: boolean;
}

export default function DocumentsPage() {
  const [documents, setDocuments] = useState<DocResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [expanded, setExpanded] = useState<Record<string, ExpandedDoc>>({});
  const [reindexing, setReindexing] = useState<Set<number>>(new Set());
  const [docTypes, setDocTypes] = useState<string[]>([]);

  const fetchDocs = useCallback(async () => {
    setLoading(true);
    try {
      // Search for documents by querying with a broad term
      const data = await graphSearch(searchQuery || " ", "Document", 100);
      const docs = (data.results || []).filter((r: DocResult) =>
        r.labels?.includes("Document")
      );
      setDocuments(docs);

      // Extract unique doc types
      const types = [
        ...new Set(docs.map((d: DocResult) => d.properties?.doc_type as string).filter(Boolean)),
      ] as string[];
      setDocTypes(types.sort());
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
      setExpanded((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      return;
    }

    setExpanded((prev) => ({ ...prev, [key]: { node: null, loading: true } }));
    try {
      const nodeId = uuid || `doc-${docId}`;
      const node = await getGraphNode(nodeId);
      setExpanded((prev) => ({ ...prev, [key]: { node, loading: false } }));
    } catch {
      // If uuid-based lookup fails, try removing the expanded entry
      setExpanded((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
    }
  };

  const handleReindex = async (docId: number) => {
    setReindexing((prev) => new Set([...prev, docId]));
    try {
      await postReindexDoc(docId);
    } catch (e) {
      console.error("Reindex failed:", e);
    } finally {
      setReindexing((prev) => {
        const next = new Set(prev);
        next.delete(docId);
        return next;
      });
    }
  };

  const filtered = typeFilter
    ? documents.filter((d) => d.properties?.doc_type === typeFilter)
    : documents;

  return (
    <div className="space-y-4 p-8">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Documents</h1>
        <Badge variant="secondary">{filtered.length} documents</Badge>
      </div>

      <div className="flex gap-3">
        <div className="flex-1 flex gap-2">
          <Input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && fetchDocs()}
            placeholder="Search documents..."
          />
          <Button onClick={fetchDocs} disabled={loading}>
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Search className="h-4 w-4" />
            )}
          </Button>
        </div>
        <select
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
          className="rounded-md border bg-background px-3 py-2 text-sm"
        >
          <option value="">All types</option>
          {docTypes.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      <Card>
        <ScrollArea className="h-[calc(100vh-220px)]">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8"></TableHead>
                <TableHead>Title</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Date</TableHead>
                <TableHead>ID</TableHead>
                <TableHead className="w-24">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((doc) => {
                const p = doc.properties;
                const docId = p.paperless_id as number;
                const key = String(docId);
                const isExpanded = !!expanded[key];

                return (
                  <>
                    <TableRow
                      key={key}
                      className="cursor-pointer hover:bg-accent/50"
                      onClick={() => toggleExpand(docId, p.uuid as string)}
                    >
                      <TableCell>
                        {isExpanded ? (
                          <ChevronDown className="h-4 w-4" />
                        ) : (
                          <ChevronRight className="h-4 w-4" />
                        )}
                      </TableCell>
                      <TableCell className="font-medium max-w-md truncate">
                        {(p.title as string) || `Document #${docId}`}
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary" className="text-xs">
                          {(p.doc_type as string) || "unknown"}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground">
                        {(p.date as string) || "—"}
                      </TableCell>
                      <TableCell>
                        <a
                          href={`http://10.10.10.20:8000/documents/${docId}/`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="flex items-center gap-1 text-xs text-blue-500 hover:underline"
                          onClick={(e) => e.stopPropagation()}
                        >
                          #{docId} <ExternalLink className="h-3 w-3" />
                        </a>
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleReindex(docId);
                          }}
                          disabled={reindexing.has(docId)}
                        >
                          {reindexing.has(docId) ? (
                            <Loader2 className="h-3 w-3 animate-spin" />
                          ) : (
                            <RefreshCw className="h-3 w-3" />
                          )}
                        </Button>
                      </TableCell>
                    </TableRow>
                    {isExpanded && (
                      <TableRow key={`${key}-detail`}>
                        <TableCell colSpan={6} className="bg-muted/30">
                          {expanded[key]?.loading ? (
                            <div className="flex items-center gap-2 py-4 justify-center">
                              <Loader2 className="h-4 w-4 animate-spin" />
                              Loading details...
                            </div>
                          ) : expanded[key]?.node ? (
                            <div className="p-3">
                              <pre className="text-xs overflow-auto max-h-48 rounded bg-muted p-3">
                                {JSON.stringify(expanded[key].node, null, 2)}
                              </pre>
                            </div>
                          ) : (
                            <p className="py-2 text-sm text-muted-foreground">
                              No details available
                            </p>
                          )}
                        </TableCell>
                      </TableRow>
                    )}
                  </>
                );
              })}
              {filtered.length === 0 && !loading && (
                <TableRow>
                  <TableCell
                    colSpan={6}
                    className="text-center py-8 text-muted-foreground"
                  >
                    No documents found
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </ScrollArea>
      </Card>
    </div>
  );
}
