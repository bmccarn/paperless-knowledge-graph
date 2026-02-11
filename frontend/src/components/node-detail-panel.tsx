"use client";

import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Collapsible, CollapsibleTrigger, CollapsibleContent } from "@/components/ui/collapsible";
import { getGraphNode } from "@/lib/api";
import {
  X,
  Loader2,
  ExternalLink,
  FileText,
  Users,
  ArrowRightLeft,
  Network,
  ChevronDown,
  ChevronRight,
} from "lucide-react";

const NODE_COLORS: Record<string, string> = {
  Person: "#60a5fa",
  Organization: "#34d399",
  Document: "#94a3b8",
  MedicalResult: "#f87171",
  Medical_Result: "#f87171",
  FinancialItem: "#fbbf24",
  Financial_Item: "#fbbf24",
  Address: "#22d3ee",
  Date: "#fb923c",
  Account: "#a78bfa",
};

function getColor(label: string): string {
  return NODE_COLORS[label] || "#c084fc";
}

interface NodeProps {
  id: string;
  name: string;
  label: string;
  props: Record<string, unknown>;
  color: string;
}

interface RelationshipData {
  rel_type: string;
  rel_props: Record<string, unknown>;
  neighbor_labels: string[];
  neighbor_props: Record<string, unknown>;
  direction?: string;
}

interface NodeDetail {
  labels: string[];
  properties: Record<string, unknown>;
  relationships: RelationshipData[];
}

interface NodeDetailPanelProps {
  node: NodeProps;
  onClose: () => void;
  onExpandNeighbors: (nodeId: string) => void;
}

function renderMarkdown(text: string) {
  return text.split("\n").map((line, i) => {
    const formatted = line
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/\*(.+?)\*/g, "<em>$1</em>")
      .replace(/`(.+?)`/g, '<code class="bg-muted px-1 rounded text-[11px]">$1</code>');
    return (
      <p
        key={i}
        className={line.trim() === "" ? "h-2" : ""}
        dangerouslySetInnerHTML={{ __html: formatted }}
      />
    );
  });
}

export function NodeDetailPanel({ node, onClose, onExpandNeighbors }: NodeDetailPanelProps) {
  const [detail, setDetail] = useState<NodeDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showRelationships, setShowRelationships] = useState(true);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setDetail(null);
    getGraphNode(node.id)
      .then(setDetail)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [node.id]);

  const relationships = detail?.relationships || [];
  const description =
    (detail?.properties?.description as string) ||
    (node.props.description as string) ||
    null;
  const aliases =
    (detail?.properties?.aliases as string[]) ||
    (node.props.aliases as string[]) ||
    [];

  const sourceDocuments: Array<{ paperless_id: number; title: string; doc_type: string; date: string }> = [];
  const connectedEntities: Array<{ name: string; label: string; uuid?: string }> = [];
  const seenDocs = new Set<number>();
  const seenEntities = new Set<string>();
  const incomingRels: RelationshipData[] = [];
  const outgoingRels: RelationshipData[] = [];

  for (const rel of relationships) {
    const neighborLabel = rel.neighbor_labels?.[0] || "Unknown";
    const neighborName = (rel.neighbor_props?.name as string) || (rel.neighbor_props?.title as string) || "Unknown";
    const neighborUuid = rel.neighbor_props?.uuid as string | undefined;
    const paperlessId = rel.neighbor_props?.paperless_id as number | undefined;

    if (neighborLabel === "Document" && paperlessId && !seenDocs.has(paperlessId)) {
      seenDocs.add(paperlessId);
      sourceDocuments.push({
        paperless_id: paperlessId,
        title: (rel.neighbor_props?.title as string) || `Document #${paperlessId}`,
        doc_type: (rel.neighbor_props?.doc_type as string) || "unknown",
        date: (rel.neighbor_props?.date as string) || "",
      });
    }

    if (neighborLabel !== "Document") {
      const key = neighborUuid || neighborName;
      if (!seenEntities.has(key)) {
        seenEntities.add(key);
        connectedEntities.push({ name: neighborName, label: neighborLabel, uuid: neighborUuid });
      }
    }

    if (rel.direction === "incoming" || rel.rel_props?.source_doc) {
      incomingRels.push(rel);
    } else {
      outgoingRels.push(rel);
    }
  }

  function groupByType(rels: RelationshipData[]) {
    const grouped: Record<string, RelationshipData[]> = {};
    for (const r of rels) {
      const key = r.rel_type;
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(r);
    }
    return grouped;
  }

  const paperlessUrl = (id: number) => `http://10.10.10.20:8000/documents/${id}/`;

  return (
    <ScrollArea className="flex-1">
      <div className="p-5 space-y-5">
        {/* Header */}
        <div className="flex items-start justify-between gap-3">
          <div className="space-y-2 flex-1 min-w-0">
            <Badge
              className="text-xs"
              style={{ backgroundColor: `${node.color}20`, color: node.color, borderColor: `${node.color}40` }}
            >
              {node.label}
            </Badge>
            <h2 className="font-semibold text-lg leading-tight break-words">{node.name}</h2>
            {aliases.length > 0 && (
              <p className="text-xs text-muted-foreground">
                Also known as: {aliases.join(", ")}
              </p>
            )}
          </div>
          <Button variant="ghost" size="icon" onClick={onClose} className="shrink-0 h-8 w-8 -mt-1 -mr-2">
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Loading */}
        {loading && (
          <div className="flex items-center gap-2 py-6 justify-center text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin text-primary" />
            <span className="text-xs">Loading details...</span>
          </div>
        )}
        {error && (
          <div className="rounded-lg bg-destructive/10 border border-destructive/20 p-3">
            <p className="text-xs text-destructive">Failed to load: {error}</p>
          </div>
        )}

        {/* Description */}
        {description && (
          <>
            <Separator />
            <div>
              <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wider mb-2">
                Description
              </h3>
              <div className="text-sm leading-relaxed space-y-1 bg-accent/30 rounded-lg p-3.5">
                {renderMarkdown(description)}
              </div>
            </div>
          </>
        )}

        {/* Properties */}
        {detail && (
          <>
            <Separator />
            <div>
              <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wider mb-2">
                Properties
              </h3>
              <div className="space-y-2">
                {Object.entries(detail.properties)
                  .filter(([key]) => !["description", "aliases", "uuid", "name", "title"].includes(key))
                  .map(([key, value]) => (
                    <div key={key} className="flex gap-3">
                      <span className="text-[10px] text-muted-foreground uppercase min-w-[70px] shrink-0 pt-0.5 tracking-wider">
                        {key}
                      </span>
                      <span className="text-xs break-all">
                        {typeof value === "object" ? JSON.stringify(value) : String(value)}
                      </span>
                    </div>
                  ))}
              </div>
            </div>
          </>
        )}

        {/* Source Documents */}
        {sourceDocuments.length > 0 && (
          <>
            <Separator />
            <div>
              <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wider mb-2 flex items-center gap-1.5">
                <FileText className="h-3.5 w-3.5" />
                Source Documents ({sourceDocuments.length})
              </h3>
              <div className="space-y-1.5">
                {sourceDocuments.map((doc) => (
                  <a
                    key={doc.paperless_id}
                    href={paperlessUrl(doc.paperless_id)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-start gap-2 rounded-lg border bg-card/50 px-3 py-2.5 text-xs hover:bg-accent/50 transition-colors group"
                  >
                    <ExternalLink className="h-3 w-3 shrink-0 mt-0.5 opacity-40 group-hover:opacity-100 transition-opacity" />
                    <div className="flex-1 min-w-0">
                      <p className="font-medium truncate">{doc.title}</p>
                      <div className="flex gap-2 mt-1 text-muted-foreground">
                        <Badge variant="outline" className="text-[9px] px-1.5 py-0">
                          {doc.doc_type}
                        </Badge>
                        {doc.date && <span>{doc.date}</span>}
                      </div>
                    </div>
                  </a>
                ))}
              </div>
            </div>
          </>
        )}

        {/* Relationships */}
        {relationships.length > 0 && (
          <>
            <Separator />
            <Collapsible open={showRelationships} onOpenChange={setShowRelationships}>
              <CollapsibleTrigger className="justify-between py-1">
                <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wider flex items-center gap-1.5">
                  <ArrowRightLeft className="h-3.5 w-3.5" />
                  Relationships ({relationships.length})
                </h3>
                {showRelationships ? (
                  <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                )}
              </CollapsibleTrigger>
              <CollapsibleContent className="mt-2">
                {incomingRels.length > 0 && (
                  <div className="mb-3">
                    <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1.5">Incoming</p>
                    {Object.entries(groupByType(incomingRels)).map(([type, rels]) => (
                      <div key={type} className="mb-2">
                        <Badge variant="outline" className="text-[10px] mb-1.5">← {type} ({rels.length})</Badge>
                        <div className="pl-2 space-y-0.5">
                          {rels.map((r, i) => (
                            <div key={i} className="flex items-center gap-1.5 text-xs py-0.5">
                              <span className="h-2 w-2 rounded-full shrink-0" style={{ backgroundColor: getColor(r.neighbor_labels?.[0] || "") }} />
                              <span className="truncate">
                                {(r.neighbor_props?.name as string) || (r.neighbor_props?.title as string) || `Doc #${r.rel_props?.source_doc}`}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                {outgoingRels.length > 0 && (
                  <div>
                    <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1.5">Outgoing</p>
                    {Object.entries(groupByType(outgoingRels)).map(([type, rels]) => (
                      <div key={type} className="mb-2">
                        <Badge variant="outline" className="text-[10px] mb-1.5">{type} → ({rels.length})</Badge>
                        <div className="pl-2 space-y-0.5">
                          {rels.map((r, i) => (
                            <div key={i} className="flex items-center gap-1.5 text-xs py-0.5">
                              <span className="h-2 w-2 rounded-full shrink-0" style={{ backgroundColor: getColor(r.neighbor_labels?.[0] || "") }} />
                              <span className="truncate">
                                {(r.neighbor_props?.name as string) || (r.neighbor_props?.title as string) || "Unknown"}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CollapsibleContent>
            </Collapsible>
          </>
        )}

        {/* Connected Entities */}
        {connectedEntities.length > 0 && (
          <>
            <Separator />
            <div>
              <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wider mb-2 flex items-center gap-1.5">
                <Users className="h-3.5 w-3.5" />
                Connected Entities ({connectedEntities.length})
              </h3>
              <div className="space-y-0.5">
                {connectedEntities.map((ent, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs rounded-md px-2 py-1.5 hover:bg-accent/40 transition-colors">
                    <span className="h-2.5 w-2.5 rounded-full shrink-0" style={{ backgroundColor: getColor(ent.label) }} />
                    <span className="flex-1 truncate">{ent.name}</span>
                    <Badge variant="secondary" className="text-[9px] px-1.5 py-0 shrink-0">{ent.label}</Badge>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}

        {/* Open in Paperless */}
        {(node.props.paperless_id != null || detail?.properties?.paperless_id != null) && (
          <>
            <Separator />
            <a
              href={paperlessUrl((detail?.properties?.paperless_id || node.props.paperless_id) as number)}
              target="_blank"
              rel="noopener noreferrer"
            >
              <Button variant="outline" size="sm" className="w-full text-xs gap-2">
                <ExternalLink className="h-3.5 w-3.5" />
                Open in Paperless
              </Button>
            </a>
          </>
        )}

        {/* Actions */}
        <Button
          size="sm"
          variant="secondary"
          className="w-full text-xs gap-2"
          onClick={() => onExpandNeighbors(node.id)}
        >
          <Network className="h-3.5 w-3.5" />
          Expand Neighbors
        </Button>
      </div>
    </ScrollArea>
  );
}
