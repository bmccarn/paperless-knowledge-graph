"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { FileText, Loader2, Network, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { getGraphNode } from "@/lib/api";
import {
  documentId,
  nodeFromPayload,
  relationshipDirection,
  relationshipSupport,
  sourceDocumentIds,
  type ExplorerNode,
} from "@/lib/graph-data";

interface RelationshipData {
  rel_type: string;
  rel_props: Record<string, unknown>;
  neighbor_labels: string[];
  neighbor_props: Record<string, unknown>;
  direction?: string;
}

interface NodeDetail {
  properties: Record<string, unknown>;
  relationships: RelationshipData[];
}

interface Props {
  node: ExplorerNode & { color: string };
  onClose: () => void;
  onExpandNeighbors: (nodeId: string) => void;
  expanding: boolean;
  onSelectNode: (node: ExplorerNode) => void;
}

export function NodeDetailPanel({
  node,
  onClose,
  onExpandNeighbors,
  expanding,
  onSelectNode,
}: Props) {
  const [state, setState] = useState<{
    id: string;
    detail: NodeDetail | null;
    error: string | null;
  }>({ id: "", detail: null, error: null });
  const [retry, setRetry] = useState(0);

  useEffect(() => {
    let active = true;
    getGraphNode(node.id)
      .then((detail) => {
        if (active) setState({ id: node.id, detail, error: null });
      })
      .catch((error) => {
        if (active)
          setState({
            id: node.id,
            detail: null,
            error: error instanceof Error ? error.message : "Request failed.",
          });
      });
    return () => {
      active = false;
    };
  }, [node.id, retry]);

  const detail = state.id === node.id ? state.detail : null;
  const error = state.id === node.id ? state.error : null;
  const loading = state.id !== node.id;
  const props = { ...node.props, ...detail?.properties };
  const relationships = (detail?.relationships || []).filter(
    (rel) => typeof rel?.rel_type === "string",
  );
  const description =
    typeof props.description === "string" ? props.description : "";
  const aliases = Array.isArray(props.aliases)
    ? props.aliases.filter(
        (value): value is string => typeof value === "string",
      )
    : [];
  const ownDocumentId = documentId(props.paperless_id);
  // A neighboring document is a connection, not necessarily evidence for every edge.
  const sourceIds = [
    ...new Set(
      relationships.flatMap((rel) => sourceDocumentIds(rel.rel_props || {})),
    ),
  ].sort((a, b) => a - b);

  return (
    <div className="space-y-5 p-5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 space-y-2">
          <Badge variant="outline" style={{ color: node.color }}>
            {node.label}
          </Badge>
          <h2 className="break-words text-lg font-semibold leading-tight">
            {node.name}
          </h2>
          {!!aliases.length && (
            <div className="flex flex-wrap gap-1">
              {aliases.map((alias, index) => (
                <Badge key={index} variant="secondary" className="text-[10px]">
                  {alias}
                </Badge>
              ))}
            </div>
          )}
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={onClose}
          aria-label="Close node inspector"
          className="shrink-0"
        >
          <X className="h-4 w-4" />
        </Button>
      </div>
      <Button
        variant="secondary"
        size="sm"
        className="w-full"
        onClick={() => onExpandNeighbors(node.id)}
        disabled={expanding}
      >
        {expanding ? (
          <Loader2 className="h-4 w-4 animate-spin" />
        ) : (
          <Network className="h-4 w-4" />
        )}
        {expanding ? "Expanding…" : "Load neighbors into graph"}
      </Button>
      {ownDocumentId !== null && (
        <Link
          href={`/documents/${ownDocumentId}`}
          className="flex items-center gap-2 rounded-lg border p-3 text-sm hover:bg-accent"
        >
          <FileText className="h-4 w-4" />
          Inspect document #{ownDocumentId}
        </Link>
      )}
      {loading && (
        <p
          role="status"
          className="flex items-center gap-2 text-xs text-muted-foreground"
        >
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading recorded details…
        </p>
      )}
      {error && (
        <div
          role="alert"
          className="rounded border border-destructive/30 p-3 text-xs"
        >
          <p>Could not load details: {error}</p>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => {
              setState({ id: "", detail: null, error: null });
              setRetry((value) => value + 1);
            }}
          >
            Retry details
          </Button>
        </div>
      )}
      {description && (
        <section>
          <h3 className="mb-2 text-xs font-medium">Recorded description</h3>
          <p className="whitespace-pre-wrap break-words rounded-lg bg-background p-3 text-sm leading-relaxed">
            {description}
          </p>
          <p className="mt-2 text-[11px] text-muted-foreground">
            Extracted descriptions may contain errors. Check the source
            document.
          </p>
        </section>
      )}
      <section>
        <h3 className="mb-2 text-xs font-medium">
          Recorded relationship sources
        </h3>
        {sourceIds.map((id) => (
          <Link
            key={id}
            href={`/documents/${id}`}
            className="mb-2 flex items-center gap-2 rounded border p-2.5 text-xs hover:bg-accent"
          >
            <FileText className="h-3.5 w-3.5" />
            Document #{id}
          </Link>
        ))}
        {!loading && !error && !sourceIds.length && (
          <p className="text-xs text-muted-foreground">
            No source references are recorded on the returned relationships.
          </p>
        )}
      </section>
      <section>
        <h3 className="mb-3 text-xs font-medium">
          Returned relationships ({relationships.length})
        </h3>
        <p className="mb-3 text-[11px] leading-relaxed text-muted-foreground">
          Arrows follow the stored direction. Inferred connections are model
          deductions; source references do not independently verify them.
        </p>
        {relationships.map((rel, index) => {
          const neighbor = nodeFromPayload({
            labels: rel.neighbor_labels,
            props: rel.neighbor_props,
          });
          const direction = relationshipDirection(rel.direction);
          const otherName =
            neighbor?.name ||
            String(
              rel.neighbor_props?.name ||
                rel.neighbor_props?.title ||
                "Unknown endpoint",
            );
          const sources = sourceDocumentIds(rel.rel_props || {});
          const support = relationshipSupport(rel.rel_props || {});
          return (
            <div
              key={index}
              className="mb-3 space-y-2 rounded-lg border bg-background/50 p-3 text-xs"
            >
              <div className="flex flex-wrap items-center gap-1.5">
                <Badge variant="outline" className="text-[10px]">
                  {direction === "incoming"
                    ? "Incoming"
                    : direction === "outgoing"
                      ? "Outgoing"
                      : "Direction unknown"}
                </Badge>
                {rel.rel_props?.implied || rel.rel_props?.inferred ? (
                  <Badge
                    variant="outline"
                    className="text-[10px] text-amber-300"
                  >
                    Inferred
                  </Badge>
                ) : null}
              </div>
              <p className="break-words font-medium">
                {direction === "incoming"
                  ? `${otherName} → ${node.name}`
                  : direction === "outgoing"
                    ? `${node.name} → ${otherName}`
                    : `${node.name} ↔ ${otherName}`}
              </p>
              <p className="break-words text-muted-foreground">
                {rel.rel_type.replaceAll("_", " ")}
              </p>
              {neighbor && (
                <button
                  onClick={() => onSelectNode(neighbor)}
                  className="text-blue-400 underline underline-offset-2"
                >
                  Inspect {otherName}
                </button>
              )}
              <div className="flex flex-wrap gap-2">
                {sources.map((id) => (
                  <Link
                    key={id}
                    href={`/documents/${id}`}
                    className="text-blue-400 underline underline-offset-2"
                  >
                    Source #{id}
                  </Link>
                ))}
                {!sources.length && (
                  <span className="text-muted-foreground">
                    No source reference recorded
                  </span>
                )}
              </div>
              {support.map((item) => (
                <details key={item.documentId} className="rounded border p-2">
                  <summary className="cursor-pointer">Evidence from document #{item.documentId}{item.inferred ? " · inferred" : ""}</summary>
                  {item.quotes.map((quote, index) => (
                    <blockquote key={index} className="mt-2 whitespace-pre-wrap break-words border-l-2 pl-2 text-muted-foreground">{quote}</blockquote>
                  ))}
                  {item.rationale.map((reason, index) => (
                    <p key={index} className="mt-2 whitespace-pre-wrap break-words">Extraction rationale: {reason}</p>
                  ))}
                  {!item.quotes.length && <p className="mt-2 text-muted-foreground">No source quote recorded.</p>}
                </details>
              ))}
            </div>
          );
        })}
        {!loading && !error && !relationships.length && (
          <p className="text-xs text-muted-foreground">
            No relationships returned for this node.
          </p>
        )}
      </section>
      <details className="border-t pt-4 text-xs">
        <summary className="cursor-pointer text-muted-foreground">
          All recorded properties
        </summary>
        <pre className="mt-2 whitespace-pre-wrap break-all rounded bg-background p-3">
          {JSON.stringify(props, null, 2)}
        </pre>
      </details>
    </div>
  );
}
