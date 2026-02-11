export interface StatusResponse {
  status: string;
  graph: {
    nodes: number;
    relationships: number;
    entities: number;
    documents: number;
  };
  embeddings: { document_chunks: number; entity_embeddings: number; docs_with_embeddings: number; };
  last_sync: string | null;
  active_tasks: Record<string, string | { status: string; type: string }>;
}

export interface TaskResponse {
  task_id: string;
  status: string;
  message: string;
}

export interface TaskStatus {
  status: string;
  result?: unknown;
  error?: string;
  started?: string;
}

export interface GraphNode {
  labels: string[];
  properties?: Record<string, unknown>;
  props?: Record<string, unknown>;
}

export interface GraphRelationship {
  start: string | null;
  end: string | null;
  type: string;
  props: Record<string, unknown>;
}

export interface NeighborsResponse {
  nodes: GraphNode[];
  relationships: GraphRelationship[];
}

export interface SearchResult {
  query: string;
  type: string | null;
  results: GraphNode[];
}

export interface QueryMessage {
  role: "user" | "assistant";
  content: string;
  sources?: unknown[];
  graph_context?: unknown;
  timestamp: number;
}

export function getNodeId(node: GraphNode): string {
  const p = node.properties || node.props || {};
  return (p.uuid as string) || (p.paperless_id as string) || (p.name as string) || "";
}

export function getNodeName(node: GraphNode): string {
  const p = node.properties || node.props || {};
  return (p.name as string) || (p.title as string) || getNodeId(node);
}

export function getNodeLabel(node: GraphNode): string {
  return node.labels?.[0] || "Unknown";
}
