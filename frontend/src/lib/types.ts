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
  freshness?: FreshnessStatus;
}

export interface FreshnessStatus {
  stale: boolean;
  paperless_documents: number;
  indexed_documents: number;
  docs_with_embeddings?: number;
  hashed_documents?: number;
  missing_documents: number;
  extra_documents?: number;
  missing_embedding_documents?: number;
  extra_embedding_documents?: number;
  missing_hash_documents?: number;
  extra_hash_documents?: number;
  modified_after_last_sync_documents?: number;
  changed_since_index_documents?: number;
  exact_id_check?: boolean;
  drift?: {
    sample_limit: number;
    missing_from_graph: FreshnessDocumentRef[];
    extra_in_graph: number[];
    missing_embeddings: FreshnessDocumentRef[];
    extra_embeddings: number[];
    missing_hashes: FreshnessDocumentRef[];
    extra_hashes: number[];
    modified_after_last_sync: FreshnessDocumentRef[];
    changed_since_index?: FreshnessDocumentRef[];
  };
  last_sync: string | null;
  latest_paperless_modified: string | null;
  latest_paperless_id: number | null;
  latest_paperless_title: string | null;
  last_failed_extraction: {
    doc_id?: number;
    title?: string;
    error?: string;
    task_id?: string;
    at?: string;
  } | null;
}

export interface FreshnessDocumentRef {
  id: number;
  title?: string | null;
  modified?: string | null;
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

export interface SearchResult {
  query: string;
  type: string | null;
  results: GraphNode[];
}
