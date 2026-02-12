# Paperless Knowledge Graph

A knowledge graph system that extracts structured entities and relationships from [Paperless-ngx](https://github.com/paperless-ngx/paperless-ngx) documents. Uses LLM-powered document classification and type-aware extraction, stores results in Neo4j (graph) and pgvector (embeddings), and provides hybrid search with a visual graph explorer frontend.

## Features

- **Document-type-aware extraction** — Classifies documents first (invoice, medical, tax, etc.), then uses specialized extraction prompts per type
- **Entity resolution** — Fuzzy matching + embedding similarity to merge duplicate entities automatically
- **Hybrid search** — Vector similarity (pgvector) + trigram keyword search + graph traversal combined
- **Graph-aware retrieval** — Multi-hop subgraph expansion from discovered entities (2-3 hops)
- **Iterative query pipeline** — Initial retrieval → gap analysis → follow-up retrieval → synthesis with citations
- **Concurrent processing** — Configurable parallel document processing with semaphore control
- **Retry with backoff** — Exponential backoff + jitter on all LLM and database calls for transient error resilience
- **TTL-based caching** — Query, vector, graph, and entity caches to reduce redundant LLM/DB calls
- **Live progress tracking** — Real-time progress for reindex/sync tasks via polling
- **Interactive frontend** — Next.js app with 2D/3D graph explorer, document browser, natural language query, and live debug logs

## Architecture

```
Paperless-ngx → LiteLLM (model routing) → Document Classification → Type-Specific Extraction
  → Entity Resolution (fuzzy + embeddings) → Neo4j (graph) + pgvector (embeddings)
  → Hybrid Query Pipeline (vector + keyword + graph + LLM synthesis)
```

### Components

| Component | Description |
|-----------|-------------|
| `app/pipeline.py` | Orchestrates sync/reindex — classification, extraction, graph + embedding storage |
| `app/classifier.py` | LLM-based document type classification |
| `app/extractor.py` | Type-specific entity/relationship extraction with fallback prompts |
| `app/graph.py` | Neo4j operations — create/query nodes, relationships, subgraph traversal |
| `app/embeddings.py` | pgvector storage, chunking, vector/keyword search, dimension migration |
| `app/entity_resolver.py` | Fuzzy match + embedding similarity entity deduplication |
| `app/query.py` | Iterative hybrid query pipeline with gap analysis and LLM synthesis |
| `app/cache.py` | TTL-based caching for queries, vectors, graph, entities (Redis or in-memory) |
| `app/retry.py` | Shared retry utilities — exponential backoff for LLM, shorter retry for DB |
| `app/config.py` | Pydantic settings (env-based configuration) |

### Stack

- **Backend:** FastAPI (Python 3.12)
- **Graph DB:** Neo4j 5 Community + APOC
- **Vector DB:** PostgreSQL 16 + pgvector + pg_trgm
- **LLM Routing:** LiteLLM (centralized model management, usage tracking, caching)
- **Embeddings:** OpenAI text-embedding-3-large (3072-dim) via LiteLLM
- **Frontend:** Next.js 14 + shadcn/ui + react-force-graph-2d/3d + zustand + d3-force

## Quick Start

```bash
cp .env.example .env
# Edit .env with your Paperless URL/token, LiteLLM URL/key, Neo4j password, Postgres creds

docker compose up -d
```

### Environment Variables

See [`.env.example`](.env.example) for all available configuration options.

| Variable | Description | Default |
|----------|-------------|---------|
| `PAPERLESS_URL` | Paperless-ngx instance URL | `http://localhost:8000` |
| `PAPERLESS_TOKEN` | Paperless API token | — |
| `PAPERLESS_EXTERNAL_URL` | External URL for Paperless links in frontend | Same as `PAPERLESS_URL` |
| `LITELLM_URL` | LiteLLM proxy URL | `http://localhost:4000` |
| `LITELLM_API_KEY` | LiteLLM API key | — |
| `EMBEDDING_MODEL` | Embedding model name | `text-embedding-3-large` |
| `GEMINI_MODEL` | LLM model for extraction | `gemini-2.5-flash` |
| `NEO4J_USER` / `NEO4J_PASSWORD` | Neo4j credentials | `neo4j` / — |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | pgvector credentials | `knowledge_graph` / `kguser` / — |
| `REDIS_URL` | Redis connection URL (optional) | `redis://localhost:6379` |
| `OWNER_NAME` | Your name (used in query prompts) | — |
| `OWNER_CONTEXT` | Brief context about yourself | — |
| `MAX_CONCURRENT_DOCS` | Parallel doc processing limit | `10` |

## API Endpoints

### Core

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/status` | GET | Node/relationship/embedding counts |
| `/health` | GET | Component health check (Neo4j, pgvector, LiteLLM, cache stats) |
| `/config` | GET | Frontend configuration (paperless URL) |
| `/sync` | POST | Incremental sync — processes new/changed documents |
| `/reindex` | POST | Full reindex — clears graph + embeddings, reprocesses all documents |
| `/reindex/{doc_id}` | POST | Reindex a single document |
| `/task/{task_id}` | GET | Live task progress (processed, errors, ETA, current doc) |

### Query & Search

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/query` | POST | Natural language query with hybrid retrieval + LLM synthesis |
| `/graph/search?q=...&type=...` | GET | Search graph nodes by name, optional type filter |
| `/graph/node/{uuid}` | GET | Get node details + relationships |
| `/graph/neighbors/{uuid}?depth=2` | GET | Multi-hop neighborhood traversal |
| `/graph/initial?limit=300` | GET | Initial graph load for explorer |

### Maintenance

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/resolve-entities` | POST | Run entity resolution (merge duplicates) |
| `/create-indexes` | POST | Build IVFFlat vector indexes (run after reindex) |
| `/logs` | GET | Buffered log lines (JSON), filterable by level/time |
| `/logs/stream` | GET | SSE live log stream |

## Frontend Pages

| Page | Description |
|------|-------------|
| `/` | Dashboard — stats, quick actions, recent activity |
| `/graph` | Interactive 2D/3D graph explorer with physics simulation |
| `/documents` | Document browser with type filtering, sorting, pagination, batch reindex |
| `/query` | Natural language query interface with cited results |
| `/debug` | Live terminal-style log viewer with level filtering and auto-scroll |

## Services

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 3001 | Next.js app |
| Backend | 8484 | FastAPI API |
| Neo4j Browser | 7474 | Graph DB web UI |
| Neo4j Bolt | 7687 | Graph DB protocol |
| PostgreSQL | 5433 | pgvector + pg_trgm |

## Workflow

1. **Initial setup:** `docker compose up -d`
2. **Full reindex:** `POST /reindex` — classifies + extracts all Paperless documents
3. **Build indexes:** `POST /create-indexes` — creates IVFFlat vector indexes (needs data first)
4. **Entity resolution:** `POST /resolve-entities` — merges duplicate entities
5. **Ongoing sync:** `POST /sync` — processes only new/changed documents
6. **Query:** `POST /query {"question": "What invoices mention Acme Corp?"}` — hybrid search + LLM answer

## License

MIT
