# Paperless Knowledge Graph

A Docker-based knowledge graph system that extracts structured information from Paperless-ngx documents using Gemini 2.5 Flash, stores entities and relationships in Neo4j, and provides vector search via pgvector.

## Architecture

```
Paperless-ngx documents → Document classification (Gemini) → Type-specific extraction (Gemini)
    → Entity resolution (fuzzy match + embeddings) → Neo4j (graph) + pgvector (embeddings)
```

## Quick Start

```bash
docker compose up -d
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/status` | GET | Health check, node/relationship counts |
| `/sync` | POST | Incremental sync from Paperless |
| `/reindex` | POST | Full reindex (clears graph, reprocesses all) |
| `/reindex/{doc_id}` | POST | Reindex a single document |
| `/query` | POST | Natural language query (`{"question": "..."}`) |
| `/graph/search?q=...` | GET | Search graph nodes by name |
| `/graph/node/{uuid}` | GET | Get a node and its relationships |
| `/graph/neighbors/{uuid}?depth=2` | GET | Neighborhood traversal |
| `/task/{task_id}` | GET | Check background task status |

## Services

- **App**: FastAPI on port 8484
- **Neo4j**: Browser on port 7474, Bolt on port 7687
- **PostgreSQL + pgvector**: Port 5433
