# Paperless Knowledge Graph

Paperless Knowledge Graph turns a [Paperless-ngx](https://github.com/paperless-ngx/paperless-ngx) archive into a searchable knowledge graph. An LLM classifies each document, then extracts entities and relationships with a prompt written for that document type. The graph goes to Neo4j, and the text and embeddings go to PostgreSQL with pgvector. A Next.js frontend lets you ask questions in plain language, browse documents and explore the graph. Every answer cites its source documents, and the app checks each claim against the original OCR text before it shows the answer.

## How it works

https://github.com/user-attachments/assets/f7b28ffc-746a-4237-bc67-d10b57a2c045

1. A sync reads documents and their OCR text from Paperless-ngx.
2. The classifier assigns a document type, such as invoice, lab result, contract, insurance, tax, property or military record. The extractor then runs the prompt for that type.
3. Entity resolution links extracted entities to existing ones only when the source evidence supports it. Similar names become review candidates, and a person approves merges of existing entities.
4. The app writes entities and relationships to Neo4j, and chunked text and embeddings to PostgreSQL.
5. A question runs vector similarity, trigram keyword search and graph traversal together to find source passages.
6. The model drafts an answer. A source audit then checks every claim and citation against the original OCR, and the app repairs, qualifies or withholds claims it cannot support.

## Features

- **Type-aware extraction.** The app classifies each document first, then uses an extraction prompt specialized for that type.
- **Evidence-aware entity resolution.** Matching uses canonical names, verified aliases and document-local UUID/type bindings. Name similarity only suggests candidates. Merging existing entities requires human review, and a "not the same" decision stays recorded.
- **Hybrid search.** Queries combine pgvector similarity, trigram keyword search and graph traversal.
- **Graph-aware retrieval.** Retrieval expands two to three hops from the entities it finds.
- **Planned, audited queries.** A [Strands](https://strandsagents.com) planner drives retrieval and synthesis. A full source audit follows, with at most one repair before the app returns an answer limited to what the evidence supports.
- **Strict mode.** For high-stakes questions, the app builds an evidence pack, a claim ledger, trust scores and repair notes.
- **Timeline mode.** Change-over-time questions return dated events from the verified answer, sorted by date, with the source wording and links kept.
- **Entity steward.** The steward suggests merges, splits and review items after manual merges, after each sync and on a schedule. It never merges entities by itself.
- **Concurrent processing.** A semaphore limits how many documents process in parallel.
- **Retries.** LLM and database calls retry transient errors with exponential backoff and jitter.
- **Caching.** Query, vector, graph and entity caches use Redis. Set `REDIS_URL` to a value starting with `memory:` to use bounded in-process caches instead.
- **Task progress.** Sync and reindex tasks report progress and can be cancelled.
- **Frontend.** The Next.js app has a 2D/3D graph explorer, a document browser, topic hubs, natural language queries with evidence and trust review, an entity review queue and a live log viewer.

## Answer behavior

- **Citations.** The app checks source attributions against the supplied OCR evidence and each claim's validated references before it renders document links.
- **Partial answers.** When unsupported claims remain after the audit and one repair, the app can re-audit the supported parts as a new partial answer. A "verified partial answer" contains only claims that passed source checks. It states how many claims were omitted and that the answer is incomplete. The app withholds answers whose audit failed, conflicted, did not finish or ran while sources changed, and it never caches a partial answer as a completed one.
- **History and "latest" questions.** For history questions, retrieval reserves a bounded set of indexed sources across the recorded periods before synthesis. When an answer reports the latest documented value, it describes the newest retrieved record. It does not claim that the value is still current or that the archive is complete.
- **Dates.** Written-month, ISO and numeric dates go through the same calendar validation. `SOURCE_DATE_ORDER` sets how the app reads numeric dates, and citations keep the original text.
- **Conversation context.** Saved conversations keep every message for display. Model prompts include only the latest 10 messages, within a 12,000-character budget shared by planning, synthesis and source auditing. When the budget runs out, the app drops the oldest context first. A single message that is too long keeps its ending and gets an explicit truncation marker.
- **Audit limits.** Source audits run in batches of four answer units with limited concurrency. The overall deadline grows with the number of batches, and every model call and repair attempt has its own deadline. An incomplete or truncated audit cannot certify an answer.

See the [query reliability specification](docs/specs/evidence-query-reliability.md) for details.

## Architecture

```
Paperless-ngx -> OpenAI-compatible model endpoint -> document classification -> type-specific extraction
  -> entity resolution (source evidence + reviewed identity) -> Neo4j (graph) + pgvector (embeddings)
  -> hybrid query pipeline (Strands plan + vector + keyword + graph + evidence verifier)
```

### Components

| Component | Description |
|-----------|-------------|
| `app/pipeline.py` | Runs sync and reindex: classification, extraction, graph and embedding storage |
| `app/classifier.py` | LLM document type classification |
| `app/extractor.py` | Type-specific entity and relationship extraction with fallback prompts |
| `app/graph.py` | Neo4j operations: nodes, relationships and subgraph traversal |
| `app/embeddings.py` | pgvector storage, chunking, vector and keyword search, dimension migration |
| `app/entity_resolver.py` | Evidence-aware identity matching and human-reviewed merges with vetoes |
| `app/entity_policy.py`, `app/entity_bindings.py` | Provenance and spelling policy, document-local UUID/type bindings |
| `app/entity_steward.py` | Merge, split and review suggestions; never merges on its own |
| `app/evidence.py` | Evidence packs, source quality, date signals, claim ledgers and verifier repair helpers |
| `app/query.py` | Iterative hybrid query pipeline with modes, evidence packs, verification and synthesis |
| `app/query_quality.py` | Query planning fallbacks, timeline sorting and trust scores |
| `app/strands_orchestrator.py` | Strands planner, verifier, answer editor and entity reviewer |
| `app/timeline.py` | Final-answer dates, source-date binding and compatibility with saved timelines |
| `app/cache.py` | TTL caches for queries, vectors, graph and entities (Redis or in memory) |
| `app/retry.py` | Shared retry helpers: exponential backoff for LLM calls, shorter retries for the database |
| `app/config.py` | Pydantic settings loaded from the environment |

### Stack

- **Backend:** FastAPI (Python 3.12)
- **Graph database:** Neo4j 5 Community with APOC
- **Vector database:** PostgreSQL 16 with pgvector and pg_trgm
- **Model access:** any OpenAI-compatible endpoint, such as a LiteLLM proxy, OpenRouter, OpenAI or a local server
- **Embeddings:** OpenAI text-embedding-3-large (3072 dimensions) by default, from the chat endpoint or a separate one
- **Frontend:** Next.js 16 and React 19 on Node 24 LTS, with shadcn/ui and react-force-graph

## Quick start

You need a running Paperless-ngx instance and an OpenAI-compatible model endpoint that serves the models listed under [Models](#models). [Model providers](#model-providers) has sample settings for LiteLLM, OpenRouter and other endpoints. The main compose file runs the backend, frontend, Neo4j, PostgreSQL with pgvector, and Redis. It connects to Paperless through `PAPERLESS_URL` and `PAPERLESS_TOKEN`.

```bash
cp .env.example .env
# Set your Paperless URL and token, your model endpoint and key, and the Neo4j and Postgres passwords
docker compose up -d
```

The frontend runs at `http://localhost:3001` and the API at `http://localhost:8484`. The graph starts empty. Run a full reindex, as described under [Workflow](#workflow), to import your documents.

### Local testing

For a disposable Paperless-ngx instance, use the sample compose file in [`examples/`](examples/):

```bash
cp examples/paperless.env.example examples/paperless.env
docker compose --env-file examples/paperless.env \
  -f examples/docker-compose.paperless.yml up -d
```

Create an API token in Paperless at `http://localhost:8000`. Then copy the sample knowledge graph environment, add the Paperless token and model endpoint settings, and start the stack:

```bash
cp examples/kg-local.env.example .env
# edit .env
docker compose up -d
```

To run only the databases and start the backend directly on your machine, use [`examples/docker-compose.datastores.yml`](examples/docker-compose.datastores.yml). [`examples/README.md`](examples/README.md) covers the full local flow and the environment values for running on the host.

### Environment variables

[`.env.example`](.env.example) lists the common settings.

| Variable | Description | Default |
|----------|-------------|---------|
| `PAPERLESS_URL` | Paperless-ngx URL | `http://localhost:8000` |
| `PAPERLESS_TOKEN` | Paperless API token | none |
| `PAPERLESS_EXTERNAL_URL` | Paperless URL used for document links in the frontend | Same as `PAPERLESS_URL` |
| `PAPERLESS_SKIP_TAG_NAMES` | Comma-separated Paperless tags. Documents with these tags are left out of sync, reindex and freshness checks. | `needs-review` |
| `LLM_BASE_URL` | OpenAI-compatible endpoint for chat models. A trailing `/v1` is optional. | Uses `LITELLM_URL` |
| `LLM_API_KEY` | API key for `LLM_BASE_URL` | none |
| `LITELLM_URL` | LiteLLM proxy URL, used when `LLM_BASE_URL` is empty | `http://localhost:4000` |
| `LITELLM_API_KEY` | LiteLLM API key, used with `LITELLM_URL` | none |
| `LLM_MODEL` | Primary model for classification, extraction, answers and entity helpers. `GEMINI_MODEL` is an older name for the same setting. | `gemini-3.8-flash` |
| `FALLBACK_MODEL` | Model route used after rate limits or errors | `gpt-5.4-mini` |
| `EMBEDDING_BASE_URL` | Separate OpenAI-compatible endpoint for embeddings | Same as the chat endpoint |
| `EMBEDDING_API_KEY` | API key for `EMBEDDING_BASE_URL` | none |
| `EMBEDDING_MODEL` | Embedding model name | `text-embedding-3-large` |
| `EMBEDDING_DIMENSIONS` | Vector length that `EMBEDDING_MODEL` returns | `3072` |
| `STRANDS_ENABLED` | Enables the Strands planner, verifier and editor | `true` |
| `STRANDS_MODEL` | Optional model override for Strands calls | Same as `LLM_MODEL` |
| `STRANDS_MAX_CONCURRENT_CALLS` | Maximum concurrent helper calls and audit workers per answer (1 to 16) | `4` |
| `STRANDS_CALL_TIMEOUT_SECONDS` | Deadline for one helper call | `45` |
| `ANSWER_AUDIT_TIMEOUT_SECONDS` | Time allowed per audit batch and per repair. The total audit deadline scales with the number of batches. | `60` |
| `QUESTION_PIPELINE_ENABLED` | Enables the experimental original-source question pipeline | `false` |
| `SOURCE_DATE_ORDER` | How to read numeric dates: `mdy`, `dmy` or `reject_ambiguous`. Two-digit years never supply a century. | `mdy` |
| `BACKEND_URL` | Backend URL the frontend server uses for API calls and streaming | `http://app:8000` |
| `NEO4J_URI` | Neo4j Bolt URI | `bolt://neo4j:7687` |
| `NEO4J_USER` / `NEO4J_PASSWORD` | Neo4j credentials | `neo4j` / none |
| `POSTGRES_HOST` / `POSTGRES_PORT` | PostgreSQL host and port | `pgvector` / `5432` |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | PostgreSQL credentials | `knowledge_graph` / `kguser` / none |
| `REDIS_URL` | Redis URL. The compose file sets it to `redis://redis:6379`. | `redis://localhost:6379` |
| `OWNER_NAME` | Your name, used in query prompts | none |
| `OWNER_CONTEXT` | Short context about you, used in query prompts | none |
| `MAX_CONCURRENT_DOCS` | Documents processed in parallel | `10` |
| `AUTO_SYNC_INTERVAL_MINUTES` | Minutes between automatic incremental syncs. `0` turns this off. | `0` |
| `ENTITY_STEWARD_INTERVAL_MINUTES` | Minutes between scheduled steward reviews. `0` turns this off. | `360` |
| `ENTITY_STEWARD_CANDIDATE_LIMIT` | Candidates per scheduled steward run | `40` |
| `FRESHNESS_CACHE_TTL_SECONDS` | Seconds to cache `/freshness` results | `60` |

### Models

The app sends every model request to an OpenAI-compatible API, so each model name must exist on your endpoint. The defaults are `gemini-3.8-flash` as the primary model ([Google model documentation](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)), `gpt-5.4-mini` as the fallback and `text-embedding-3-large` for embeddings. These are LiteLLM route names. Other providers name the same models differently, as the samples below show.

The classifier, extractor, query engine, entity helpers, conversation titles and the default for `/models` all use `LLM_MODEL`. Strands uses `STRANDS_MODEL` when it is set. `/models` lists the models from the endpoint's `GET /v1/models`. With `LITELLM_URL`, it also tries LiteLLM's `/model/info`. If neither works, the chat model picker offers only `LLM_MODEL` and shows the error. The app does not send a thinking-level setting, so do not configure the unsupported `minimal` level for Gemini 3.8 Flash in the proxy.

Query cache keys include the query model and the Strands model, so changing either one does not return stale cached answers. Ingestion fingerprints include the extraction model and the source policy version. After you change the extraction model, the next normal sync rebuilds the affected documents. You don't need to wipe the graph, run Reindex All or redo OCR in Paperless. Changing models does not change the OCR hash that document feedback uses.

### Model providers

The chat endpoint comes from `LLM_BASE_URL` and `LLM_API_KEY`. When `LLM_BASE_URL` is empty, the app uses `LITELLM_URL` and `LITELLM_API_KEY`, so existing installs keep working without changes. Embeddings use the chat endpoint unless you set `EMBEDDING_BASE_URL`. Each API key is sent only to its own URL: an empty `LLM_API_KEY` stays empty and never borrows `LITELLM_API_KEY`. Leave the key empty for a local server that needs none.

Write a base URL with or without `/v1`. The app adds `/v1` to a bare host such as `http://litellm:4000` and keeps a URL that already has a version segment, such as `https://openrouter.ai/api/v1` or `https://generativelanguage.googleapis.com/v1beta/openai`. A base URL cannot include a query string. The backend refuses to start if one does.

`/health` checks both endpoints. Its `llm` component sends a five-token chat request to `LLM_MODEL`. Its `embeddings` component embeds a short string and reports `unhealthy` when the vector length differs from `EMBEDDING_DIMENSIONS`.

**LiteLLM proxy.** The proxy maps route names to providers, so the default model names work when your proxy defines those routes:

```env
LITELLM_URL=http://your-litellm:4000
LITELLM_API_KEY=your-litellm-key
LLM_MODEL=gemini-3.8-flash
FALLBACK_MODEL=gpt-5.4-mini
EMBEDDING_MODEL=text-embedding-3-large
EMBEDDING_DIMENSIONS=3072
```

**OpenRouter.** OpenRouter model names carry a provider prefix. OpenRouter also serves `openai/text-embedding-3-large`, so one key covers chat and embeddings:

```env
LLM_BASE_URL=https://openrouter.ai/api/v1
LLM_API_KEY=your-openrouter-key
LLM_MODEL=google/gemini-3.8-flash
FALLBACK_MODEL=openai/gpt-5.4-mini
EMBEDDING_MODEL=openai/text-embedding-3-large
EMBEDDING_DIMENSIONS=3072
```

**Any other OpenAI-compatible endpoint.** This covers Requesty, OpenAI, Google's OpenAI-compatible Gemini API and local servers. Use the model names that the endpoint's `GET /v1/models` returns. If the endpoint does not serve embeddings, point `EMBEDDING_BASE_URL` at one that does. This sample runs chat through Google's Gemini API and embeddings through OpenAI:

```env
LLM_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai
LLM_API_KEY=your-gemini-key
LLM_MODEL=gemini-3.8-flash
FALLBACK_MODEL=gemini-3.5-flash
EMBEDDING_BASE_URL=https://api.openai.com/v1
EMBEDDING_API_KEY=your-openai-key
EMBEDDING_MODEL=text-embedding-3-large
EMBEDDING_DIMENSIONS=3072
```

#### Change the embedding dimensions

The pgvector columns have a fixed length. If `EMBEDDING_DIMENSIONS` differs from the stored columns, the backend refuses to start and asks for an explicit backed-up migration. It never erases vectors on its own. Above 4000 dimensions, the optional HNSW candidate indexes are skipped because pgvector cannot build them; retrieval stays exact. To switch to a model with a different vector length:

1. Back up the PostgreSQL database.
2. Set `EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS` to the new model and its vector length.
3. Clear the stored vectors and resize both columns. This example uses 1536 dimensions:

   ```sql
   TRUNCATE document_embeddings, entity_embeddings;
   DROP INDEX IF EXISTS idx_embeddings_halfvec_hnsw, idx_entity_halfvec_hnsw;
   ALTER TABLE document_embeddings ALTER COLUMN embedding TYPE vector(1536);
   ALTER TABLE entity_embeddings ALTER COLUMN embedding TYPE vector(1536);
   ```

4. Start the backend and run a full reindex, as described under [Workflow](#workflow).

Vectors from different models are not comparable, even at the same length. After you change `EMBEDDING_MODEL`, run the same steps, and skip the `ALTER TABLE` statements if the length stays the same.

### Extraction

Extraction has no document size or window count limit. The extractor processes overlapping windows through the end of every document, and the app records a document as complete only after every window finishes. Transient failures stay retryable.

Extraction requests set no output token limit of their own and use the provider's default. If the provider truncates output, the extractor splits only the affected window into smaller overlapping ranges. Each replacement window must finish every pass before the document counts as complete. If truncation continues at the smallest split size, the document fails instead of being stored partially.

Each extraction pass gets up to three attempts in total. Retries apply deterministic corrections to the response format and repeat only the pass that failed. The SDK's own retries are off. The default extractor uses the SDK's 600-second read, write and pool timeouts and a 5-second connect timeout. Window logs record only offsets and completed and pending counts, never source text.

After changing models, test extraction on one small and one large document before running a full sync.

## API endpoints

### Core

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/status` | GET | Node, relationship and embedding counts |
| `/readyz` | GET | Lightweight readiness check for Kubernetes probes |
| `/health` | GET | Component health (Neo4j, pgvector, chat and embedding endpoints, cache stats) |
| `/freshness` | GET | Compares exact document IDs across Paperless, the graph, embeddings and hashes. Add `?force=true` to skip the short status cache. |
| `/freshness/repair` | POST | Starts a background repair for the drifted IDs reported by `/freshness?force=true` |
| `/ops/guardrails` | GET | Machine-readable sync age, exact ID drift, model health and recent error alerts |
| `/config` | GET | Frontend configuration (the Paperless URL) |
| `/models` | GET | Chat models available from the configured endpoint |
| `/sync` | POST | Incremental sync of new and changed documents |
| `/reindex` | POST | Full reindex. Each document is prepared and replaced individually, so existing data stays usable until its replacement is ready. |
| `/reindex/{doc_id}` | POST | Starts a background reindex of one document |
| `/task/{task_id}` | GET | Task progress: processed count, errors, ETA and current document |
| `/task/{task_id}/cancel` | POST | Requests cancellation of a running task |

### Documents

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/documents` | GET | Paged list of indexed documents with search and type filters |
| `/document/{doc_id}/detail` | GET | Document detail, extracted graph facts, chunks and processing status |
| `/document/{doc_id}` | DELETE | Removes a document's graph, embeddings and hash from the knowledge graph. Paperless is not changed. |
| `/document/{doc_id}/feedback` | GET, POST | Lists or records extraction feedback for human review |
| `/document/{doc_id}/feedback/{feedback_id}/resolve` | POST | Marks feedback as resolved |

### Query and search

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/query` | POST | Natural language query with hybrid retrieval and LLM synthesis |
| `/query/stream` | POST | Server-sent events stream with trace and status events, answer replacement after repair, and trust metadata |
| `/graph/search?q=...&type=...` | GET | Searches graph nodes by name, with an optional type filter |
| `/graph/node/{uuid}` | GET | Node details and relationships |
| `/graph/neighbors/{uuid}?depth=2` | GET | Multi-hop neighborhood |
| `/graph/initial?limit=300` | GET | Initial graph for the explorer |
| `/conversations` | GET, POST | Lists or creates saved conversations |
| `/conversations/{conv_id}` | GET, PATCH, DELETE | Reads, renames or deletes a conversation |
| `/generate-title` | POST | Generates a short conversation title from a message |

`/query` and `/query/stream` accept a `mode`:

| Mode | Purpose |
|------|---------|
| `quick` | Single-pass retrieval for fast lookups |
| `deep` | Planned multi-pass retrieval with synthesis and citations |
| `timeline` | Chronological extraction for change-over-time questions |
| `strict` | High-accuracy path for medical, legal, tax, insurance, financial and exact-value questions. Adds an evidence pack, claim ledger, verifier repair and trust scores. |

### Entity maintenance

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/resolve-entities` | POST | Reports identity review candidates. Merging existing entities still requires review. |
| `/entity-review/candidates` | GET | Possible duplicate entities with deterministic scores and the latest steward suggestions |
| `/entity-review/steward` | POST | Runs the entity steward now. It records suggestions only. |
| `/entity-review/steward/task` | POST | Runs the entity steward as a background task |
| `/entity-review/merge` | POST | Merges a reviewed duplicate pair, then schedules a focused steward pass |
| `/entity-review/split` | POST | Marks a pair as distinct |
| `/entity-review/ignore` | POST | Hides a candidate pair from review |
| `/create-indexes` | POST | Builds IVFFlat vector indexes. Run it after a reindex. |
| `/logs` | GET | Buffered log lines as JSON, filterable by level and time |
| `/logs/stream` | GET | Live log stream over server-sent events |

## Frontend pages

| Page | Description |
|------|-------------|
| `/` | Dashboard with stats, quick actions and recent activity |
| `/graph` | Interactive 2D/3D graph explorer |
| `/documents` | Document browser with type filters, sorting, pagination and batch reindex |
| `/documents/[id]` | Document detail, extracted graph facts, chunks, processing status and extraction feedback |
| `/hubs` | Topic hubs (insurance, taxes, medical, vehicles, home) that page through matching documents and suggest questions |
| `/query` | Natural language queries with citations, modes, trace, trust scores, evidence review and claim ledger |
| `/entities/review` | Review queue for possible duplicate entities, with steward suggestions |
| `/debug` | Live log viewer with level filters and auto-scroll |

The graph explorer opens in 2D. You can expand nodes, inspect sources, filter by type, focus on a neighborhood and switch to 3D. See [graph validation](docs/audits/graph-validation.md) for its regression tests and a synthetic preview.

## Services

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 3001 | Next.js app |
| Backend | 8484 | FastAPI API |
| Neo4j Browser | 7474 | Graph database web UI |
| Neo4j Bolt | 7687 | Graph database protocol |
| PostgreSQL | 5433 | pgvector and pg_trgm |

## Workflow

1. **Start the stack:** `docker compose up -d`
2. **Full reindex:** `POST /reindex` classifies and extracts every Paperless document.
3. **Build indexes:** `POST /create-indexes` creates IVFFlat vector indexes. It needs data, so run it after the first reindex.
4. **Review entities:** `GET /entity-review/candidates` lists possible duplicates. Optionally run `POST /entity-review/steward` for suggestions, then merge or split pairs with the review endpoints.
5. **Keep it current:** `POST /sync` processes only new and changed documents and schedules a steward pass afterward.
6. **Ask questions:** `POST /query {"question": "What invoices mention Acme Corp?", "mode": "deep"}`
7. **High-stakes questions:** `POST /query {"question": "What is my current premium?", "mode": "strict"}` adds the evidence pack, verifier and claim ledger.

## Evidence-aware entity identity

Entity resolution uses versioned evidence rules and document-local UUID/type bindings. Legacy aliases stay searchable but are not trusted. Similarity alone cannot link two identities or add a trusted alias. The bulk `/resolve-entities` endpoint only reports candidates, and reviewed merges go through `/entity-review/merge`. See the [policy, migration and repair gates](docs/specs/evidence-aware-entity-resolution.md).

## Evaluation harness

Run the factual evaluation against a disposable API loaded with the versioned synthetic corpus described in [evals/README.md](evals/README.md):

```bash
python3 scripts/eval_harness.py --base-url http://localhost:8484
```

The default cases are in `evals/fixtures/accuracy-v1.json`. They check known values, units, source spans and expected abstentions, and a case cannot pass on self-reported confidence. `evals/canonical_questions.json` holds separate smoke checks. Add `--json` for machine-readable output. A live API evaluation may call the configured model.

## Operational checks

Run a read-only API smoke test:

```bash
python3 scripts/api_smoke_tests.py --base-url http://localhost:8484
```

After a restore or migration, compare exact document IDs across Paperless, the graph, vectors and hashes:

```bash
python3 scripts/kg_exact_drift_audit.py --base-url http://localhost:8484
```

The audit exits with a non-zero code when it finds drift. `--base-url` defaults to `KG_URL` when that variable is set. Add `--repair --wait` to start the targeted repair and wait for it to finish.

Export the graph and vector data before risky changes:

```bash
bash scripts/export_state.sh
```

## Agent skills and repository audits

This repository includes Matt Pocock's engineering and productivity skills under `.agents/skills/`. Start with [`AGENTS.md`](AGENTS.md) and the [skill setup guide](docs/agents/skills.md), which list the available workflows, pinned upstream sources and project conventions. Use `$improve-codebase-architecture` for an architecture review, or `$code-review` with a baseline commit and spec to review a change.

The [September 4, 2026 audit](docs/audits/2026-09-04-repo-audit.md) records prioritized findings, offline reproductions, validation results and next steps. The [functional and accuracy audit](docs/audits/2026-09-04-functional-accuracy-audit.md) extends that review to backend features, evidence verification, entity review and the graph explorer. The [specification](docs/specs/accuracy-and-reliability.md) and [implementation report](docs/audits/2026-09-04-implementation-report.md) describe the fixes that followed, how they were validated and the accuracy limits that remain.

## License

MIT
