# Repository audit — September 4, 2026

Historical baseline. The [implementation report](2026-09-04-implementation-report.md) records the subsequent fixes and current validation; the defect-asserting probes below are not current CI gates.

Revision: `caaaa5a86d2eb17c5d4260a42077f488646242e6`.

Follow-up: the [functional and accuracy audit](2026-09-04-functional-accuracy-audit.md) adds backend acceptance probes and records the subsequent graph browsing rework. This document preserves the findings and validation state of the initial audit.

The most valuable first changes are query request isolation and reliable ingestion checkpoints. Seven offline reproductions confirm incorrect model selection, answer-cache collisions, missed incremental work, stale entity identities, and overlapping cancelled ingestion. These are bounded correctness fixes worth landing before larger architectural refactors.

## Scope and evidence

Reviewed ingestion, graph/vector storage, entity resolution, ordinary/streaming queries, the document and query frontend flows, Docker configuration, and CI. Recent history guided the survey: in the last 40 commits, `app/query.py` changed 17 times and `app/main.py` 15 times. Architecture suggestions use the installed `improve-codebase-architecture` and `codebase-design` disciplines.

This is a source audit with local checks and isolated behavior reproductions. It does not establish the state of the deployed system, database query plans, actual latency, or exposure to individual dependency advisories. No production data, model endpoint, or live datastore was used. Application behavior and dependency versions were left unchanged.

### Checks run

| Check | Result |
| --- | --- |
| Installed skill integrity | 25 skills / 74 files match upstream Git blob hashes at `3cca18b368ae95cdbdebbff572ccafa662551015` |
| Python parsing | All 21 application and operational-script Python files parse |
| Offline reproductions | Seven defect scenarios reproduced using original function bodies with controlled dependencies |
| Frontend locked install | `npm ci --ignore-scripts` succeeded |
| Frontend lint | Passed: zero errors and warnings |
| Frontend TypeScript | `tsc --noEmit --incremental false` passed |
| Frontend production build | Passed, including all ten generated static pages |
| npm advisory scan, all dependencies | 24 affected package entries: 17 high, 4 moderate, 3 low |
| npm advisory scan, production dependencies | 5 high-severity package entries |

Local tools were Python 3.14.7 and Node 26.8.1. Deployment uses Python 3.12 and Node 20 images, so these checks do not prove behavior under those exact runtimes. Backend integration tests and model-quality evaluations were not run; there is no checked-in automated backend regression suite.

Run the offline evidence from the repo root:

```bash
python3 docs/audits/reproduce-2026-09-04.py
```

The script also works from another directory when invoked by its absolute path. It asserts the current defects, not desired behavior. It reads the selected function bodies from the checkout and uses in-memory dependencies; after fixes, replace these probes with regression tests asserting the intended behavior.

## Prioritized findings

P1 means a correctness, integrity, or dependency issue to address promptly. P2 means a material reliability, performance, or maintenance improvement. Source-traced findings still need the proposed integration checks.

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| A01 | P1 | Sync checkpoint can strand failed or newly modified documents | Reproduced, two scenarios |
| A02 | P1 | Query model and answer-cache identity are not isolated per request | Reproduced, three scenarios |
| A03 | P1 | Generic entity cache can return UUIDs removed by reindex | Reproduced |
| A04 | P1 | Cancellation releases ingestion admission before work stops | Reproduced |
| A05 | P1 | Shared graph relationships lose their remaining document support | Source traced |
| A06 | P1 | Frontend lockfile matches known dependency advisories | npm registry scan |
| A07 | P2 | Derived-state replacement and cache invalidation have inconsistent completion rules | Source traced |
| A08 | P2 | Synchronous Redis operations block async query work | Source traced; latency unmeasured |
| A09 | P2 | Default vector dimension is incompatible with the attempted HNSW index type | Source + upstream documentation |
| A10 | P2 | Document browsing and graph search silently truncate the corpus | Source traced |
| A11 | P2 | CI publishes images without a regression or lint gate | Workflow inspection |
| A12 | P2 | Host-mode configuration and setup documentation disagree with the implementation | Source traced |

### A01 — Preserve retryable work across sync checkpoints

**Where:** [pipeline.py](../../app/pipeline.py#L1306), lines 1306–1374; [paperless.py](../../app/paperless.py#L35), lines 35–42. Full reindex also sets a completion-time checkpoint at pipeline lines 1448–1449.

Sync fetches documents modified after the previous checkpoint, processes them, and advances the checkpoint to the end of the run even after per-document errors or cancellation. A failed document that remains unchanged is excluded next time. A document modified after the initial fetch but before the final timestamp is also excluded from the next incremental fetch; the later deletion scan does not ingest it.

**Change:** capture a scan-start watermark and preserve failed/cancelled work for retry. Advancing only to scan start handles the moving-window gap but does not by itself solve failed documents; retain a retry set or hold the checkpoint appropriately.

**Verify:** fail one document without editing it again; add or modify a document while a scan is paused; cancel with pending documents. Every item must be reconsidered by a later sync.

### A02 — Give each query its own model and complete cache identity

**Where:** [query.py](../../app/query.py#L69), lines 69–75, 626–635, 727–736; module-level `query_engine` at the end of the file.

Both query entry points write `self._model_override` on the singleton. Subsequent awaited work reads that field, so request B can change the model used by request A. Both answer-cache keys omit the selected model. Conversation keys use only the first 50 characters of the last four messages, allowing semantically different histories to reuse an answer even though synthesis consumes additional context.

**Change:** carry immutable request configuration through execution and use a stable, structured cache identity containing the effective model, mode, all conversation context actually consumed, and the corpus/configuration version that changes answers. Avoid lossy context prefixes.

**Verify:** interleave two model selections; ask the same question with different models; vary conversation text beyond character 50. Check both ordinary and streaming delivery and compare saved final answers.

### A03 — Invalidate entity identities with their graph generation

**Where:** [entity_resolver.py](../../app/entity_resolver.py#L686), lines 686–729; [cache.py](../../app/cache.py#L201), lines 201–209; [pipeline.py](../../app/pipeline.py#L1404), lines 1404–1408.

`EntityResolver._cache` is separate from the shared `entity_cache`. Clearing the shared caches or the graph leaves resolver UUIDs intact. Resolving an already seen generic entity after graph clearing returns an old UUID without recreating the entity. Relationship creation can then match no endpoint. The offline probe reproduces the stale identity; actual Cypher outcomes still need a disposable Neo4j test.

**Change:** make resolver identity caches part of graph lifecycle invalidation, including deletion and merges, or validate entries against a graph generation. Preserve current source support when reusing an entity.

**Verify:** resolve a generic entity, clear/rebuild its graph state in the same process, and confirm the returned UUID exists. Reindex the corpus twice and compare entities and relationships.

### A04 — Keep ingestion exclusive until cancelled work has stopped

**Where:** [main.py](../../app/main.py#L331), lines 331–378, 426–436, 885–931, 1066–1076; [pipeline.py](../../app/pipeline.py#L1322), lines 1322–1333.

Admission rejects only tasks marked `running`. Cancellation immediately marks a task `cancelled`, while in-flight document work continues. Another ingestion task is then admitted and can write concurrently with the first. The normal sync/full-reindex wrappers later overwrite the cancelled status with `completed`. The probe demonstrates two simultaneously active ingestion operations and the overwritten terminal state.

**Change:** retain task ownership and exclusivity through a `cancelling` state until work has drained. Centralize terminal-state handling and retain task references so shutdown can finish or cancel active work before closing databases.

**Verify:** pause an in-flight document, cancel, attempt replacement ingestion, and assert it is rejected until the original task stops. Its final status must remain truthful.

### A05 — Track each document's support for a shared relationship

**Where:** [graph.py](../../app/graph.py#L282), lines 282–295 and 445–466; [pipeline.py](../../app/pipeline.py#L970), lines 970–975 and 733–765.

Relationship identity is the endpoint pair plus type. A later supporting document overwrites the single `source_doc` property through `r += $props`. Deleting that latest source deletes the whole relationship, including support still present in another document. The global orphan cleanup also runs between separately awaited entity creation and attachment steps during concurrent ingestion.

**Change:** represent document support explicitly and remove only the departing source's support. Make repeated ingestion idempotent for weights/support. Limit cleanup to affected entities with concurrency-safe ownership.

**Verify:** in disposable Neo4j, let two documents support the same relationship, remove either, and retain the other's evidence. Interleave entity creation and deletion cleanup. A mock alone cannot validate Cypher semantics here.

### A06 — Refresh vulnerable frontend dependencies as a dedicated change

**Where:** [frontend/package.json](../../frontend/package.json) and [frontend/package-lock.json](../../frontend/package-lock.json). Evidence: [full scan](2026-09-04-npm-audit.json), [production-only scan](2026-09-04-npm-audit-production.json).

The production-only scan flags `lodash-es`, `nanoid`, `next`, `postcss`, and `sharp`, each at high severity in the dependency report. These are affected package entries, not five confirmed exploitable paths. The full dependency graph has 24 affected entries. Next.js is pinned to 16.1.6; the scan proposes 16.3.4 as its available remediation at audit time.

**Change:** update Next.js together with its compatible lint configuration, refresh affected transitive packages through the lockfile, and inspect the diff. Check advisory applicability against actual app features; do not use an unchecked `npm audit fix --force`.

**Verify:** repeat production/all-dependency scans, lint, type check, production build, and ordinary/streaming proxy smoke tests. See the scan's advisory links, including [Next.js response cache confusion](https://github.com/advisories/GHSA-68g3-v927-f742) and [sharp inherited libvips advisories](https://github.com/advisories/GHSA-f88m-g3jw-g9cj).

### A07 — Coordinate replacement completion and cache coherence

**Where:** [pipeline.py](../../app/pipeline.py#L237), lines 237–372 and 1488–1506; [main.py](../../app/main.py#L358), lines 358–362, 912–917, and 1418–1425.

Ordinary document processing prepares extraction before deleting existing derived state, but targeted reindex deletes graph/chunks first. A classification/extraction failure can therefore erase an otherwise usable index during targeted reindex. Normal sync/full reindex clear query caches before writes, then clear only freshness afterward; queries during ingestion can repopulate caches with intermediate answers that survive completion. Manual entity merge also lacks shared cache invalidation.

**Change:** consolidate a document's prepare/replace/purge lifecycle, record recoverable partial progress across Neo4j and PostgreSQL, and tie cache identity/invalidation to completed mutations. Two databases do not provide a shared atomic transaction automatically.

**Verify:** inject failures at each replacement step; query before, during, and after mutation; confirm retries converge and completed results use the current graph/corpus.

### A08 — Remove blocking Redis work from async request paths

**Where:** [cache.py](../../app/cache.py#L84), lines 84–138 and 154–174; [query.py](../../app/query.py#L2022), lines 2022–2028 and query entry points.

The cache uses the synchronous Redis client directly in async query paths. Each stalled socket can block the event loop for its configured timeout. Cache clearing, invalidation, and even size/statistics use `KEYS`, adding keyspace-wide work. The in-memory fallback has no size bound, and expired entries are not swept on a schedule.

**Change:** use an async cache interface backed by the existing Redis and in-memory adapters, bounded fallback storage, and incremental scanning or namespace generations for invalidation. Avoid counting the whole keyspace on health requests.

**Verify:** measure event-loop responsiveness and concurrent query latency with delayed Redis; compare invalidation cost as cache size grows. No latency improvement is claimed until measured.

### A09 — Make the vector index compatible with the stored dimensions

**Where:** [embeddings.py](../../app/embeddings.py#L13), line 13 and lines 251–290; search ordering at lines 433–526.

The schema uses 3,072-dimensional `vector`, while index creation attempts HNSW with `vector_cosine_ops`. Current [pgvector HNSW documentation](https://github.com/pgvector/pgvector#hnsw) limits `vector` indexes to 2,000 dimensions and supports `halfvec` up to 4,000. The code catches the dimension error and continues without the index; its suggestion that a version upgrade alone enables this index is unsupported by those docs. Actual deployed index presence was not inspected.

**Change:** evaluate a half-precision expression index with matching query casts, or a lower embedding dimension with an explicit rebuild plan. [pgvector's half-precision indexing example](https://github.com/pgvector/pgvector#half-precision-indexing) shows why the query expression must match the index. Compare recall before choosing.

**Verify:** inspect `pg_indexes`, run `EXPLAIN (ANALYZE, BUFFERS)` on representative queries, and compare retrieval quality and p50/p95 latency. Treat approximate retrieval as a measured tradeoff.

### A10 — Paginate documents and push search into the datastore

**Where:** [documents/page.tsx](../../frontend/src/app/documents/page.tsx#L80), lines 80–100 and 159–183; [graph.py](../../app/graph.py#L578), lines 578–629.

The document page fetches at most 200 results and then paginates/filter-counts that subset locally. Graph search loads at most 5,000 unordered nodes before filtering in Python, so matches beyond that cap cannot be found. Search runs for each input change without a debounce or stale-response guard. These are both completeness limits and unnecessary work as the corpus grows.

**Change:** add datastore-side filtering, stable ordering, real pagination, and totals. Use appropriate graph indexes, preserve resolved entity UUIDs through ingestion to avoid repeated broad rematching, and debounce/cancel frontend searches with stale-response protection.

**Verify:** seed more than 200 documents and 5,000 searchable nodes; reach the last page and find a match beyond the previous cap. Resolve responses out of order and retain only the newest search.

### A11 — Add behavior and validation gates before publishing

**Where:** [.github/workflows/container-images.yml](../../.github/workflows/container-images.yml); [requirements.txt](../../requirements.txt); [frontend/Dockerfile](../../frontend/Dockerfile#L1).

CI builds and pushes images on `main`, with no PR validation job or explicit lint/regression gate. Backend dependencies are largely unpinned. The frontend image uses `npm install` despite having a lockfile. Current static checks pass, but the reproduced concurrency and ingestion defects demonstrate the missing behavioral feedback.

**Change:** add focused regression coverage at query execution and ingestion seams; run it and existing static checks on pull requests before image publication. Lock the backend's resolved dependencies and use `npm ci` in the frontend image. A backend `.dockerignore` would also reduce the root build context, which now includes frontend dependencies and local tooling.

**Verify:** a deliberately failing regression blocks the validation job; rebuilding a fixed lock resolves the same dependencies. Use the deployment's Python/Node versions in CI.

### A12 — Align host-mode routing and quick-start documentation

**Where:** [frontend/next.config.ts](../../frontend/next.config.ts#L12), lines 12–15; [stream route](../../frontend/src/app/api/query/stream/route.ts#L7), line 7; [README.md](../../README.md).

Ordinary frontend requests rewrite to the literal `http://app:8000`, while the streaming route honors `BACKEND_URL`. Host development pointed at a local backend can therefore have working streaming and failing ordinary requests. The README also calls the frontend Next.js 14 although the manifest pins 16.1.6, references a missing root `.env.example`, and describes IVFFlat despite HNSW index creation in code.

**Change:** share the backend destination configuration across both proxy paths, document build-time versus runtime behavior, and align quick-start/version/index descriptions with the implementation. The existing `examples/kg-local.env.example` provides a starting environment template.

**Verify:** run a host frontend with a non-container backend URL and exercise `/api/status` plus `/api/query/stream`; follow the documented fresh-clone setup literally.

## Architecture candidates

These are design opportunities, not approved rewrites. Each should absorb real lifecycle knowledge rather than add a forwarding layer.

| Candidate | Strength | Before → after | Benefit and behavior test |
| --- | --- | --- | --- |
| Query-execution module | Strong | Two workflows plus shared mutable model → one execution with request-owned state and ordinary/streaming delivery adapters | Locality for evidence and finalization; leverage for both adapters. Compare final answers/citations across adapters and interleave models. |
| Document-derived-state module | Strong | Callers coordinate graph, chunks, hashes, identity caches → one recoverable document lifecycle | Locality for completion and invalidation. Fail each storage stage and retry through the same interface callers use. |
| Ingestion-task module | Strong | Three launch paths and independent cancellation/checkpoint rules → one admission, ownership, and terminal-state lifecycle | Leverage across sync, reindex, and repair. Pause/cancel work and verify exclusivity and retryability. |
| Entity-provenance module | Strong | One mutable source per shared edge → explicit per-document support and safe relationship lifetime | Locality for support removal. Remove one of two supporting documents in real Neo4j and retain the edge. |

The deletion test for each candidate: removing the proposed module should redistribute its ordering, ownership, or support rules into several callers. If removing it merely eliminates a pass-through wrapper, it has not added depth. Keep Neo4j and PostgreSQL as actual storage adapters; introduce additional seams only where behavior really varies.

## Suggested implementation order

1. **Query isolation and cache identity:** fix A02 with concurrent-request regression tests; use this bounded change to establish the backend test harness.
2. **Dependency refresh:** address A06 in a separate lockfile/framework change, preserving a clear validation diff.
3. **Ingestion reliability:** A01 and A04, with failure/cancellation/checkpoint tests before wider task refactoring.
4. **Derived-state integrity:** A03, A05, and A07, using disposable Neo4j/PostgreSQL and fault injection.
5. **Measured performance:** A08–A10, collecting event-loop delay, retrieval plans/recall, graph rows transferred, and representative p50/p95 latency first.
6. **Continuous checks and setup:** A11–A12; run all fixed-defect regressions in CI and align docs/configuration.

Keep the architecture work incremental around those fixes. No production migration, dependency upgrade, or broad refactor is included in this setup/audit change.
