# Cache reliability and complete indexed browsing

The current Redis adapter runs synchronous socket calls in async request/task paths, uses Redis `KEYS` for invalidation and health statistics, and the fallback dictionary is unbounded and exposes mutable cached values. Graph search selects 5,000 arbitrary nodes before filtering in Python. The documents page downloads at most 200 nodes and then displays client-side search, counts and pagination as though that subset were the index.

## Cache contract

Keep the synchronous cache interface for compatibility and expose async wrappers implemented with `asyncio.to_thread`. Every async application call site must await these wrappers or offload equivalent synchronous calls. In-memory caches use a lock, monotonic expiry, an explicit entry budget with least-recently-used eviction, and defensive deep copies on read/write. Expired entries are removed during writes/statistics and never counted as live.

Redis clear switches a namespace generation rather than enumerating keys. Old entries expire by TTL. Prefix invalidation uses bounded `SCAN` batches, never `KEYS`. Health statistics report Redis size as unknown instead of scanning the keyspace. Redis URL parsing must retain authentication/TLS options. Corpus invalidation advances a separate shared generation; query identity captures that generation and must not commit an answer computed against an older generation after a mutation. Async wrappers include `cache_get`, `cache_set`, `get_corpus_generation_async`, `invalidate_on_sync_async`, and `get_all_cache_stats_async`.

Cache failures must not change answer correctness: reads fail as misses; a local fallback generation prevents reuse across local failed invalidations and retries publishing an invalidation after Redis recovery. A cache generation does not make concurrent multi-database mutation atomic or solve independent-worker admission during a partition.

## Browsing contract

Move text filtering and deterministic ordering into Neo4j before pagination. Preserve default `GraphStore.search_nodes` list results for retrieval callers; an explicit paginated mode returns results, exact matching count, offset, limit and has-more. Validate labels, sort fields, directions and bounded page sizes. Only interpolate validated identifiers; bind query text and filter values as parameters. Documents endpoint searches and paginates the complete indexed Document label, with type counts from the full matching search set. It does not claim to enumerate unindexed Paperless documents.

Documents UI delegates search/type/sort/pagination to the backend; counts describe all matching indexed documents, selections are page-scoped, stale requests cannot overwrite newer searches, and failures display retry controls. Graph search offers further pages and discloses total matching results. Offset pagination is deterministic for a stable graph; concurrent ingestion can shift pages, so it is not a snapshot guarantee.

Domain hubs use the same complete graph-search endpoint, with 12 documents per server page and the server's full matching indexed count. Each domain retains its page number when switching views. Request generations guard success, error, and loading updates; switching a domain invalidates the prior request immediately. Failures expose backend detail with an explicit retry action. A shrinking result set moves an out-of-range selection to its last valid page. Domain matching remains a keyword-search view, not a guarantee that every returned document belongs to the domain.

## Acceptance

- Memory adapter expiry, capacity, mutation isolation and concurrent access; Redis namespace clear, SCAN prefix invalidation and health with a fake that rejects `KEYS`.
- Block a Redis operation in a controlled adapter while an independent event-loop task continues.
- Corpus generation changes on invalidation and cannot silently revert after a temporary Redis failure.
- Real-module Neo4j tests seed more than 5,000 nodes and find a late match; document pages reach IDs beyond 200; ordering, filters and exact totals agree; invalid parameters fail predictably.
- Existing retrieval list callers and frontend graph/document consumers retain compatible data shapes; frontend lint/type/build checks validate integration.

## Validation recorded

Eleven cache behavior tests pass with memory/Redis adapters, including controlled socket blocking while the event loop advances. The 18 ingestion regressions also pass after offloading cache calls. Seven browsing tests pass against a disposable local Neo4j 5 database using the real imported `GraphStore` and real FastAPI routes; the suite creates 5,105 uniquely tagged documents and removes only those fixtures afterwards. It verifies matches beyond 5,000 nodes, document offsets beyond 200, global type counts, deterministic order, empty/final pages, and rejected query parameters. Changed frontend files pass ESLint and the full frontend passes TypeScript checking.

Commands: `python -m unittest tests.test_cache tests.test_ingestion -q`; `NEO4J_TEST_URI=bolt://127.0.0.1:17687 python -m unittest tests.test_browsing -q`. Use the repository's disposable datastore procedure and Python 3.12 environment. No production resources or model endpoints are used. These checks do not establish dense-graph search latency, retrieval ranking quality, browser rendering behavior, or distributed mutation consistency during Redis partitions.

The separate [browser validation](../audits/accuracy-ui-validation.md) subsequently passed sixteen synthetic UI scenarios, including server pagination, errors/retry, graph search and expansion, relationship source support, review resolution, source navigation, interrupted answers, and hub pagination/stale-response handling. Hub checks reach all 303 matching documents, preserve the page per domain, retry a controlled 503, and release old success/error responses while the new domain remains pending to verify its loading state survives. The report records screenshots and guarded, repeatable test runners. Hub changes also pass ESLint and full frontend TypeScript checking.
