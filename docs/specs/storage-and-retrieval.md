# Storage integrity and retrieval decisions

This supplements the [implementation specification](accuracy-and-reliability.md). The chosen design keeps exact vector retrieval, preserves source support per document, and treats completion hashes as recovery markers across datastores.

## Relationship support

A single `source_doc` property cannot represent two documents supporting the same edge. Storing duplicate edges per document would complicate graph identity and browsing. The implementation keeps one endpoint/type edge with `source_doc_ids` and JSON `support_records`, one record per source document. Each record preserves original confidence, inferred status, quotes and rationale. Re-ingesting the same source replaces its record without inflating weight. Canonical entity merges union same-source quotes and retain conservative inferred/confidence summaries.

Creation, deletion and canonical merging use Neo4j write transactions. Document deletion removes only that document's support; another source keeps the edge alive. Orphan cleanup considers only affected nodes, preserving unrelated entities being prepared by another document worker. Legacy scalar support is read conservatively; evidence already overwritten by older versions cannot be reconstructed without reindexing the source documents.

Review decisions store identity snapshots in PostgreSQL JSONB. Sorting UUID pairs also sorts their snapshots. Source-grounded names and document associations carry vetoes through supported canonical merges and reindexing, while ambiguous identity remains blocked for review.

## Vectors

The configured embeddings have 3,072 dimensions. pgvector's ordinary vector HNSW index supports 2,000 dimensions, while halfvec supports 4,000. The [upstream reference](https://github.com/pgvector/pgvector#hnsw) documents those limits and [half-precision indexing](https://github.com/pgvector/pgvector#half-precision-indexing).

`create_vector_indexes()` creates halfvec expression indexes. `vector_search()` defaults to exact distance ordering over the original vectors. An explicit `approximate=True` option retrieves halfvec candidates and reranks them using full-precision distance. Filtered, document-scoped and entity searches remain exact. There is no implicit switch to approximate search when indexes exist.

The real PostgreSQL regression uses 320 deterministic synthetic vectors with 3,072 dimensions and four query vectors. Exact result IDs match an independent NumPy cosine baseline. Halfvec indexes exist and appear in the forced index plan; candidate retrieval has recall@10 of 1.0 for those four queries. On this small fixture it was generally slower than exact search. See the measured timings in the [validation artifact](../audits/2026-09-04-implementation-validation.json). This is not a production benchmark or evidence of universal approximate recall.

Startup now refuses incompatible vector dimensions without dropping tables, hashes or source data. An operator must prepare an explicit migration; a regression preserves existing data when the mismatch is detected.

## Recovery and query snapshots

Document text, extraction coverage and document vectors are prepared before replacement. A completion hash is written last. Failed replacement leaves a missing marker for reconciliation, and active/failed replacement invalidates query caches. A query whose corpus generation changes or whose evidence documents lack completion markers returns an indexing/retry disposition instead of publishing an answer from that snapshot.

Graph mutations initiated by HTTP routes reserve the same process-local admission slot used by ingestion. A cancelling task retains ownership until its writes drain. Manual merge/delete cannot overlap that task in the supported single-process deployment. These mechanisms do not provide distributed admission or an atomic transaction spanning PostgreSQL, Neo4j and Redis.

## Validation

`tests/test_storage_integrity.py` uses actual Neo4j/PostgreSQL for shared support, deletion in either order, idempotence, scoped orphans, review snapshots, prepared vectors, dimension mismatch and the vector comparison. `tests/test_redis_storage.py` verifies namespace invalidation between clients, literal-prefix scanning and shared corpus generation against actual Redis. Other tests cover Redis outages, bounded memory, copies, event-loop progress and partial ingestion failures with controlled adapters.
