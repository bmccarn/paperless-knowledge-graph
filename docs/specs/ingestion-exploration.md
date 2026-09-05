# Ingestion recovery exploration and specification

The existing completion-time checkpoint excludes documents that fail unchanged and documents modified while a scan runs. Cancellation marks a task terminal while its document writers still run. Targeted reindex deletes graph/chunks before model preparation, whereas ordinary processing prepares extraction first. Query caches clear before writes but can retain intermediate answers afterwards. A failed vector call can silently return without a chunk while the completion hash still commits.

## Selected implementation

Keep a small sequential prepare/replace lifecycle rather than introducing a distributed transaction framework. Capture a UTC scan-start watermark before any Paperless fetch. Retain the previous checkpoint after any processing/deletion error or cancellation; only clean scans advance to scan start. Reconcile a complete Paperless snapshot against graph, chunk, and completion-hash ID sets, adding missing derived documents to the incremental candidates and purging orphaned state from all three stores. This catches interrupted targeted replacements even when their Paperless modification date predates the checkpoint.

Force reindex shares `process_document(..., force=True)` and rebuilds each document in place. Fetch the corpus before mutating it; do not globally clear usable derived state. Prepare classification, complete extraction, text chunks, optional summary, and all document vectors before replacement. Partial/failed extraction and empty vectors are errors before graph/chunk deletion. Preserve extraction coverage and source-span metadata. Immediately before the first replacement mutation, remove the completion hash. Commit it only after required graph/chunk writes succeed. This marker supports convergence on retry; it does not make Neo4j and PostgreSQL atomic. Readers may observe partial replacement after a datastore failure until retry.

Before deleting graph entities, hydrate existing human review identities so UUID replacement does not erase review intent. The resolver validates cached identities against current graph state. Preserve relationship rationale, inferred/explicit status, and original OCR source spans on graph records.

Use cooperative cancellation: set `cancelling`, stop admission of new documents, let already admitted writers drain, then publish `cancelled`. Both running and cancelling tasks retain the ingestion slot. Final status is failed for errors, cancelled for cancellation, completed only for clean work. Partial targeted repair never advances the corpus checkpoint, because its target list cannot establish that all source changes were handled. Invalidate shared query/graph caches after attempted mutations and task completion, including failed partial writes.

## Rejected alternatives and limits

- Advancing only to scan start does not retry unchanged failures. Holding the checkpoint handles incremental failures without another retry table; missing completion markers cover targeted partial writes.
- Deleting all data then rebuilding is unnecessary for a full reindex and makes model outages destructive. Rebuilding documents individually preserves useful data during preparation and stable unrelated entities.
- Cross-database rollback or generation-swapped shadow indexes would improve read consistency but requires a broader storage redesign. This change reports incomplete replacement, leaves retryable markers, and does not claim atomicity.
- Task admission remains process-local. Cooperative cancellation and clean status do not provide a persistent distributed task queue or prevent separate API worker processes from starting independent ingestion.
- A moving remote paginated source is not a snapshot transaction. Scan-start checkpoints and full reconciliation reduce lost work, but a Paperless server changing page membership during pagination can require a later reconciliation scan.

## Acceptance

Real imported modules with controlled Paperless/model/store adapters must demonstrate: unchanged failures retry; a document modified during a paused scan remains eligible next scan; missing hash/chunks/graph recover despite old modification dates; deletion failures retain the checkpoint; targeted classification/extraction/vector failures preserve the old graph/chunks; partial storage writes never commit a success hash and retry converges; partial extraction cannot commit; skipped-tag purge removes all derived stores; cancellation rejects competing task admission until active writers finish and reports cancelled; errors report failed; completion and partial writes invalidate caches. Positive clean sync/reindex must process successfully and advance only the eligible watermark.

No tests call production datastores, Paperless, or model endpoints. Fault-injection tests demonstrate orchestration behavior; real Neo4j/PostgreSQL integration remains necessary to assess underlying transactional behavior.

## Implemented validation

`/private/tmp/paperless-accuracy-venv/bin/python -m unittest tests.test_ingestion -q` passes 18 tests under Python 3.12. The suite imports the actual pipeline and task endpoints, replaces only Paperless/model/store/cache dependencies with controlled adapters, and checks both negative and positive outcomes. It includes cancellation admission/drain, interrupted replacement recovery, metadata-only vector failure, long OCR preservation, source-modification races, failed task status, targeted-repair checkpoint behavior, and invalidation of answers cached during successful or failed replacements. Dashboard task-state changes pass ESLint. The fixtures validate ordering and recovery decisions, not real datastore transaction isolation or production model accuracy.
