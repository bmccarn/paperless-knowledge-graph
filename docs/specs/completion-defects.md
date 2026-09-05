# Completion defects C01–C08

Scope: close the eight reproduced findings in [the completion review](../audits/2026-09-04-completion-review.md), preserving the earlier work in checkpoint `ecde3d0` on `codex/accuracy-completion-fixes`. That review supplies the original triggers and failing observations. This specification defines the corrective behavior; the closure report records validation of the final implementation.

## Acceptance

| Finding | Required behavior | Public validation boundary |
| --- | --- | --- |
| C01 | Generated summaries and index metadata cannot certify answers or timeline events. New chunks carry explicit origin and separate raw OCR. Legacy summary slot 9999 stays excluded. Summary-only retrieval must hydrate original OCR or abstain. Public references resolve to the actual OCR. Change the answer policy version so previously certified cache entries are not reused. | Evidence/finalizer, ordinary query delivery, timeline validation, PostgreSQL retrieval methods |
| C02 | Review decisions and automatic identity lookup have a defined order. A lookup already holding the resolver lock completes before a split can be acknowledged; subsequent lookups honor the accepted split. API decision acceptance shares ingestion admission, including cancellation. Person-to-organization redirection must not acquire the same lock twice. | Resolver methods, ASGI review routes |
| C03 | OCR hash remains the source-content identity for feedback. A separate versioned ingestion fingerprint covers OCR and ingest-relevant source metadata. Sync reconciles the full source snapshot against this fingerprint, including legacy null records and corrections older than the checkpoint. Freshness reports the same drift and repair selects it. Only successful completed document writes store the new fingerprint. | Ingestion, ASGI freshness, PostgreSQL completion records |
| C04 | Returned entity-resolution errors fail full-reindex completion just as raised exceptions do. The prior checkpoint survives, and the task report retains error details. | Full reindex task result and persisted checkpoint |
| C05 | Each query and history request owns a view generation. Changing conversation, New Conversation and unmount invalidate every older request's progress, completion, recovery, error and loading updates. A new question belongs to the newly selected/created conversation. | Real browser with delayed SSE and failed history recovery |
| C06 | Single and batch reindex completion refresh the currently displayed search, filter, sort and page. A refresh triggered by an older action must not invoke its captured search callback. Reindex failures remain visible independently of a successful list refresh. | Real browser with delayed task polling and changed search/filter/page |
| C07 | Starting an initial/reset/seed request clears the previous graph once. Its eventual sample merges into interactions made during that request. Superseded request epochs remain rejected; selection, inspector and neighborhood expansion survive arrival without duplicate identities. | Real browser and graph-data regressions |
| C08 | Cached and newly computed answers use the same final delivery check: read relevant incomplete-document markers, then recheck corpus generation. Changed snapshots return the indexing/retry disposition, never a supported old answer. | Ordinary query and SSE delivery, including a suspended cache hit |

## Implementation and migration

PostgreSQL initialization adds nullable `document_hashes.ingestion_fingerprint`, `document_embeddings.source_content`, and `source_kind` defaulting to `legacy`. Initialization remains repeatable. Existing OCR hashes are preserved. New source policy `source-origin-v2` selects legacy documents for reconciliation on the next sync; until then, retrieval excludes reserved generated summaries and strips the known legacy index-metadata envelope before certification. New ingestion writes raw OCR separately from embedding input and labels summary/metadata records explicitly.

The resolver's existing mutation lock serializes lookup and accepted decisions; private resolution helpers permit redirects within one lock. This remains process-local, as is ingestion admission. Document refresh is a state invalidation so React runs the fetch with current view parameters. Graph sample merging and query view generations extend the existing stale-response mechanisms.

## Validation and limits

Each original defect requires a regression that fails on its old behavior and passes after correction. Run the full backend suite with opt-in disposable datastores, frontend unit tests, lint, types, production build, and production browser contracts. Browser tests use the repository's synthetic fixture and a pinned Playwright development dependency; CI starts and stops its own fixture and production frontend. Rebuild both local Docker images after source changes.

This work does not deploy or rewrite production data. A deployed migration and subsequent successful reconciliation are necessary to refresh existing derived metadata. Saved conversation text remains historical. No model-accuracy percentage, production retrieval precision/recall, or dense-graph performance claim follows from synthetic regression results. Those require the reviewed archive and representative workload specified by the broader accuracy plan.
