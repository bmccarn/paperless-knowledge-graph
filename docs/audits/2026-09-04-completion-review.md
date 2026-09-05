# Completion review — September 4, 2026

**Follow-up:** all eight findings below were subsequently fixed and validated on `codex/accuracy-completion-fixes`. See the [closure report](2026-09-04-completion-closure.md) for current status. This review and its probes preserve the pre-fix evidence.

**Verdict: incomplete. Eight additional defects are reproduced: three P1 correctness/integrity blockers and five P2 defects.** The earlier implementation report overstated completion. Its passing test results remain valid, but its blanket per-finding closure does not. Application source was unchanged during this review; only audit artifacts and status documentation were added.

The review compares the current working tree, including new files, with baseline `caaaa5a86d2eb17c5d4260a42077f488646242e6`. All 118 source/configuration hashes in the earlier validation record still match. The installed review skill's standards and specification axes were reviewed independently; an additional local review covered query finalization, source handling, cache delivery and the enabled model adapter. Findings below are observed defects, not speculative code smells.

## Standards axis

### C01 — P1: generated summaries can certify themselves as source evidence

`app/pipeline.py:360-362` stores a generated document summary in the document embedding table at chunk index `9999`. `app/evidence.py:142-147` admits that chunk, and `app/answer_finalization.py:67-89` converts it into quoteable evidence without enforcing source origin. The actual summary prefix is retained in the reproduction.

**Observed:** original OCR says `$321`; the generated summary says `$999`; a candidate quoting the summary returns **supported**, complete, score `0.8`, with a Document 101 citation. The controlled auditor finds the value actually present in the text supplied to it. This establishes a provenance defect; it does not measure how often a live auditor would make the same decision. Even the evidence item's heuristic `summary` label does not prevent certification.

This conflicts with `CONTEXT.md:3` (Paperless is the source; extracted state is derived) and the source requirements in the [main spec](../specs/accuracy-and-reliability.md#answer-finalization-and-evidence-b01b05). Generated summaries may guide retrieval, but original OCR must supply certifying quotes. Filter existing legacy `9999` records as well as new generated records; separate source content from generated metadata. Cover strict answers, timeline events and public evidence, including when original-source hydration fails.

Evidence: [probe](completion-review/summary-source-probe.py), [result](completion-review/summary-source-result.json).

### C02 — P1: an in-flight entity lookup can bypass an accepted split

`app/entity_resolver.py:647-665` snapshots review decisions before awaiting lookup. Automatic resolution does not hold the lock used by decision recording. The split route at `app/main.py:1503-1505` remains available during ingestion.

**Observed:** pause a same-name lookup for document 22 after its old decision snapshot; successfully record a split between identities for documents 11 and 22; resume. Resolution returns the prohibited document-11 UUID. A fresh lookup returns the correct document-22 UUID. The probe establishes the returned identity; a subsequent ingestion relationship would use that identity.

The spec requires split/never-merge decisions to veto every entry point. Serialize decision acceptance and identity assignment, or place decision writes under ingestion admission. Cover person/organization redirects without nested-lock deadlock. Acceptance must specify which operation takes effect first and prevent an acknowledged decision from being silently ignored.

Evidence: [probe](completion-review/entity-review-race-probe.py), [result](completion-review/entity-review-race-result.json).

### C03 — P2: metadata edits disappear behind an advanced sync checkpoint

`app/pipeline.py:324-328` skips documents based only on OCR content. Sync nevertheless advances the checkpoint at `1442-1445`.

**Observed:** correct the source title and created date without changing OCR. Sync reports completed, processed zero, skipped one, and advances its checkpoint. The old graph title and embedding content remain; the next sync selects zero documents. This contradicts the documented freshness model and clean-scan requirement.

Track ingest-relevant metadata in an ingestion fingerprint or deliberately refresh changed metadata. Preserve the distinction between that fingerprint and the OCR hash used by feedback review. Existing indexes need a migration/reconciliation path. Test title/date/type corrections without OCR changes and verify subsequent freshness truthfully reflects them.

Evidence: [probe](completion-review/metadata-sync-probe.py), [result](completion-review/metadata-sync-result.json).

### C04 — P2: full reindex hides reported entity-resolution errors

`app/pipeline.py:1436` ignores the result from `resolve_all_entities()`. That method catches individual merge exceptions and returns them in `errors` (`app/entity_resolver.py:886-889,927-930`), so the outer exception handler never sees them.

**Observed:** an entity-resolution result containing a database failure still produces completed, errors zero, and an advanced checkpoint. This violates the [ingestion specification](../specs/ingestion-exploration.md), which reserves completed for clean work.

Inspect returned errors, retain the previous checkpoint on postprocessing failure, and expose failed/partial status. Add a regression for a returned error report alongside the existing raised-exception cases.

Evidence: [probe](completion-review/reindex-postprocess-probe.py), [result](completion-review/reindex-postprocess-result.json).

Standards result: four confirmed findings; highest severity P1 in source provenance and concurrent review enforcement. No additional high-impact style hypothesis is asserted.

## Specification axis

### C05 — P1: delayed answers cross conversation boundaries

`frontend/src/app/query/page.tsx:521` unconditionally replaces displayed messages when a request completes. Navigation at `584-588` and New Conversation do not invalidate that request.

**Observed in Chromium:** hold conversation A's response, select B, release A. A's answer replaces B's displayed history. A subsequent question is sent with B's ID, despite being prompted by the answer from A. This breaks the requirement that each request owns its conversation context.

Bind request, progress, completion, recovery and loading updates to a conversation/view generation. Backend work may finish for its owning conversation while the current view remains separate. Test switching to another saved conversation and starting a new one during a delayed response.

Evidence: [probe](completion-review/conversation-switch-race.mjs), [screenshot](completion-review/conversation-switch-race.png).

### C06 — P2: reindex completion overwrites a newer document search

`frontend/src/app/documents/page.tsx:143,163` invokes a captured `fetchDocs` after single/batch reindexing. That callback still owns the old search parameters and presents its response as the newest request.

**Observed in Chromium:** start reindexing; search `Synthetic archive 300` and see one match; complete reindexing. All 303 records replace the filtered result while the query text and heading still claim a matching view. This violates the [browsing spec](../specs/cache-and-browsing-exploration.md) requirement that stale requests cannot overwrite newer searches.

Refresh using current view state or invalidate a state-driven fetch. Test reindex completion after changing search, filter and page, for both single and batch operations.

Evidence: [probe](completion-review/document-reindex-search-race.mjs), [screenshot](completion-review/document-reindex-search-race.png).

### C07 — P2: initial graph loading discards an intervening selection

`frontend/src/components/graph/graph-container.tsx:93` replaces graph state from an empty graph when the initial sample arrives. Search selection at `141-147` can already have added and selected a node.

**Observed in Chromium:** delay the initial sample, find/select `synthetic-search-000`, then release the sample. The selected node is discarded and its inspector disappears. The viewer still has a reproducible interaction defect despite the earlier stable-expansion checks.

Preserve graph interactions performed during initialization or defer them until it settles. Test initial loading and reset/seed changes against search selection and neighborhood expansion; repeated expansion must still preserve identity and counts.

Evidence: [probe](completion-review/graph-initial-selection-race.mjs), [screenshot](completion-review/graph-initial-selection-race.png).

Specification result: three confirmed findings; highest severity P1 in conversation isolation. No material scope creep identified.

## Additional query/cache review

### C08 — P2: cache hits bypass the in-flight corpus-change guard

`app/query.py:458-461` returns a cached answer immediately after awaiting the cache read. The generation/incomplete-document checks at `550-565` run only on the uncached path.

**Observed:** warm a supported answer; pause a subsequent cache read after obtaining its value; invalidate the corpus; resume the read. The request still returns cached, supported, complete, with the old value. The controlled suspension models asynchronous Redis I/O. This is an in-flight stale response, not evidence that all subsequent cache requests remain stale.

Use a shared delivery check for cached and freshly computed answers. Recheck generation and relevant completion state before returning a hit; on change, retry under a new snapshot or return the indexing/retry disposition. Include ordinary and SSE delivery in the regression.

Evidence: [probe](completion-review/cache-hit-race-probe.py), [result](completion-review/cache-hit-race-result.json).

## What remains validated

- Fresh combined backend run: **153 passed, zero skipped**, against fresh disposable Neo4j, PostgreSQL and Redis services. [Raw output](completion-review/backend-datastore-tests.log).
- Fresh frontend run: **11 tests passed**, ESLint passed, full TypeScript checking passed.
- The **actual enabled Strands/LiteLLM adapter** invoked a synthetic localhost HTTP provider and returned the expected audit JSON. This closes a runtime-adapter check omitted by the disabled-provider unit fixtures; it does not evaluate a real model. [Probe](completion-review/enabled-model-adapter-probe.py), [result](completion-review/enabled-model-adapter.log).
- Fresh npm audit: **zero affected entries**. [Registry response](completion-review/npm-audit.json).
- Matt Pocock setup: **25 skills, all 74 vendored-file hashes match** the pinned provenance manifest.
- All **118 previously validated source/configuration hashes match**. The earlier successful production and Docker builds remain evidence for that exact source snapshot; no image was published or deployed during this review.
- The new review produced **eight defect reproductions**, including three real browser races. These are separate from the passing acceptance suite. The prior 16 positive UI scenarios did not cover these interactions.

All review services were stopped and the three disposable datastore containers removed. Application code remains local and uncommitted. The [machine-readable record](completion-review/results.json) captures counts, findings, source identity and artifact paths. [Reproduction instructions](completion-review/README.md) explain the controlled probes.

## Completion gates still open

Close C01–C08 with behavioral regressions that fail before each fix and pass afterward, then update the acceptance suite and validation record. The standards and specification axes both currently fail completion.

Production model accuracy, retrieval precision/recall over the actual archive, and representative dense-graph interaction performance remain unmeasured. These were declared outside the local implementation's validation scope; they are still required before claiming the application meets the user's accuracy and graph-usability goals in production. Existing derived data and saved answers are not retroactively corrected by code changes. Cross-store replacement remains recoverable rather than atomic, and mutation admission remains process-local. Eight minor optional backend Ruff findings also remain recorded; they are separate from the eight functional defects above.

No percentage-complete estimate is assigned: the remaining provenance and isolation defects are significant regardless of how many checks pass.
