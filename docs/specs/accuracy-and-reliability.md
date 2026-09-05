# Accuracy and reliability implementation specification

Status: locally implemented and regression-validated, including closure of C01–C08 from the [completion review](../audits/2026-09-04-completion-review.md). See the [closure report](../audits/2026-09-04-completion-closure.md) for current evidence and the remaining production evaluation/migration limits, and the [implementation report](../audits/2026-09-04-implementation-report.md) for prior validation. Authorized by the request to explore, specify, implement, and validate the remaining audit gaps. This specification is local to the repository; no issue publication or production operation is required.

## Problem and scope

Users need faithful answers and graph facts derived from their Paperless documents. The functional audit reproduced acceptance of unsupported strict answers, fabricated evidence, incomplete verification, incorrect current-state labels, ignored entity decisions, incomplete extraction and ineffective quality evaluations. The initial audit also found request-state leakage, missed ingestion work, lost relationship support, incomplete browsing and weak validation gates.

The implementation addresses B01–B09 and the related A01–A12 findings. Correctness is the acceptance criterion; counts of model calls, successful HTTP responses and model confidence are not substitutes. Existing graph viewer improvements remain in place. Production data/model evaluation and destructive deployment are outside this local implementation; all database tests use synthetic disposable stores.

## Exploration decisions

Three independently explored finalization interfaces were considered:

1. A minimal finalizer owning segmentation, provenance, audit coverage, bounded repair and the final verdict.
2. A general typed event pipeline with resumable stages and replayable revisions.
3. An answerer that assembles all prose exclusively from validated atomic claims.

Choose the minimal finalizer with a sequential implementation and progress callbacks. It provides one acceptance policy with lower integration cost than a general workflow engine. Retain the useful constraint from claim assembly: no model rewrite after acceptance. Ordinary and streaming delivery consume the same completed result. Stream progress while processing; buffer factual answer text until acceptance, avoiding provisional text being saved or promoted after transport errors.

The model is a true external dependency with production and scripted-test adapters. Neo4j/PostgreSQL/Redis have disposable local adapters. Provenance, date parsing, coverage accounting and grading are deterministic in-process behavior. Persistence and HTTP delivery stay outside the finalizer.

Detailed entity, ingestion, and extraction explorations accompany this spec in this directory.

## Required behavior

### Answer finalization and evidence (B01–B05)

- Each request owns its model, conversation context, evaluation date and evidence snapshot. No mutable singleton model override.
- Partition the complete candidate answer into bounded server-owned text units with exact offsets; every non-whitespace region is accounted for. No silent answer prefix truncation and no model-controlled exemption for unchecked text.
- Audit units using exact evidence spans selected from full chunk content, including relevant tails. Bind results to the current answer/evidence revision. Explicit limits produce incomplete coverage, never verified status.
- A supported unit requires an affirmative semantic assessment and deterministic reference validation: known evidence ID, matching document ID, exact quote under NFC/whitespace normalization, and a source span actually included in the audit input. Preserve digits, punctuation, signs, dates and units. Source titles come from the evidence snapshot.
- Quote membership establishes provenance, not entailment. Numeric/date inconsistencies and disagreements remain unresolved. Retrieved source quantity cannot override an unsupported, conflicting, unknown or unchecked unit.
- Audit before deciding repair. Missing evidence, malformed references, late ledger failures and conflicts can trigger one bounded repair. Re-audit the entire repaired text with a new revision. An empty/failed/repeated repair cannot inherit a successful audit.
- Deep, timeline and strict factual answers require the complete gate; otherwise return deterministic evidence-limited text without repeating rejected precise claims. Quick mode is explicitly unaudited and cannot receive a supported/high-trust verdict. Empty evidence never becomes retrieval-backed certainty.
- Timeout, provider failure and malformed output converge on explicit terminal dispositions. Ordinary, SSE completion, caching and saved conversations use the same final payload.
- Use explicit UTC evaluation time. Distinguish latest observed document date, historical facts, effective/expiry intervals and established current status. A dated or newest document alone cannot establish current state.
- Timeline candidates require valid calendar dates with honest precision, source membership, quoted support, and assessment of the date's meaning. Document-date fallbacks must be labeled document dates. Reject invented events and unknown sources.
- Public evidence contains every referenced item and exact support excerpt; display limits cannot orphan citations. Cache keys include complete relevant history, selected model, policy version, corpus generation and evaluation date.

### Entity review and relationship provenance (B06, A03, A05)

- Split/never-merge decisions veto every automatic and manual merge entry point. Queue hiding is not enforcement. Low-confidence/high-risk model suggestions do not bypass thresholds.
- Record identity snapshots with decisions and hydrate legacy decisions while their nodes exist. Preserve veto associations across canonical merges/rebuilds; unresolved identities require review rather than a guessed merge.
- A cached entity UUID must exist before reuse. Invalidate identities on graph mutation; do not share stale identities across graph generations.
- Shared graph relationships retain explicit per-document support. Repeated ingestion of the same source is idempotent. Deleting one document removes only its support, retaining another's edge. Clean only affected orphans, avoiding the global sweep that races unrelated entity creation.

### Extraction and feedback (B07, B09)

- Process complete document content in bounded overlapping windows with source offsets and a coverage report. Missing/failed windows prevent a complete extraction verdict.
- Validate entities against actual source content, including small lists. Relationship endpoints must belong to accepted source-grounded entities. Retain supporting quotes/spans, inferred status and rationale.
- Reconcile window metadata deliberately. Conflicting values are surfaced, not silently overwritten by whichever window finishes last.
- Feedback has explicit open/resolved states and a review interface. Flagged derived facts are identifiable during retrieval and answer auditing. Original OCR remains inspectable. Successful explicit correction/reindex can resolve a flag and invalidate affected answers; merely recording a flag cannot claim correction.

### Ingestion and task lifecycle (A01, A04, A07)

- Capture scan-start watermark. Advance only after a clean scan; failed, cancelled and partially purged work remains retryable. Modifications arriving during a scan are considered next time.
- Cancellation enters a cancelling state and retains admission ownership until in-flight work drains. Task completion cannot overwrite a cancelled result.
- Fetch/classify/extract before replacing usable derived state. Record incomplete replacement via missing completion state; reconciliation must retry it. Cross-store writes are recoverable, not falsely described as atomically transactional.
- Rebuild per document instead of dropping the entire corpus before preparation. Successful final mutations invalidate query caches; partial failures cannot retain a completed hash or authoritative cached answer.

### Completeness, performance and delivery (A06, A08–A12)

- Use datastore-side graph/document search with deterministic ordering, pagination and totals. Document and hub views must not label a capped subset as the entire archive. Requests handle stale responses, empty results and errors.
- Keep Redis I/O off async request loops, avoid keyspace-wide blocking KEYS, and bound the in-memory fallback. Cache identity must change after completed corpus mutations.
- Provide a valid vector-index strategy for configured dimensions with explicit exact/approximate behavior. Compare indexed retrieval to an exact baseline on synthetic data; retain accuracy-preserving defaults unless measured evidence supports approximation.
- Refresh affected dependencies through reviewed lockfiles; validate install, production advisory scan, types, lint and build. Use locked installs in images.
- Share backend proxy configuration for ordinary and streaming requests. Align documented setup/runtime versions and add PR validation gates before image publication.

## Acceptance and testing interfaces

Tests observe results through the finalizer, QueryEngine ordinary/streaming interfaces, public extraction/resolver functions, ingestion/task entry points, storage interfaces and HTTP/browser flows. Use known literal expected results and external adapters, not AST-extracted implementation bodies or assertions that reproduce the algorithm.

At each meaningful behavior, first run a failing regression, implement the smallest coherent change, and confirm the same case passes. Include positive supported/correct cases alongside failure cases.

| Area | Required cases |
| --- | --- |
| Finalizer | supported premium; wrong digit/unit; wrong document/quote; missing unit; tail claim/evidence; unsupported ledger/coarse verifier disagreement; empty evidence; timeout; failed repair; new unsupported repair claim |
| Dates/timeline | impossible date; year/month precision; historical/expired/future/current intervals; unknown source; current-state ambiguity; document date is not event date |
| Delivery/cache | ordinary/SSE terminal equality; saved metadata equality; disconnect/error never finalizes draft; concurrent models/history; fresh corpus invalidates old results |
| Entity decisions | vetoed automatic/manual merges; low-confidence high-risk steward; canonical identity reassociation; stale UUID after delete/rebuild |
| Extraction | facts after 31,000 characters; failed window; repeated metadata; conflicting values; valid relationships; absent/rejected endpoints |
| Ingestion | failed document unchanged next scan; edit during scan; cancellation while a document is in flight; repair after partial writes; prepare failure retains existing state |
| Storage | two documents supporting one edge; repeat source idempotence; delete either source; scoped orphan cleanup; feedback lifecycle |
| Evaluations | all six fabricated zero-source responses fail; correct facts/sources pass; expected abstentions pass; wrong high-confidence values fail |
| Features | pagination beyond old 200/5,000 caps; last-page search; stale request response; API proxy destinations; graph regressions remain green |

Run unit/behavior tests on Python 3.12, disposable real datastore tests, frontend tests/types/lint/build, dependency scans and patch checks. Record measured limitations and per-finding status in the implementation report. Do not claim production answer accuracy from synthetic tests.
