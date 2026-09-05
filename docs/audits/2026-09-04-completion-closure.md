# Completion closure — September 4, 2026

**Subsequent validation:** the [pre-publication review](2026-09-05-pr-review.md) fixed an additional relationship-evidence compatibility defect and records the latest test results. The results below describe the C01–C08 closure snapshot.

**C01–C08 are closed in the local implementation, with behavioral regressions and final validation.** Branch `codex/accuracy-completion-fixes` preserves all earlier work in checkpoint `ecde3d0ca0eded79caa2b724ff3a5ca7c3122534`. The subsequent changes implement the [closure specification](../specs/completion-defects.md). The [original completion review](2026-09-04-completion-review.md) and its failing probes remain historical evidence.

## Defect closure

| Defect | Corrected behavior | Regression evidence |
| --- | --- | --- |
| C01 — source provenance | Explicit origin/raw-OCR storage; generated summaries, legacy slot 9999 and index metadata cannot certify answers or events. Real query orchestration hydrates OCR or abstains. Answer-policy version invalidates old certified cache entries. | `test_source_origin.py` (5 tests), real PostgreSQL read/migration tests; [original red](completion-closure/c01-red.log), [metadata red](completion-closure/c01-metadata-red.log) |
| C02 — accepted splits | Resolver lookups and decision acceptance serialize; redirects remain within one lock. API split/ignore actions share ingestion admission, including cancellation. | Person, organization and generic resolution races; redirect and ASGI admission tests; [resolver red excerpt](completion-closure/c02-red-excerpt.log) |
| C03 — metadata drift | Versioned fingerprints reconcile metadata corrections and legacy records even behind the checkpoint. OCR hashes remain separate. Freshness and its dashboard explain the drift and expose targeted repair. | Ingestion/reconciliation, ASGI freshness, real migration and dashboard browser tests; [ingestion red](completion-closure/c03-red.log), [freshness red](completion-closure/c03-freshness-red.log), [dashboard red](completion-closure/c03-dashboard-red.log) |
| C04 — hidden reindex errors | Returned resolution errors fail completion, retain details and preserve the prior checkpoint. | Full-reindex result/checkpoint regression; [red](completion-closure/c04-red.log) |
| C05 — conversation isolation | View generations guard history, progress, completion, recovery failures and loading state. A question in the new view uses its own conversation ID. | Saved-conversation switch and New Conversation during failed recovery/new pending query; [completion red](completion-closure/c05-red.log), [recovery red](completion-closure/c05-recovery-red.log) |
| C06 — document refresh | Reindex invalidates a state-driven fetch using the current search, filter, sort and page. Operation errors remain separate from list errors. | Single and batch reindex completed after changing search, type and page; [red](completion-closure/c06-c07-red.log) |
| C07 — graph initialization | Initial/reset/seed samples merge with intervening selection and neighborhood expansion. Selected identity and inspector survive; repeated expansion retains five nodes/three relationships in this fixture. | Three delayed-sample browser cases plus graph-data tests; [red](completion-closure/c06-c07-red.log), [seed-change screenshot](completion-closure/browser/completion-ui/graph-seed-selection.png) |
| C08 — cache delivery | One public delivery check follows cache reads and writes. It reads relevant incomplete-document state, then checks generation, returning a retry disposition for a changed snapshot. | Suspended cache-hit and cache-write races through ordinary and SSE delivery, plus incomplete-source checks; [read red](completion-closure/c08-red.log), [write red](completion-closure/c08-write-red.log) |

The red logs preserve the test versions used during implementation; final tests include additional coverage and boundary improvements. The C02 excerpt retains the original resolver assertion failure; a separate mock-serialization setup error is omitted and is not claimed as defect evidence. Browser development also exposed an incorrect test locator (`alertdialog` versus the application's `dialog`); the corrected final suite passed without changing the application to satisfy that locator.

## Final validation

- **170 backend tests passed, zero skipped**, with disposable PostgreSQL/pgvector, Neo4j/APOC and Redis. This includes 27 datastore integration cases. [Log](completion-closure/backend-datastore-tests.log)
- **11 frontend unit tests passed**, ESLint and full TypeScript checking passed. [Tests](completion-closure/frontend-tests.log), [lint](completion-closure/frontend-lint.log), [types](completion-closure/frontend-typecheck.log)
- **24 production Chromium scenarios passed**: 8 new completion regressions, 12 existing accuracy flows, 1 relationship-support inspection, and 3 domain-hub flows. Browser assertions recorded no uncaught page errors. The runner uses the standalone server and asset layout used by Docker, with an owned synthetic backend. Selected query and graph screenshots were also visually inspected. [Log](completion-closure/browser-production.log), [new results](completion-closure/browser/completion-ui/results.json)
- **Production frontend build and both linux/amd64 Docker builds passed.** The backend image also ran the offline suite with networking disabled: 143 passed, 27 datastore cases intentionally skipped. Tests, evaluation scripts and evaluation fixtures were mounted read-only; none belongs in the production image. [Frontend build](completion-closure/frontend-build.log), [backend image build](completion-closure/docker-backend.log), [frontend image build](completion-closure/docker-frontend.log), [image tests](completion-closure/backend-image-tests.log)
- **Zero npm vulnerabilities** including development dependencies; workflow validation with actionlint and patch whitespace checks passed. [Audit](completion-closure/npm-audit.json)
- **25 Matt Pocock skills / 74 vendored files** still match the pinned provenance manifest. The new browser suite uses pinned Playwright and runs in the existing CI validation job before image publication is eligible.

Exact source hashes, image identities, counts and artifact hashes are recorded in [results.json](completion-closure/results.json). All owned UI services were stopped and the three disposable datastore containers removed. No push, publication, deployment, production document mutation or live model call was performed.

## Standards

The independent review identified one validation-boundary finding: C01 query tests initially overrode internal retrieval/synthesis methods. Those tests now use an unmodified `QueryEngine`, the actual OpenAI SDK with a scripted HTTP transport, and external model/storage/Paperless adapters. All five source-origin tests passed. The follow-up review found no remaining material standards findings or material code smells.

## Spec

The independent review identified two integration gaps: mutation during an awaited cache write, and the dashboard's missing fingerprint-drift presentation/repair action. Both received behavioral regressions, were corrected, and passed final validation. The follow-up review found no remaining material specification findings or scope creep.

Review totals: Standards 1 found/1 closed (validation boundary); Spec 2 found/2 closed (cache delivery and freshness UI). Zero unresolved findings on either axis.

## Remaining production work

Local closure of these eight defects does not establish production model accuracy or dense-graph performance. The reviewed archive evaluation, retrieval precision/recall and representative graph-interaction measurements remain outstanding. Existing saved conversation text is historical; code changes do not retroactively correct it. After deployment, database initialization adds origin/fingerprint fields and a successful sync reconciles legacy derived records. Cross-store replacement remains recoverable rather than atomic; resolver/admission locks remain process-local. The earlier optional Ruff observations are outside these eight functional defects.

Run instructions and the validation environment are in [the artifact README](completion-closure/README.md).
