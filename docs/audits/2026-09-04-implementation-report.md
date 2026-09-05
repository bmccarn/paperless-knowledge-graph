# Accuracy and reliability implementation — September 4, 2026

**Completion status superseded:** the subsequent [completion review](2026-09-04-completion-review.md) reproduces eight additional defects. The test/build results below remain valid for the recorded source snapshot, but the table does not establish full closure of the original findings.

The requested exploration, specification and implementation are in the working tree. The [main specification](../specs/accuracy-and-reliability.md) defines the acceptance policy; the narrower explorations record alternatives and implementation tradeoffs. This report supersedes the open-status descriptions in the two historical audits. No production documents or model endpoints were used for validation.

## Delivered behavior

| Findings | Implementation | Evidence |
| --- | --- | --- |
| B01–B04 | One finalization boundary for ordinary/SSE delivery; complete answer units, exact source manifests, numeric/unit checks, bounded repair, revision-bound ledgers and explicit failure dispositions. Source windows replace synthesis prefix cuts. | Finalizer, hardening, evidence-input, query-delivery and ASGI persistence tests. |
| B05 | Valid calendar precision and source-bound timelines; explicit evaluation date; current claims cannot bypass policy through the planner. Supported historical facts may be qualified without claiming present status. | Temporal and timeline tests, including contradictory records and timeline-only citations. |
| B06, A03 | Human vetoes cover exact/fuzzy/LLM/bulk/manual paths. Identity snapshots survive supported rebuilds/merges; cached UUIDs are validated; steward confidence/risk thresholds are enforced. | Resolver/steward tests and transactional real-Neo4j merge tests. |
| B07 | Complete bounded overlapping extraction windows, real-source verification even for small entity lists, accepted endpoints, metadata conflicts and persisted quotes/rationale. | Positive 35,058-character document plus provider-failure, invented-endpoint and coverage tests. |
| B08 | Independent versioned factual fixtures replace keyword/self-confidence pass criteria; legacy questions are labeled smoke checks. | All six former fabricated zero-source responses fail; correct low-confidence facts, sourced timelines and expected abstentions pass. |
| B09 | Durable open/resolved feedback, explicit review UI, correction/hash guards, cache invalidation and audit quarantine. Original OCR remains inspectable. | PostgreSQL migration/lifecycle tests, ASGI cases and the browser correction flow. |
| A01, A04, A07 | Scan-start watermark, retryable failures, preparation before replacement, per-document reindex, completion markers and admission retained while cancellation drains. Manual mutations share admission. | Ingestion/task/failure tests; real startup; query snapshot and HTTP mutation checks. |
| A02, A08 | Request-scoped model/context; complete cache identity; corpus generations; Redis I/O off the event loop, namespace invalidation and bounded copied memory values. | Concurrent requests, histories, in-flight mutation and real Redis tests. |
| A05 | Idempotent per-document relationship support, source-preserving deletion, scoped orphan cleanup and transactional canonical merging. | Real Neo4j source/merge/rollback tests; inspector displays source quotes and rationale. |
| A09 | Valid 3,072-dimensional halfvec indexes with explicit approximate candidate search; exact retrieval remains the default. Dimension mismatch cannot erase data on startup. | Independent cosine baseline, real query plan and preservation tests. |
| A10 | Datastore search/pagination and exact matching counts for documents and graph search, with stale-response protection and retryable errors; domain hubs use the same paging contract. | 5,105-document Neo4j fixture and browser pagination checks beyond the old caps. |
| A06, A11, A12 | Refreshed frontend lockfile, Node 24, hashed Python lock, locked image installs, PR gates before publication, and one runtime backend URL for ordinary/SSE proxies. | Dependency scan, builds, workflow validation and real browser proxy flows. |

The graph viewer retains the earlier 2D-first rework, stable identities, source-aware inspector, explicit expansion, search and optional 3D. Repeated expansion does not inflate counts. Document and answer source markup remains literal text; only validated matching document citations become links.

## Validation record

The final run passed **153 backend tests with no skips**, **11 frontend regressions**, and **16 browser scenarios with zero runtime errors**. Frontend lint, TypeScript checking, the production build, both local Docker builds, workflow validation and Compose schema validation passed. The network-disabled backend image passed 128 tests with 25 explicit datastore-integration skips. The optional backend Ruff check still reports eight minor findings; backend lint is not claimed clean.

Final counts, source hashes and benchmark samples are in [implementation-validation.json](2026-09-04-implementation-validation.json). Raw [backend output](2026-09-04-backend-validation.log), [frontend build output](2026-09-04-frontend-build.log), and [real datastore startup/shutdown output](2026-09-04-datastore-startup.log) are retained. Backend error logs include deliberate failure fixtures; the final test result is successful. The [browser report](accuracy-ui-validation.md) includes screenshots and reproducible synthetic scenarios. The [dependency/build report](../specs/dependency-and-delivery-validation.md) records image builds, lock verification and the npm advisory change from 24 affected entries to zero.

Python behavior tests import real application modules. Model/provider responses are controlled. Datastore checks use explicitly selected local disposable Neo4j, PostgreSQL and Redis instances. The original audit's AST probes deliberately asserted defects and remain historical reproduction artifacts; they are not current CI gates.

The [backend reproduction procedure](accuracy-backend-validation.md) records the exact opt-in test configuration. All temporary application servers and disposable datastore containers were stopped after validation; the two validation images remain local.

## Accuracy limits and rollout implications

- Semantic entailment remains a model judgment. Deterministic checks establish provenance, coverage and known consistency rules; they do not prove all OCR, model claims or retrieval results correct. The displayed audit score is not a calibrated probability.
- Extraction and source selection have explicit budgets. Documents exceeding extraction limits remain incomplete rather than silently losing tails. Numeric/date/unit validation is conservative; unsupported conversions, calculations and some valid paraphrases can require review.
- Current real-world status cannot be inferred from a latest document date or an apparently active term alone. Historical facts remain useful when explicitly qualified; no completeness claim is made about records outside the retrieved set.
- Feedback conservatively prevents disputed document evidence from certifying an answer until review; it does not edit original OCR or automatically assert that reindexing corrected a fact.
- Cross-store replacement is recoverable, not atomic. Admission is process-local; distributed multiworker coordination and live-corpus model precision/recall are not established by these tests.
- Legacy overwritten edge provenance, stale extracted facts and old saved answers are not retroactively repaired merely by changing code. Reviewed reindexing is needed to regenerate source support. Existing saved messages retain their historical metadata.
- Synthetic browser checks establish functional behavior, not production-scale graph frame rate. The small vector benchmark does not justify enabling approximate retrieval for the user's corpus.

The implementation remains local and uncommitted. The next deployment should use the locked builds and a reviewed corpus evaluation; synthetic passes are not a claim of production answer accuracy.
