# Issue 16 and strict-query acceptance closure

Status: implementation in progress. Owner: Codex in the current user session, sole live-data/deployment operator. Requested on 2026-09-07: write a detailed working spec, then implement all seven outstanding areas described in the investigation.

## Starting evidence and scope

- Tracker: [issue 16](https://github.com/bmccarn/paperless-knowledge-graph/issues/16).
- Source baseline: remote main `30773c66da446d5305a35ea2fcb6a56c088e2c4e`, including PR15's legacy-history safeguard.
- Implementation branch: `fix/kg-acceptance`, isolated worktree `/private/tmp/kg-acceptance`. Includes local token-cap fix `9f8e165`.
- Runtime verified September 7: API/frontend PR14 `f7614e2`; GitOps PR136 open, not deployed. Automatic sync / Steward / document concurrency remain staged at 0 / 0 / 2 until acceptance.
- Investigation: `docs/audits/2026-09-07-issue16-investigation.md` in the original checkout. Private source/provider/rollback artifacts stay outside this public repository.
- Overnight strict insurance request: real output-limit exceptions, then audit timeout; candidate contained 56 units, requiring 14 sequential calls under a shared 60-second deadline.
- Reproduced defects: citation metadata enters numeric fact validation; case-only proposal/review changes discard source-supported entities and positive aliases.
- Invalid/unproven assumptions: the six canary vector-name differences are orthographic; the final alias fixture supplied graph type DocumentRef to an extraction validator expecting Document. Neither establishes a wrong model identity decision.

This plan covers implementation, independent review, exact-revision validation, release, representative live acceptance, justified bounded reconciliation, ordinary fingerprint migration, final corpus proof and schedule restoration. Public tracker updates may contain only sanitized findings and aggregate evidence.

## Requirements and acceptance matrix

| ID | Requirement | Required acceptance evidence | State |
| --- | --- | --- | --- |
| Q1 | No application output cap in any Strands helper | Actual SDK request serialization omits every output-limit field; provider truncation still fails certification | Local fix 9f8e165; carried into this branch |
| Q2 | Valid source attributions do not become false numeric claims | Real finalizer accepts supported quote plus verified title/ID attribution; wrong/ambiguous metadata, factual numbers and adversarial citation text fail; initial and repaired answers behave identically | Pending |
| Q3 | Audit execution scales with the work and does not serialize unrelated queries | 56-unit public-query regression completes with controlled latency; bounded concurrency, finite per-call/wave deadlines, cancellation and isolated transport cleanup; ordinary/SSE payloads agree | Pending |
| I1 | Review casing preserves the proposed source identity and its receipts | Public extraction through binding/persistence positive regression; wrong name/hint/type, ambiguous identities, denied/conflicting reviews and repeated windows remain conservative | Pending |
| I2 | Real source aliases have a valid positive acceptance control | Correct extraction type, exact source/hash/span, accepted endpoints, persisted trusted alias, negative neighbor and repeat-processing evidence | Pending |
| V1 | Vector consistency has an explicit, tested contract | Exact UUID/type/dimension checks plus separately reported literal and policy-name/authorized-alias comparisons; different identities fail; verifier revision/hash retained | Pending |
| V2 | Historical vector drift is classified and any repair is justified | Private exact-identity manifest, protected preimages, independent dry-run review, writer/state guards, no-write second run, post-repair acceptance | Pending |
| R1 | Intended code including PR15 is running | Exact-head offline and disposable-datastore/frontend checks, independent standards/spec review, immutable image digests, GitOps rollout and actual runtime predicate checks | Pending |
| R2 | Real supported queries succeed on intended release | Original insurance question and retained exact-source canary query rerun once per diagnostic change; source-grounded useful answer or documented genuine evidence limitation, matching final/ledger digests, no timeout/citation false rejection | Pending |
| U1 | Visually verify all UI surfaces and their meaningful interactions with computer/browser control | Inventory routes/features; inspect rendered navigation, chat modes, conversation history/isolation, document list/detail/refresh, graph expansion/filter/selection/source inspection, review/feedback, domains/settings/status, loading/empty/error states and responsive layout; retain a per-flow visual test record and fix reproduced UI defects | Pending |
| M1 | Full derived corpus is current | One ordinary sync task admitted and recorded, exact eligible/processed IDs, source hashes, new extraction fingerprint, coverage/tails, dimensions and identity checks on two stable snapshots | Pending |
| M2 | Normal operation is restored | Actual post-sync Steward/database writer drain, GitOps controls restored to 60 / 360 / 10 while preserving tested immutable images, Flux/readiness/external health verified | Pending |

## Q2: separate attribution metadata from audited factual prose

The application already renders final citations from validated references. Normalize only recognized attribution forms against certifying evidence metadata before splitting or auditing the candidate:

- `[Source: "exact title"]`, `(Source: "exact title")`, `(Paperless document N)` and model-generated Document links.
- Title matching must be exact after harmless Unicode/whitespace normalization and identify a unique supplied document. IDs must belong to supplied certifying evidence. Generated summaries do not authorize attribution.
- Only the complete, syntactically restricted attribution is removable. Wrong IDs/titles, ambiguous titles, invented amounts, extra prose, malformed/nested markup and facts disguised as citation text cannot be silently accepted or erased.
- Keep citation declarations associated with the audit units they qualify. A declared document must occur in those units' independently validated source references; membership elsewhere in the retrieval pack alone cannot prove attribution.
- Preserve all factual content; final citations come only from accepted references. Candidate offsets/digests must refer to the canonical candidate actually audited, and repaired candidates repeat the same process.
- Bump the finalization/cache policy version so cached answers from the old acceptance behavior cannot be reused.

Implementation focus: `answer_finalization.py`; public delivery/cache regressions in `tests/test_query_delivery.py`; existing numeric, temporal, source-origin and digest tests remain mandatory.

## Q3: bounded concurrent auditing with work-aware deadlines

The global LiteLLM-client cleanup lock currently forces every helper through a serial critical section. Removing the lock while retaining global cleanup would let one call close another's transport.

- Keep Strands as the agent loop and keep the same LiteLLM proxy URL, credentials and model route. Use the pinned Strands OpenAI-compatible transport with request-owned clients, removing process-global LiteLLM cleanup/transport mutation from this wrapper.
- Limit active helper requests and each answer's audit workers (initial concurrency 4), with no output-token ceiling. Every client/stream closes on success, provider failure, timeout or cancellation.
- Audit all units in stable batches of four. Accumulate results deterministically by original offsets, regardless of completion order. Missing, duplicate or out-of-batch assessments remain failures.
- Treat the configured audit timeout as a wave allowance, multiplying by the number of waves required by the candidate and concurrency. Retain finite per-call deadlines. Repair has its own finite allowance; a repaired candidate is fully re-audited with its own work-derived budget. There are at most two audit attempts.
- Preserve the unit-count safety bound, strict source validation, numeric/temporal rules and full answer-level validation. A deadline never certifies a partial ledger.
- Retain stream keepalives and cancellation propagation. Test overlapping queries and helpers through the real SDK/HTTP boundary, rather than only through mocked auditor methods.
- Ask the auditor for the shortest complete supporting quotation and no explanatory prose; this reduces redundant output without imposing a token cap.

Synthetic timing checks establish removal of serial scaling, not real-model performance. Record original live request timing separately. If live evidence exposes another bottleneck, add its reproduction and amend this section before changing behavior.

## I1 and I2: preserve source identities through review

- Use the extraction identity normalization consistently at admission, type reconciliation and name-usage receipt lookup. Case/whitespace spelling changes must not be treated as new candidates.
- Require an unambiguous proposed identity including source-grounded identity hint. Do not widen this boundary to fuzzy name matches or freely transferable graph aliases.
- Same-type review preserves the stable accepted ID used by co-reference proposals. A genuine type correction still requires its separate exact contextual quote and rationale; unsupported changes retain the proposal's type and description.
- Positive/negative identity receipts must still bind the accepted IDs, exact source/window, current policy, hints and types. Preserve durable vetoes and human/unknown historical decisions.
- Exercise actual public extraction, document binding, graph/vector persistence, multiple windows, source replacement and repeated processing. A successful test must assert entities/aliases are present, not only that coverage says complete.
- Correct the private DocumentRef fixture to use extraction Document, preserving the later graph mapping. Establish a real positive source only when an exact source passage supports co-reference; do not manufacture an alias to satisfy a test.
- Add a distinct extraction-reconciliation version to the ingestion fingerprint. Do not invalidate trusted alias/revocation policy provenance merely to trigger reprocessing.

## V1 and V2: consistency and reconciliation

The current vector writer retains the accepted source spelling, while the graph keeps canonical display spelling. The contract permits that difference only when UUID/type/dimension match and the spelling agrees under the existing typed orthographic policy or a provenance-verified alias for that identity. Literal variants remain visible as a separate diagnostic. Vector text may never rename graph identities.

- Implement a reusable pure consistency classifier with synthetic positive/negative controls. Preserve exact graph label compatibility for the known extraction Document to graph DocumentRef translation and documented legacy label representation.
- Add public ingestion tests with actual disposable stores for reused identities, alternate source spelling, repeated documents/source replacement and a genuinely different identity. Review any verifier-contract change independently.
- Re-audit live exact identities with read-only database sessions. Earlier totals (148 orphan vectors, 268 remaining label discrepancies) are a baseline, not a deletion manifest.
- Build a private bounded reconciliation manifest only for individually established stale/orphan/incorrect rows. Capture all affected rows and relevant source/generation/history preconditions before writes; preserve shared support and durable decisions.
- Verify ingestion and post-sync Steward are drained, compare optimistic preconditions immediately before writes, and abort on drift. Apply once, verify a no-write repeat and rollback usability. Never infer graph type or identity from an old vector row.
- Re-audit after normal migration, since processing can replace stale derived records. Do not unnecessarily repair records that normal source-backed processing will replace.

## Release and live acceptance sequence

1. Commit this plan before implementation and update each matrix state with actual evidence as work progresses. Preserve failing diagnostic artifacts; put reusable synthetic regressions into tests.
2. Complete Q2/Q3/I1/V1, including the fingerprint/cache changes. Run focused red-to-green checks, then the full offline and real disposable-datastore suite. Run frontend contracts and delivery checks relevant to changed behavior.
3. Apply the repo code-review skill to the exact implementation diff against 30773c6, using this spec. Record Standards and Spec reports separately, fix findings and revalidate the changed head.
4. Publish the reviewed branch/PR with sanitized validation. Require exact-head CI. Merge only the validated source and verify main image publication/digests.
5. Verify live ownership and actual writer drain. Deploy through narrowly scoped GitOps immutable image pins with controls 0 / 0 / 2, including PR15's history safeguard. Do not blindly merge stale GitOps PR136 if this release supersedes it.
6. Verify readiness, model route, output-limit omission, PR15 history behavior and runtime controls. Run bounded canary processing required by the extraction-fingerprint change and the corrected real alias/query acceptance checks. Preserve private before/after receipts.
   Complete U1 with browser/computer control, not only Playwright assertions or source inspection. Use disposable synthetic data for destructive/review mutations, and verify deployed read-only/live-query flows against production. Record viewport, route, action, observed result and screenshot evidence without putting private document content into public artifacts. Validate browser console/network failures where available. Automated browser contracts complement this manual visual exercise.
7. Complete justified reconciliation at the appropriate drained point. Admit one ordinary sync only after representative acceptance passes; record its task ID immediately and poll that task rather than submitting again.
8. Obtain exact corpus/source/fingerprint/coverage/vector proof with a stable eligible snapshot; verify actual post-sync Steward drain. Resolve any newly reproduced defect through the same loop before admitting more work.
9. Restore schedules through GitOps and verify the resulting live state. Update issue16 with sanitized closure evidence only after all relevant matrix rows pass.

## Invariants and exclusions

- Paperless content is read-only: no uploads, re-OCR, source edits, wipe or Reindex All.
- No blanket identity/alias/history deletion, promotion of unknown historical decisions, numeric exemptions, generated-summary certification, disabled positive alias paths or model changes to hide a failure.
- Preserve three bounded extraction attempts, disabled SDK retries, finite native extraction timeouts, adaptive splitting only after actual truncation, complete source/tail coverage and 3,072-dimensional embeddings.
- Do not claim completion from a green build, matching counts, a completed ingestion task or a stopped worker. Distinguish source committed, source merged, release deployed, data repaired, real canaries accepted, corpus accepted and schedules restored.
- Production changes use one operator and no overlapping ingestion/deployment/reconciliation. Private data stays in the protected operator workspace and local private evidence directory.
- All periodic/post-sync/post-merge Steward calls must use the same task admission registry as ingestion and manual mutations. Publish actual in-process Steward running state so a completed sync cannot be mistaken for writer drain. Regress admission in both directions and retain the post-sync child task ID.

## Execution log

- 2026-09-07: plan created; isolated branch begins at 9f8e165 on main 30773c6. Q1 has passing offline serialization/acceptance tests and independent review from the previous turn. Remaining rows are not yet accepted.
- 2026-09-07: user added explicit browser/computer-control visual end-to-end acceptance; U1 and the live acceptance sequence updated. Citation regressions ran red: 6 test methods, 9 assertion failures against the starting implementation.
- 2026-09-07: Q2/I1/Q3/V1 implemented locally. Red controls reproduced citation rejection/unsafe attribution, casing loss, sequential 56-unit timeout and global helper serialization. Focused checks pass. The first full actual-datastore suite passed 319 tests with zero skips, including public alias ingestion/replacement and vector consistency. Production frontend build, 12 unit regressions and 24 automated browser scenarios passed; manual visual U1 remains pending.
- 2026-09-07: added and reproduced two writer-admission failures: post-sync Steward was not registered, and manual/periodic Steward could overlap ingestion. All automatic/manual Steward paths now share task admission and expose in-process running state; the post-sync parent retains its child task ID. Focused regressions pass; full suites are being rerun on this change.
