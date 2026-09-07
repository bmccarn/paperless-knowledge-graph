# Issue 16 and strict-query acceptance closure

Status: implementation merged and deployed; live acceptance and corpus migration in progress. Owner: Codex in the current user session, sole live-data/deployment operator. Requested on 2026-09-07: write a detailed working spec, then implement all seven outstanding areas described in the investigation.

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
| Q1 | No application output cap in any Strands helper | Actual SDK request serialization omits every output-limit field; provider truncation still fails certification | Passed serialized SDK regressions and deployed runtime checks |
| Q2 | Valid source attributions do not become false numeric claims | Real finalizer accepts supported quote plus verified title/ID attribution; wrong/ambiguous metadata, factual numbers and adversarial citation text fail; initial and repaired answers behave identically | Local implementation and regressions pass; live acceptance pending |
| Q3 | Audit execution scales with the work and does not serialize unrelated queries | 56-unit public-query regression completes with controlled latency; bounded concurrency, finite per-call/wave deadlines, cancellation and isolated transport cleanup; ordinary/SSE payloads agree | Local implementation and regressions pass; live acceptance pending |
| Q4 | Explicitly requested sources survive bounded model-input selection | A retrieved, exact-title/ID source must reach both synthesis and audit despite lexical distractors; whole-window budgets and reference validation remain unchanged | Reproduced live: target ranked first in pack, absent from selected audit windows; regression in progress |
| I1 | Review casing preserves the proposed source identity and its receipts | Public extraction through binding/persistence positive regression; wrong name/hint/type, ambiguous identities, denied/conflicting reviews and repeated windows remain conservative | Local implementation and regressions pass; live acceptance pending |
| I2 | Real source aliases have a valid positive acceptance control | Correct extraction type, exact source/hash/span, accepted endpoints, persisted trusted alias, negative neighbor and repeat-processing evidence | Pending |
| I3 | Historical insurance provider identity agrees with the current source after migration | A live UI-discovered provider relation names a bank absent from the current OCR; capture its private before-state, reprocess through the public pipeline, require the exact source insurer and preserve other-document support/history | Provider identity corrected by targeted reprocessing; quote propagation fix reviewed, final release acceptance pending |
| V1 | Vector consistency has an explicit, tested contract | Exact UUID/type/dimension checks plus separately reported literal and policy-name/authorized-alias comparisons; different identities fail; verifier revision/hash retained | Local implementation and regressions pass; live acceptance pending |
| V2 | Historical vector drift is classified and any repair is justified | Private exact-identity manifest, protected preimages, independent dry-run review, writer/state guards, no-write second run, post-repair acceptance | Pending |
| P1 | Candidate veto reads scale with possible identity matches | Public resolver regression with 100 unrelated same-type candidates performs one full-neighborhood read for the actual match; existing source-alias, ambiguity and durable-veto controls still pass | Passed 101-to-1 read regression, 54 differential cases, independent reviews and CI; merged in PR19 |
| R1 | Intended code including PR15 is running | Exact-head offline and disposable-datastore/frontend checks, independent standards/spec review, immutable image digests, GitOps rollout and actual runtime predicate checks | Passed: application PR17, GitOps PR139, runtime checks and original-history checksum |
| R2 | Real supported queries succeed on intended release | Original insurance question and retained exact-source canary query rerun once per diagnostic change; source-grounded useful answer or documented genuine evidence limitation, matching final/ledger digests, no timeout/citation false rejection | Pending |
| U1 | Visually verify all UI surfaces and their meaningful interactions with computer/browser control | Inventory routes/features; inspect rendered navigation, chat modes, conversation history/isolation, document list/detail/refresh, graph expansion/filter/selection/source inspection, review/feedback, domains/settings/status, loading/empty/error states and responsive layout; retain a per-flow visual test record and fix reproduced UI defects | All local flows passed; deployed acceptance in progress |
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
- 2026-09-07: independent Standards and Spec reviews at 498f6f8 reproduced malformed-link attribution bypass, nested-citation acceptance, graph Document/DocumentRef conflation and detached periodic Steward shutdown. Added red regressions (8 failures), fixed admission/normalization/lifecycle, and added real Strands/AsyncOpenAI concurrent public-query coverage. The full real-datastore suite now passes 326 tests without skips.
- 2026-09-07: manual browser-control visual testing reproduced stale result totals plus a false empty state on failed catalog search. Corrected the error state and added a failing-then-passing production browser regression. Named navigation/chat/document controls, added sheet descriptions, and corrected incomplete synthetic status/task payloads. All 25 automated browser scenarios, production build, lint and typecheck pass on the catalog fix; final minor labeling/documentation changes still require final-head validation. See the visual acceptance record for completed flows and explicit remaining checks.
- 2026-09-07: review of 1179fc5 caught a complete-Markdown-link boundary case (source title followed by an unverified target whose number also occurs in a valid fact), stale catalog facets missed by the first guard, and unowned broad-query retrieval children. Each received a red reproduction and correction: consume full links first, hide facets on errors, and own initial/broad retrieval through TaskGroup. Revalidate and review this final patch before release.
- 2026-09-07: user reaffirmed GitOps-only deployment and repository location. Found clean /Users/blake/development/homelab/homelab-k8s; fetched current origin/main a6db7b5 and created linked worktree /private/tmp/kg-gitops on deploy/kg-acceptance-20260907. No live desired-state change yet.
- 2026-09-07: PR17 published at 7d684c2; exact-head backend/frontend CI passed. Standards and Spec follow-ups both found no actionable defects. Final local validation: 328 actual-datastore tests without skips, 328 offline tests (44 explicit database skips), 12 frontend regressions and 25 browser scenarios. A final 320px visual check found the theme button clipped; corrected flexible mobile tabs and added a passing 26th browser scenario before merge.

- 2026-09-07 14:37 UTC: PR17 merged as bbbf82d115c45cfce171e549cb74915f32cd35c8 after final 7f7747e review and exact-head CI; main validation/image publication passed. GitOps PR139 merged as 00d4fbf14b1d93900c7d63217da41ed0c355b1f6; Flux applied both immutable image digests, both pods ready. Runtime predicates passed, all 453 original history records exact, actual Steward and database writers drained, controls remain 0/0/2. Superseded GitOps PR136 closed. Small canary admitted once with private task receipt.

- 2026-09-07 14:43 UTC: small canary completed in 80.6s, with current source/fingerprint, full window/tail/OCR coverage, exact metadata and relationship quotes, accepted existing typed vectors, all 453 history rows and 39 unrelated shared-support records preserved. Large canary admitted once after writer drain; receipts remain private. Corrected real alias control passed one provider review, an affirmed current-direct receipt and native source-alias authorization without datastore writes; actual full-pipeline persistence still pending. A strict-query probe overlapped the large reindex and returned corpus_changed, correctly withholding an answer; rerun only after writer drain.

- 2026-09-07: deployed document/graph visual inspection identified an old insurance provider relation inconsistent with current OCR. Added I3 to the live acceptance scope. This is observed historical derived-data contamination; whether intended processing corrects it remains to be verified. No ad hoc identity rename or global merge is authorized by this observation.

- Read-only vector acceptance tooling independently reviewed; fixed cancelling-state admission, cached skip-tag resolution, cleanup failure handling and false source attribution from mixed human/source alias records. Final Spec review approved private verifier SHA256 27efee7d133784404d923d8d63dba835cd801382b5be98cf632702daaf8d4b24. It grants neither repair authority nor corpus-completeness proof.

- PR18 merged as eb5f34cd0c71ca2012b7cc6b7f8b645deb1238e2; exact-head and main CI/image publication passed. Its diff from deployed bbbf82d changes only two frontend pages; backend app tree and extraction fingerprint remain identical. GitOps PR140 is a validated draft at 58583f761eb9e2a1e815ee4f1e21390248f2ce02; do not merge until active canary completion and drain.

- Large-canary storage remained active after all 50 windows and 193 document embedding requests completed. Investigation found full-neighborhood reads for every same-type entity before identity matching. A public resolver regression reproduced 101 reads for one valid match plus 100 unrelated candidates. Added P1; move evidence matching ahead of neighborhood reads while preserving matching-candidate veto semantics and all durable decisions. This demonstrates eliminated database calls, not a measured production speedup.

### Stable-canary follow-up: referenced source windows

The completed small canary remained unavailable to a strict subject/quotation query even though its two OCR items ranked first in the 90-item evidence pack. The shared synthesis/audit span selector discarded title and document-ID metadata and reranked only raw content word overlap; long unrelated notices displaced the short requested invoice. Preserve explicit document references and exact quoted titles when selecting bounded source windows. Keep whole-window selection, exact reference checks, and fail-closed coverage; invalidate cached outcomes from the old selector. Regression must exercise both rendered synthesis input and auditor selection with lexical distractors, plus non-document numeric controls. This is a reproduced source-selection defect, not evidence that model context limits are fabricated.

The large canary completed in 3068 seconds with 482 extracted entities and 192 OCR chunks. Integrity and 14 identity/history safeguards passed; zero source-backed aliases were persisted. A three-pass read-only replay of the exact source window returned five candidates and zero co-reference proposals, distinguishing omitted proposals from a proven persistence failure. The separate typed native review control passed, but full-pipeline positive alias acceptance remains open.

PR19 (`e9c8b9b`) passed both independent reviews, 54 differential cases, all 329 disposable-datastore tests, and exact-head CI before merge as `60ca5f1d`. Production remains on PR17 pending the remaining representative gates.

### Alias proposal and provider evidence follow-up

A real short source explicitly names an agency and its acronym. The unchanged extraction flow returned a combined name with no identity receipt; a read-only native-model run with a narrow prompt clarification returned both literal names and one affirmed/current-direct receipt retained by DocumentBindings. Stop telling the reviewer to remove distinct near-duplicate names before identity review; propose each literal source-written name separately, keep exact duplicates removable, and preserve all existing typed, scoped, exact-source proof and durable-veto rules. This is a proposal/retention clarification, not new alias authority. The native control has not yet established persisted production alias acceptance.

Targeted insurance reprocessing corrected the provider identity and preserved 454 immutable history rows and 3267 other-document support records. The provider relationship still omitted its existing validated metadata source span. Attach only exact current-source spans for the matching provider field, retaining typed binding and per-document support merges; unrelated/stale/wrong-offset evidence must never be attached. Cover insurance and medical providers, which share the same relationship path. Bump extraction reconciliation to review-admission-v3 for these changed outputs, leaving evidence-identity-v2 and all historic veto provenance unchanged. Repeat the affected small controls on the final release; large-document coverage was proven before this narrow change and must be rechecked on the final fingerprint during the ordinary corpus migration.

Q4 is closed locally at 672fa79 after independent reviews and 334 real-datastore tests; application PR20 merged as b0a83f05. It still requires a stable live retest after GitOps rollout. No full migration or normal schedule restoration has started.

### Final follow-up review closure

At 9e8e23c, both independent reviews found no remaining actionable defects in the alias/provider follow-up. Review reproduced a null/absent metadata field borrowing raw evidence after reconciliation; validate_metadata now returns only accepted non-null scalar paths and individually matching exact quotes. Public validation/reconciliation/pipeline and actual Neo4j repeat/replacement regressions cover this boundary. All 338 tests passed with disposable datastores (zero skips); offline checks passed with 45 explicit datastore skips. Final native prompt control produced one source-backed current-direct alias receipt from the same real short source. Production persistence, final strict query/UI acceptance, ordinary migration, bounded vector repair and schedule restoration remain pending.

### Markdown alias acceptance follow-up

GitOps PR140 deployed source b1eb57e with both immutable images and controls 0/0/2; runtime checks and all 453 original review rows passed. The short alias canary completed with current v3 fingerprint, complete OCR coverage and preserved history, but no trusted alias. A read-only replay of the exact native work-classified extraction returned both accepted names and a valid current-direct receipt. The resolver's mechanical expansion parser retained the opening Markdown bold marker, so its expansion name disagreed with the independently reviewed canonical name.

Add a synthetic public-ingestion regression that fails on persisted alias absence, then repeat processing with real disposable stores and require trusted exact-source alias, stable shared UUID/support, typed vector and current fingerprint. Strip only a paired bold wrapper around the whole forward definition, preserving distinguishing words, original source offsets, negative reviews and ambiguity. Bump reconciliation to review-admission-v4 so ordinary sync repairs affected derived outputs; evidence-identity-v2 is unchanged. Repeat the native canary after GitOps release before corpus admission.
