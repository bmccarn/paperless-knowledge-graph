# Insurance history and current-record query investigation

Status: investigation complete; reproduced problems remain unfixed. September 8, 2026. This follow-up supersedes any interpretation of the prior narrow acceptance as proof that broader insurance-history questions work reliably.

## Checked state and scope

Application source `cbdc610` is deployed; investigative checkout `9d84bd3` has the same application tree plus the completed prior documentation. The saved request ran in timeline mode from 17:07:55 to 17:10:55 UTC and displayed 179.3 seconds. Its finalization used source-audit-v10, completed two audit attempts, and returned unsupported with 10 supported and 2 unsupported claims out of 12; audit coverage was complete.

Read the existing saved conversation and bounded API-log window. There were no recorded errors, warnings, timeout messages or token-limit exceptions in that window. Reconstructed all 90 evidence items against current indexed/full OCR and checked all 25 saved references; every item identity and quote matched, and source snapshots were stable. No new model query, ingestion, repair, source write or deployment was performed for this investigation. Private source contents, policy identifiers, conversation identity and model artifacts remain in the protected local evidence directory.

## Findings

### H1 — chosen date quotations fail the deterministic value check

`app/answer_finalization.py:363` checks numeric values in the claim against its individual supporting quotations. The homeowners claim expands two-digit source years into four-digit years. Its chosen table quotation contains only the two-digit years, so `values_match` rejects the four-digit values. The same supplied OCR span also contains the exact full-year policy period; the auditor did not cite it. Replacing only the claim's years with their literal two-digit forms passes the existing numeric check. This isolates the mismatch without proving that shorter year formatting is the correct product fix.

The application claim's references contain a compact unlabelled digit string and the previous-carrier field, but no explicit separated application date. The numeric guard rejects the asserted year and day. An explicit date exists in retrieved sections, including a dated homeowners application. Adding an explicit date quotation satisfies the numeric check, but any production repair must also verify that the date belongs to the asserted auto-application event. A date elsewhere in the package is not, by itself, evidence of submission. Both claims failed with `value_mismatch`, not invalid references or unavailable documents.

The saved candidate was recovered byte-for-byte using its stored digest. Replaying it through the actual `AnswerFinalizer.finalize` boundary, with saved references and semantic approvals held fixed, reproduces precisely the same 10 supported / 2 unsupported result in under a second. This is a deterministic reproduction of the mechanical failure, not a new model assessment.

### H2 — one failed repair suppresses the entire answer

`app/answer_finalization.py:547` permits one repair and a second audit. If any material claim still fails, `app/answer_finalization.py:586` replaces the full answer with the generic abstention. `app/answer_finalization.py:617` assigns that final disposition a zero evidence score. Thus 0% is the answer-disposition score; it does not mean none of the retrieved evidence or none of the claims were supported.

This is implemented policy, but it fails the user's need for a useful grounded answer when a substantial verified portion exists. A future correction must preserve independently verified facts and clearly identify omitted or unresolved portions without certifying incomplete, contradictory or unaudited material. Simply raising the trust score or disabling the numeric guard would not correct the defect.

### H3 — timeline date formatting rejects otherwise valid events

`app/strands_orchestrator.py:131` requests ISO-normalized event dates. `app/timeline.py:29` requires that exact ISO text to occur literally in the quotation. The controlled event `2026-09-01` supported by `Policy changes effective September 1, 2026.` is rejected before semantic auditing. Changing only the source date to ISO accepts the same event and invokes the auditor.

The actual saved request reports zero accepted events and two rejected events. Raw rejected event payloads and reasons are not persisted, so this independently reproduced date-format defect cannot be asserted as the proven cause of those particular two rejections. Future diagnostics should retain bounded rejection reason codes without logging raw private source content.

### H4 — current-state handling blocks document-relative latest wording

`app/answer_finalization.py:625` returns `needs_review` for every required current-state question; there is no implemented resolved result. `temporal_acceptance` at line 416 also treats any occurrence of `latest` or `current` as a real-world current-state assertion. A controlled fully supported answer using `The retrieved statement...` is qualified and returned; changing only that phrase to `The latest retrieved statement...` produces `current_unresolved` and suppresses the answer despite 100% claim support.

This does not explain the observed request's two numeric failures, but it is a separate obstacle to answering its most-current portion. The product must distinguish the newest documented observation from a guarantee of currently active coverage. It should answer the former from cited records and qualify uncertainty about the latter. A keyword match cannot make that distinction.

### H5 — the final history evidence pack omits indexed historical policies

The 90-item pack contains 17 documents and many current-year endorsements. An existing, processed homeowners policy covering 2024–2025 is absent, as is another indexed homeowners declaration. Both document detail reads succeeded, with indexed OCR chunks and no open feedback. The historical source has 14 chunks and the other declaration has 3.

This establishes a retrieval-coverage gap for the history question. It does not yet identify the precise ranking, candidate-selection or budget stage that discarded these records. More corpus reprocessing is not a remedy for records that are already indexed and available. Historical retrieval needs explicit coverage across policy families and recorded periods, tested separately from ordinary current-policy lookup.

## Reproduction evidence

Private agent-runnable scripts under `/private/tmp/kg-insurance-history-20260908`:

- `replay-saved-values.py`: actual numeric boundary returns false for exactly the two saved failed claims.
- `replay-finalizer.py`: exact saved candidate digest and source pack reproduce the complete original 10/2 finalizer failure.
- `diagnose-saved-values.py`: one-variable controls isolate missing years and application-date numbers.
- `replay-timeline-date.py`: written-month versus ISO source date, zero versus one accepted event.
- `replay-latest-word.py`: document-relative latest wording alone changes qualified to current_unresolved.
- `source-selection-diagnostic.py`: exact full-year policy dates were supplied to the relevant audit batch; dated application sections and missing indexed historical records are separately identified.

Run with `PYTHONPATH=. /private/tmp/paperless-accuracy-venv/bin/python <script>` from the investigative checkout. The three acceptance-style reproductions intentionally exit nonzero on the current implementation. They are retained diagnostics, not passing regressions or deployed fixes.

## Implementation and acceptance needed

1. Make date comparisons format-aware while preserving exact source quotations, date precision, explicit century evidence and unambiguous event context. Do not reinterpret arbitrary digit strings, infer a century without source support, invent a day, or borrow another event's date.
2. Give repair actionable missing-value diagnostics and require references covering the entire claim. Test an explicit full-year date available beside a shorter-year table and a separate application date that must not be borrowed across events.
3. Design verified partial-answer delivery with explicit unresolved gaps and complete provenance/digests. Test mixed supported/unsupported facts, contradictory records, absent audits and repaired-answer persistence in ordinary and streamed responses.
4. Separate newest source-backed observations from active real-world status. Retain caution about cancellation and supersession; remove blanket keyword rejection of document-relative wording.
5. Retrieve history across distinct periods/policy families. Reproduce omission of an indexed prior policy and verify it survives candidate selection, evidence budgeting and synthesis input.
6. Capture structured timeline rejection reasons and regress ordinary written/slash/ISO dates, invalid dates, ambiguous locale/century and exact-source references.
7. Review and validate any resulting implementation before GitOps delivery. Acceptance must use this exact history-and-current question and its mode, retain the earlier narrow-query regression, inspect actual UI answer/timeline/citations/history, and verify fresh source support. A single shorter answer or the previous four-claim insurance query cannot substitute for this case.


## Implementation review, before release

The first implementation (`6c6066a`) passed 386 backend tests against disposable PostgreSQL, Neo4j and Redis with zero skips, frontend lint/type/build and 12 unit tests, and all production-build browser contracts. Actual desktop, mobile and source-panel screenshots showed the verified-partial notice and restored ledger. These checks were superseded by independent review findings, not treated as proof of completion.

Standards review reproduced malformed ISO dates escaping validation, zero-padded identifiers being mistaken for years, invalid repair output admitting an earlier partial subset, and year-only historical sources losing reservations. Spec review additionally reproduced four-digit quantity-format regressions, serialized JSON overhead dropping reserved sources, the remaining historical-word veto, and the actual Strands adapter swallowing failed repair as an empty result. Root review found malformed comparison metadata needed an explicit rejection and public manifests needed to retain their reservation flag.

The follow-up implementation keeps bare four-digit values under ordinary Decimal checks; validates complete date-shaped tokens; rejects invalid repair envelopes, including failures through the actual adapter; validates consistent semantic temporal categories; and budgets whole windows with serialized costs. New regressions retain all eight historical and three explicitly requested sources together within 28,000 characters, while reporting limits when the priority set cannot fit.

A read-only production metadata probe found incidental subject mentions crowding historical retrieval. Metadata subject relevance and temporal/type strata now precede duplicate/title population; indexed dates provide a fallback for undated titles. The retained older incident source reached the eight-document reservation in a local deterministic replay of the read-only metadata. This is a retrieval-stage observation, not a live model acceptance result. End-to-end regressions cover renamed higher-ID old sources and newer records across invoices, medical measurements and unknown subjects. No source, ingestion fingerprint, model route or output-token allowance changed.

Release and exact uncached live acceptance remain pending.


## Implementation and independent review

The general implementation was reviewed at `2f0daee657bbbe4274932b4fac6a6857c37a315e` against `9d84bd3c0db95fbfa6412ecde609ff25e4de795e`, then merged in [PR #34](https://github.com/bmccarn/paperless-knowledge-graph/pull/34) as `0d07743d2447dda8723d5f836818fd057c09da86`.

### Standards

No remaining actionable correctness defects, documented-standard violations or Fowler smells. The independent reviewer ran 52 focused tests with network connections blocked. Earlier findings about missing-metadata history, date-shaped scalar values, failed repair envelopes, formatted source labels and trimmed date tokens were closed with public behavior regressions.

### Spec

No remaining implementation findings. The independent reviewer ran 49 focused tests and separately inspected the exact-cache-entry acceptance operator. Earlier findings about source-window serialization, typed temporal assertions, swallowed Strands repair failures, historical diversity and date/identifier boundaries were closed. Deployment and live acceptance were explicitly outside this code review.

Standards: zero remaining findings. Spec: zero remaining implementation findings.

The final local backend suite passed 397 tests against disposable PostgreSQL/pgvector, Neo4j/APOC and Redis, with no skips. Frontend lint, type checking, production build, 12 unit tests and all production browser contracts passed. Desktop, mobile and source-detail screenshots for the partial-answer fixture were visually inspected. Both PR-head and merged-head backend/frontend CI passed. These checks establish controlled behavior, not live-corpus answer accuracy.


## First live acceptance did not pass

The first uncached Timeline run on `0d07743` retained the previously missed older source and produced four source-validated events, but the final repaired answer was withheld as incomplete: 10 units, 6 audited, 3 supported, 1 unsupported, 2 missing and 4 unchecked. No provider timeout/error was logged. All 90 pack items and 12 retained references were reconstructed against stable current OCR.

Offline selection replay reproduced loss of identifying context from a reserved historical document and omission of a latest-record source from a comparison audit batch despite its presence in the canonical pack. The all-unchecked batch establishes an incomplete protocol result; the exact original malformed envelope was not saved. H2/A3 extend the general plan to address these boundaries without accepting unchecked output. The source corpus and review history remained unchanged; live acceptance remains open.


### H2/A3 implementation validation

The general follow-up preserves up to three historically selected OCR chunks (including chunk zero) across context merging and canonical evidence selection. Opening windows are reserved before repeated excerpts; inverse-frequency term weighting improves selection of discriminating identifiers without changing the serialized source budget. Source support remains independently audited.

Audit batches now require exactly the requested IDs, recognized statuses and reference lists. One nonempty structural failure may receive a fresh audit using identical units and sources under the existing deadline. Valid negative verdicts are not retried. Empty responses, failed correction, exceptions and timeouts cannot publish earlier support or partial output. Aggregate protocol diagnostics contain only fixed error categories and attempt counts. Answer policy is v12; ingestion and model routing are unchanged.

Validation: 404 backend tests passed against disposable PostgreSQL, Neo4j and Redis with zero skips. Additional baseline controls run against the preceding implementation reproduce lost historical opening context, repetitive-notice selection and malformed-batch withholding; the corresponding current-code regressions pass. The former incomplete-audit fixture now keeps both the initial malformed response and its permitted correction incomplete, preserving its original withholding assertion. Cross-domain public query tests inspect the actual canonical synthesis payload and audit inputs for separated opening identifiers and later observations in invoices, contracts and laboratory records. Final live acceptance remains pending.


Independent review of H2/A3 found three additional gaps: full question wording could dominate a unit reservation; the real Strands adapter collapsed malformed JSON to an unavailable result; failed correction lost its attempt diagnostics. The follow-up ranks unit reservations on assertion terms, preserves a source-auditor-only parse-failure marker, and carries sanitized batch state through exceptions/cancellation. Public regressions exercise long history wording, the real Strands adapter with a synthetic transport, and failed/timeout correction. No untrusted model text enters correction diagnostics.


A second Spec review reproduced a combined-assertion gap: separate identity/date test units each received a slot, but a single assertion requiring both passages lost its later same-document source amid repeated notices. The regression now requires both exact references for invoice and laboratory claims under eight historical reservations and forty competing notices. Selection scores combined assertion coverage by document (excluding the existing query stopwords), then reserves up to two complementary whole windows, accounting for the already reserved opening. Existing serialized budgets remain unchanged; source matching remains selection only, never support proof.


Standards review then extended the same reproducer to one comparison spanning two identified documents; selecting a single winning document lost both needed continuations. Complementary selection now follows the most discriminating uncovered assertion terms across documents and retains the relevant same-document windows. It stops when no additional assertion terms are covered, while the existing whole-window serialized budget remains authoritative. A combined two-document regression requires four exact references under the same competing-source load; it failed before this correction and passes afterward.


### v12 live acceptance: failed, retained

Application merge `f132b1e` was deployed through reviewed GitOps PR151 (`b03e532`). Flux, image chains, ready pods, runtime configuration and original453review rows were verified. A separate `/sync` request arrived during verification and completed; document hashes903, document embeddings6884 and review rows529 stayed unchanged while cache generation advanced1980→1983. A fresh post-sync baseline was taken before the query.

The unchanged history question was submitted once in Timeline through the production browser, conversation `f0f01eef-7cd9-4e28-86eb-e7af13f0c950`, uncached, policyv12. It returned `audit_failed`:27audited units,10supported,5unsupported,12missing,0unchecked; all7batch protocols valid on their first attempt. Five timeline events validated. The failure reason was “The answer repair returned no valid candidate.” Raw editor output was not persisted, so its exact format failure is unknown. Current-source reconstruction verified all90pack items and33retained references. Offline selection showed irrelevant historical reservations consuming the entire first batch and excluding latest identifiers; a stage-specific, fair-unit prototype restores them without increasing the budget. A separate exact-source formatted-currency mismatch was reproduced. H3/A4/D3 in the spec record the general follow-up.

The new frontend passed actual browser graph tests:265.666×zoom, constant11pxlabels, hover/inspector, fit-view, 2D/3D navigation and zero page errors. Close-up and restored2D screenshots were visually inspected. These graph checks do not imply query acceptance.


### H3/A4/D3 implementation and review closure

Audit windows are now queued independently per claim and admitted round-robin under the same serialized budget. Synthesis retains archive breadth; audit diagnostics identify only their actual stage reservations. Comparison claims receive topical alternative records ordered by source-written dates, with explicit date-order handling. Titles and scalar magnitudes cannot displace newer OCR-matching counterevidence merely by looking richer or larger.

The editor now returns prose directly through the same request-owned Strands transport. Only normally completed nonempty text reaches a fresh complete audit. Empty/exception/timeout/truncation and caller cancellation retain their failure boundaries, and a refusal or unexpected JSON is never automatically trusted. No output cap or generative retry was added.

Balanced markup is normalized only on comparison copies. Signed quantities retain their signs, including spaces around displayed negatives. Reference validation uses bounded adjacency guards derived from the whole certified chunk so formatting cannot hide a chopped identifier, sign, magnitude or decimal tail. Original source quotes, offsets and digests remain unchanged.

Independent review reproduced and closed four further false-acceptance paths: less detailed titles hiding newer counterevidence, bare monetary amounts treated as recency years, spaced negative source quantities accepted as positive, and a decimal continuation hidden beyond a formatted source-window edge. Public regressions cover each failure. The final local actual-datastore suite at `9482029` passed 420 tests with zero skips. Native-adapter tests include complete prose, re-audit, cancellation and all failure controls. Final independent review and exact-head CI remain pending; production is still v12 and the retained failed live runs do not count as acceptance.


### Final v13 review and deployment checkpoint

The final review also closed source-context edge cases involving list markers, quoted fragments, Unicode whitespace and literal code/HTML. Structural marker authority is derived from the whole certified source using pinned CommonMark tokenization and retained as raw offsets; model-provided marker metadata cannot authorize a sign change. All affected public controls passed. Both final review axes reported zero remaining findings at `4d6c0c5`; the actual-datastore suite passed428tests with zero skips, and exact-head backend/frontend/browser CI passed.

PR36 merged as `3f5b192` and was deployed through independently reviewed, CI-passing GitOps PR152 (`74433eb`). Only two immutable image pins changed. Fresh writer drain passed immediately before merge. Flux, readiness, published-to-running image chains, original453review records, configured models/deadlines and absence of output caps passed after rollout. Separately observed ingestion activity had increased the pre-rollout baseline to924document hashes,6966embeddings and529review rows, generation2052. That complete hash digest and all counts/generation were unchanged across rollout. No resync or source mutation was performed by this release. Live query acceptance remains a separate gate.


### v13 live acceptance: failed, retained

Conversation `4e0dea2f-a60a-4e63-a5e1-f49893cc3364` submitted the unchanged Timeline question once at21:07:58Z and completed source audit at21:12:19Z, about262seconds. It was uncached, policyv13, disposition unsupported, two candidate attempts. All42units were audited:33supported,7unsupported,2missing,0conflicting/unchecked. Eight timeline events validated. All90canonical items and69retained claim references were reconstructed against stable current OCR. No provider errors/warnings were logged.

The newest auto declaration was absent from the canonical pack but exists in the bounded indexed candidate set. Historical selection also admits a four-digit street number as a period, disrupting newest-period ordering. Neighbor expansion re-ranks a diversified set but does not enforce document diversity again before the90-item cut. The exact contribution of vector retrieval versus final packing to the missing record is not recoverable from the saved payload; both boundaries need public reproductions. A certified field leader before a positive premium reproduced a false negative-amount mismatch. The complete missing verdicts excluded subset re-audit. H4/D4/A5 record the generalized follow-up.

The browser rendered the refusal, ledger and source-detail link with no page errors. The acceptance harness then timed out restoring history by a nonunique title; this is a harness ambiguity, not established application failure. It did not resubmit the query. Neither the semantic run nor the unfinished UI restoration counts as acceptance.


The read-only browser follow-up restored the exact v13 conversation using its current list identity, rendered its42-claim ledger and captured desktop/mobile views with zero page errors. No query was submitted. This closes the harness ambiguity, not the semantic failure. The H4/D4/A5 implementation's initial full datastore run passed438tests, then independent review found the additional date-ranking/unit/title and field-label defects recorded in the spec. Those findings supersede that preliminary green checkpoint.
