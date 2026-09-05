# Review feedback and factual evaluation

## Exploration and decision

Automatically reindexing and closing every extraction complaint would claim a correction without evidence that the model changed its output. Hiding the entire document would discard usable original evidence. Recording complaints without affecting answers leaves disputed extraction authoritative. Use durable open/resolved reports instead: original OCR stays inspectable, open reports annotate evidence as disputed, and the final answer contract decides whether that evidence can support a claim.

Reports carry an ID, document ID, reason, note, creation time and status. Resolution records its kind, human review note, timestamp and processing hash. The document detail page shows the review history and open count. Users may reindex through the existing admitted task workflow, inspect the completed result, then explicitly resolve it as `reindexed_and_reviewed`; this resolution requires a successful processing record newer than the report. `dismissed_after_review` records a human determination that no extraction correction was needed. Neither resolution is automatic, and neither proves extraction accuracy independently. Flagging and resolving invalidate answer caches.

Store interface: `add_document_feedback`, `get_document_feedback`, `get_open_feedback_document_ids(document_ids)`, and `resolve_document_feedback`. The open-ID lookup is bounded to the retrieved documents. A failed lookup must not silently imply no disputes. Existing rows migrate to open. Processing/detail responses distinguish all reports from open reports. Resolution cannot target another document or silently overwrite an already resolved report.

For evaluations, retaining confidence/keyword thresholds would repeat B08. Replacing them with another model's confidence would also fail to establish independent correctness. Use a versioned synthetic corpus with known document IDs, content and source spans, expected answer facts, terminal dispositions and abstentions. Existing private-corpus questions become smoke cases explicitly marked as lacking factual ground truth; they still reject zero-source and unfinalized responses, but do not count as a measured accuracy suite.

The factual scorer checks independent expected values/units and prohibited facts, required document IDs, cited reference membership and exact quote offsets against the fixture corpus, valid calendar dates, and expected terminal disposition. It does not trust response confidence, trace presence, an evidence pack copied from the response, or a model's support label as ground truth. Supported cases require source references and a complete terminal result. Expected abstention cases require the exact configured safe response or allowed deterministic abstention text, no asserted forbidden fact and an allowed non-supported disposition. Quick/unaudited responses cannot pass a factual-support case.

## Acceptance

Full imported HTTP endpoint tests verify record -> open -> reindex failure/stale processing refusal -> explicit reviewed resolution, cache invalidation, foreign/already-resolved report rejection and note validation. Storage integration checks run the actual SQL migration and lifecycle against disposable PostgreSQL when available. Frontend types/lint/build validate review UI integration; browser checks cover its loading/error and explicit review actions when a test backend is available.

Scorer regressions include all six former keyword-only, zero-source forgeries; supported low-confidence fixture answers; plausible wrong high-confidence values/units; fabricated or wrong-document quotes; invalid dates; expected abstention and forged abstention containing an unsupported assertion. Report incorrect factual answers and abstention rates separately from smoke results and execution errors. No production/model accuracy claim follows from deterministic fixture validation.

## Validation results

- Four feedback ASGI tests failed before lifecycle routes were implemented, then passed. They exercise the full imported application with a controlled storage adapter.
- Three PostgreSQL tests pass against the actual migration and store methods in isolated `feedback_test`: legacy reports migrate to open idempotently; scope/count/resolution behavior persists; a correction resolution requires the same successful newer processing hash at the update itself.
- Nine scorer tests pass. The initial run had 13 failed assertions plus one manifest-loading error, including all six accepted zero-source forgeries. Correct low-confidence answers and expected abstentions now pass; fabricated provenance, wrong values/units, invalid or invented timeline events and appended unsupported claims fail.
- Four additional ASGI delivery tests pass for ordinary/SSE/saved metadata parity, failure without draft persistence, missing terminal answer, and disconnect followed by background terminal persistence.
- Document feedback UI passes frontend lint and TypeScript checks. Full build and interactive review validation are tracked in the overall implementation report.
