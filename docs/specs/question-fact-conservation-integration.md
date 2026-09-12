# Integrate preserved source observations into the inactive query pipeline

Status: concrete implementation proposal, pending both reviews. Depends on both
independent passing grades for the fifteen-call fact-conservation diagnostic.
The alternative model arm is not triggered by a passing primary arm.

## Scope and observed benefit

Keep the current source reader, factual auditor, query retrieval, provider route,
source identity, deadlines and partial-answer rules. Replace initial free-form
composition with explicit selection from an immutable inventory of reader facts.
The selector preserved both previously omitted meanings in the focused test and
correctly handled unrelated material. The exclusion reviewer passed adversarial
condition, subject and date-role duplicate challenges. These measured results do
not prove complete reader recall or general answer accuracy.

This is question-evidence-v3, still constructor-disabled until full qualification.
It applies identically to Quick, Deep, Timeline and Strict. No ingestion, corpus
reprocessing, extraction-model change, output-token cap or deployment is included.

## Inventory and selection module

Add a single request-local module `app/answer_fact_selection.py`. Its interface
prepares a selection from QuestionEvidence and later binds conservation to an exact
final answer. Internally it owns strict inventory/disposition/exclusion validation.
The orchestrator exposes two native model adapters; it does not own inventory truth.

Build inventory rows from validated source_reading observations, never limitations.
Each row retains document ID, original observation position, exact text and source
span references. Derive a stable ID from those values, including position so repeated
identical notes remain separately accounted for. Validate every reference against
the immutable original snapshot and document ownership. Do not drop malformed or
empty observations to make selection succeed. Existing ObservationCandidate plain
text validation applies to selected texts; unsupported formatting is a stage failure,
not silently stripped content. This is a known compatibility risk to measure.

Selector inputs use original question, evaluation date, original documents and
inventory observations as in the successful diagnostic. For follow-ups, also pass
only the existing bounded `plan.conversation_context` produced by QueryEngine (last
ten messages within its existing character budget), as an explicitly untrusted
reference-resolution hint. User/assistant roles remain labelled. It can identify
what a pronoun refers to, never prove a fact or narrow the current question's scope.
Do not supply the resolved planner rewrite or requirement list as selection authority.
Standalone diagnostic inputs remain unchanged when context is empty. Add this exact
context string to QuestionEvidence's immutable snapshot (default empty for callers
without history), so snapshot/request bindings cover its identity. No new history
summarizer, hidden retrieval or context compressor is introduced. Exclude prior
answers as candidate authority and exclude reader limitations. Return exhaustive dispositions:
delivered, outside_request or duplicate_of. Preserve strict validation, direct
selected duplicate targets, no missing IDs, no chains/cycles and no model-created
text. Ordering follows delivered disposition rows. Render those selected exact
texts as the existing atomic ObservationCandidate; no second initial composer call.
This replaces a lossy prose stage rather than adding another prose repair stage.

Facts may depend on their original reader grouping and phrasing. Do not infer new
subjects, dates or relationships to make fragments self-contained. The existing
whole-candidate source auditor must reject ambiguous assertions. Preserve this risk
in full-query evaluation; a passed selection test is not permission to weaken it.

## Exclusion review

Review every proposed outside_request or duplicate_of against original question,
original passages, full inventory and the same bounded reference-resolution context. Each call receives one excluded ID with its
proposed status/target and the actual delivered IDs; other exclusions are not
implicitly assessed. Adapt the successful one-challenge protocol explicitly to
this shape, with exact expected ID validation. No correction retry or majority vote.
Use at most configured-concurrency worker tasks pulling an iterator of X exclusions;
never allocate one task or full copied payload per queued exclusion. Each exclusion
receives at most one call. Bound the entire exclusion phase by the existing
answer_audit_timeout_seconds deadline, in addition to per-call deadlines. On phase
timeout cancel/join owned workers, preserve completed results, and mark unfinished
or undispatched rows unavailable. External cancellation cancels/joins then propagates.
Keep results in inventory order regardless of completion order. Report X in metrics;
the ceiling is planner(0/1) + 2*reader_documents + selector(0/1) + X +
2*audit_batches + editor(0/1) + actual_coverage_calls + completion(0/1). No source
truncation or output-token cap is introduced.

A rejection does not convert the omitted note into a fact or append it automatically.
It records unresolved conservation, prevents complete coverage, and preserves the
independently verified selected answer. Unavailable review behaves similarly but
is separately labelled unavailable. Raw false exclusions still fail qualification,
even if these conservative runtime rules avoid false completeness. Zero exclusions
requires zero review calls and yields an empty, explicitly complete review set.

## Factual audit, completion and partial answers

Pass the selected candidate through the existing whole-answer original-source audit,
including editor/subset re-audit behavior. Selection and exclusion decisions never
certify factual support. Preserve exact source/context checks and newer-rejection
withholding behavior.

Keep the existing single append-only completion opportunity for unanswered planner
requirements, including comparisons/calculations not already explicit in reader
facts. The selection object exposes its candidate for unchanged-first-audit eligibility;
no fabricated requirement mappings are needed. Existing completion still audits the
entire combined candidate, validates final mappings and prohibits recursive repair.
It cannot rewrite the selected facts. It may add supported new assertions, but their
correctness is established by the independent source auditor, not by inventory IDs.
A narrowed plan cannot exclude a reader fact because initial selection sees the
original question directly. Missing reader facts remain a coverage/qualification
risk; this change does not claim to detect every absent source observation.

After all audit/editor/subset/completion operations, derive conservation against the
exact FINAL atomic candidate. A selected inventory fact survives only by exact
whole-unit text equality plus compatible original source binding: the final claim's
validated references must cover its proposed original span handles and document
identity. Match occurrences one-to-one in selection order; never reuse a final unit
to discharge multiple selected facts. Identical fragment text from two subjects or
documents is not identity. No substring or semantic-match guess is used. Record
final unit IDs. A separately approved duplicate is the only permitted many-to-one
conservation, through its explicitly declared delivered target. Conservative failure
to match equivalent alternate source references keeps that fact unresolved.
An approved duplicate survives only if its declared delivered target survives.
A removed or rewritten fact remains unresolved, even if the editor's new wording is
supported; this can conservatively underreport coverage but cannot silently certify
fact preservation. Rejected/unavailable exclusions remain unresolved as above.

## Receipt, coverage and restoration

Persist `finalization.fact_conservation` with version, exact inventory, dispositions,
review outcomes, final fact-to-unit mappings and status (complete/partial/unavailable).
Bind inventory, request identity, original snapshot, evaluated date, candidate,
answer, final ledger and source manifest with canonical digests. References retain
only existing span handles; original texts remain available through normal evidence.
This historical receipt records assessed model judgments, not a new proof of truth.

Run existing planner coverage and completion using their internal base receipts
first. Only after the final candidate is settled, bind fact conservation and derive
the public augmented coverage receipt. Thus completion's existing receipt validation
cannot accidentally consume a partly augmented receipt or create circular digests.
The conservation binding uses final candidate/answer/ledger/source/request identity;
it excludes public coverage. The public coverage binding adds the digest of the
complete conservation receipt. This is one-way binding, not a circular dependency.

The existing question_coverage receipt keeps planner aspect rows and its independent
assessment. Add an explicit conservation status/digest to its binding and derivation:
complete requires existing complete coverage AND complete conservation. Do not
rewrite planner requirements or pretend source IDs are user requirements. Partial
and unavailable conservation must remain visible and keep complete false. Provide
content-free reasons/counts in public coverage; the full receipt remains necessary
for persistence validation, not an implementation explanation in the chat UI. Show
a plain-language notice when source information may be missing or its preservation
check could not finish, including when all planner aspect rows say answered.

Saved-history and cache validation reconstruct inventory/disposition structure,
validate all source handles, compare exact final unit text/mappings and recompute
all receipt digests/statuses. Missing v3 conservation, altered inventory/exclusion,
changed sources/candidate/request or old-version receipts cannot certify v3 answers.
Retain existing historical invalid-receipt downgrade behavior; never rewrite saved
rows during read. Add both conservation binding fields to coverage parsing/restoration
atomically, and use pipeline version v3 to invalidate old cache identities.

If there is no verified final candidate, conservation cannot become complete.
Malformed selection fails the new stage without legacy-draft fallback. Exclusion
failure retains verified facts with unavailable/partial coverage. Empty selected
inventory cannot produce a verified complete answer; follow the existing safe empty
answer path with a content-free stage reason. Propagate cancellation; do not turn
cancelled transport into empty selection or accepted exclusions.

## Implementation and acceptance checklist

1. Implement immutable inventory/selection/exclusion protocol and tests at the module
   interface: exact text conservation, foreign refs, nested input fields, duplicate
   IDs/targets, falsey responses, mutation and cancellation. Include identical fragment
   text for two subjects/documents with one audited subset survivor; only its own
   source-bound occurrence can count. Test a large exclusion inventory with bounded
   worker creation, mixed completed/time-out results and undispatched rows. Include a
   follow-up whose prior user context selects one of two source subjects, plus a
   standalone multipart question where a narrowed plan must not suppress its second
   clause. Explicit positive and negative derived-comparison controls must traverse
   existing completion and combined audit, including newer rejection withholding.
2. Wire selector/reviewer into orchestrator and initial question pipeline, replacing
   composer. Preserve completion and source audit; update stage metrics/call ceiling.
3. Bind conservation to final text and public coverage, then add cache/history
   restoration tampering regressions. Update real HTTP/SSE delivery tests together.
4. Run focused regressions, full offline backend suite, relevant frontend/CI checks;
   independently review actual diff and resolve defects before freezing candidate.
5. Fresh unchanged twelve-case initial and forty-eight-case all-mode evaluation.
   If any fails, preserve the failed candidate; do not resume it after code changes.
6. Complete live harness assembly, original-corpus checks, independently graded real
   cross-domain queries, sealed holdout and visual browser checks. Then reviewed
   activation configuration and exact-head GitOps release, followed by production
   verification. Nothing here marks those remaining gates complete.
