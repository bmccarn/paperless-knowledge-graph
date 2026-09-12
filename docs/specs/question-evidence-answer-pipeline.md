# Question requirements, original evidence and verified answers

Status: reviewed design, implemented as an inactive development candidate. Native
quality and release gates remain open. Parent: [document-local evidence](document-local-evidence.md).
Tracker: issue #33. No production activation is implied by this document.

## Outcome

A question should receive the supported facts needed to answer it, with specific
unresolved aspects visible. The same contract covers lookup, current-state,
comparison, inventory and history questions in every document domain. Retrieval
depth may vary by mode; factual acceptance must not. Accuracy takes priority over
latency. A well-formed model response is still an interpretation requiring original
source verification. No insurance identifiers, vocabulary exceptions or question
substring patches belong in the implementation.

## Checked-in behavior motivating the change

At application code `9953b89`, query.py first synthesizes a draft for gap retrieval,
then supplies that draft, graph summaries, conversation and truncated source
presentations to a second free-prose synthesis. The independent finalizer audits
that prose and may reconstruct structured observations only after a failed audit.
Its initial candidate has no explicit requested-aspect mapping. Quick planning and
retrieval are shallow and its finalizer explicitly returns an unaudited result.
The new document-local strategy is constructor-only and has not changed that flow.

The retained audit experiment at `8632ebf` supports further work on isolated readers:
corrected local reading preserved all nine positives and rejected all three
negative observations. It does not qualify a complete answer pipeline. Broader
original-table development evaluation is running separately; preserve its results
before selecting an implementation. Do not substitute engineering test counts for
source accuracy or user-question coverage.

## Contracts and boundaries

### 1. Requested aspects

Extend the reviewed query-plan contract with ordered stable requirement IDs,
plain-language requested aspects, and explicit temporal/comparison scope. The plan
may resolve follow-up references using conversation context, but earlier assistant
answers are never evidence. Requirements describe the user's question, not facts
assumed true. Keep original question and evaluated date alongside any resolved
question. Unknown referents remain unresolved rather than silently invented.

The planner must not add adjacent document facts as requirements. Validate unique
IDs, shape and scope before retrieval. Planner unavailability is an explicit
planning failure; a deterministic direct-question requirement may allow existing
retrieval to continue, but must be labelled coarse coverage rather than a complete
semantic decomposition. Quick may still select one retrieval pass.

### 2. Request-local original reading

After retrieval and source certification, group coherent original windows by
source document. Reuse the reviewed candidate-blind local reader and bounded
reference recovery. Give it the question, requirements and only its own original
windows. Graph statements and generated summaries can identify retrieval targets,
never stand in for original evidence. The reader returns observations, exact
original references and interpretation limits, including action/date/value roles.

Read once per immutable question/requirements/evidence snapshot, before composing
an answer. Reuse the resulting untrusted notes only within that request and exact
snapshot; a changed source pack or requirement set invalidates them. No persistent
model cache, reindex or ingestion reset. Preserve every original source window for
independent verification; notes are neither replacement text nor authoritative
facts. For the first integrated candidate, existing retrieval/gap passes finish before
this reading stage; the pipeline adds no source-expansion loop. Any later supported
expansion must be a separately reviewed change producing a new certified snapshot. Do not concatenate unrelated snippets into synthetic originals.

Maintain bounded concurrency, cancellation and provider-context handling. Physical
model context limits remain real; report overflow or split coherent work explicitly
rather than silently discarding original certifying context. The first integrated
candidate does not automatically split a document further: overflow is an explicit
reader-unavailable result, not permission for an unbounded split/retry loop. No arbitrary output
max-token cap is added.

### 3. Cross-document reconciliation and composition

Introduce one explicit Strands reconciliation/composition stage. It consumes the
requirements, dated original documents and untrusted reader observations; it does
not consume the old free-prose draft. Return schema-validated, self-contained plain
observations using the existing ObservationCandidate contract, plus a mapping from
requirement IDs to proposed observation IDs and unresolved requirement reasons.
Reference proposals must belong to the certified snapshot; they guide verification
but never exclude competing original records or grant truth by themselves.

Distinguish requested/signed/selected/completed actions, original versus referenced
instruments, document versus event dates, recorded periods and current-world status.
Reconcile only source-established relationships. Chronology alone cannot prove
replacement, cancellation or archive completeness. Preserve required earlier and
latest documented observations for each relevant subject. Comparison claims need
scoped alternatives; otherwise deliver dated observations and a precise unresolved
comparison. A source's explicit signed amount can be reported as printed; do not
relax sign guards to turn an arbitrary negative into a reduction.

Composition must preserve question coverage without adding unrelated supported
facts. Neither dropping difficult requirements nor including every retrieved fact
counts as a successful answer. An empty or malformed result is unavailable and
cannot be rescued by emitting the prior unaudited draft.

### 4. Claim-local verification and delivery

Allow the finalizer to accept a validated ObservationCandidate as its initial
candidate, preserving atomic-unit IDs and digest from the first audit onward.
Independently verify each proposed observation against original source context,
including necessary alternatives. Keep source ownership, quantity/date/role,
comparison, citation and exact-candidate checks. Reader and reconciliation notes
remain untrusted inputs; adversarial notes must not change original authority.

After verification, map supported observations back to requirements. Validate that
the map names existing requirement and observation IDs; the mapping itself cannot
prove a requirement was answered. A separate coverage assessment must check the
actual supported text against each requested aspect and report answered, partial,
unresolved or unavailable. Coverage interpretation does not certify factual claims.

Retain only independently re-audited complete supported observations for a partial
answer. Omitted or failed observations cannot lend subject, date or condition
context to surviving units. Never attach an earlier candidate's ledger after
repair. Preserve existing fail-closed behavior for execution failures and corrupted
provenance. Any change to subset eligibility must have its own reproduced tests
and review; this design does not authorize simply deleting safety checks.

Quick, Deep, Timeline and Strict use the same factual acceptance layer; mode changes
retrieval effort and presentation only. Do not stream factual candidate text before
acceptance. Preserve progress events, cancellation, saved conversations and source
links. Timeline projection uses source-validated observations and dates, not a
second independent narrative capable of inventing events.

### 5. Honest user-facing state

Expose factual support and requested-aspect coverage separately. A fully audited
partial answer must display its supported text, omitted-aspect count and specific
gaps. It must not look like a complete archive inventory or a blanket failed answer.
Do not turn audit coverage into factual certainty or a fabricated probability.
Transport/planning/reader/composition/verification failures get identifiable states.
Saved and streamed responses must display the same final answer, citations and
coverage. Changing metadata must not silently upgrade previously saved answers.

## Review-resolved execution and receipt contract

The query orchestrator owns the pipeline. Planning, reading, composition, editing,
source verification and coverage use the configured Strands model route and shared
concurrency semaphore. The first candidate adds zero retrieval expansion passes
and zero recursive document splits. Existing retrieval limits remain explicit in
the manifest. Reader reference recovery is at most one extra call per document;
composition and coverage each get one native call and no semantic retry. Existing
single protocol correction per audit batch and one candidate repair remain the
only verifier/editor retries. The native qualification harness uses one SDK attempt;
production transport retries must be separately recorded, never called native stages.

For D document packets and audited batch counts B0 (initial), B1 (optional repair)
and Bs (optional subset), the pipeline native-call ceiling after retrieval is
`1 planner + 2D readers + 1 composer + 2(B0+B1+Bs) auditors + 1 editor + 1 coverage`.
Absent optional passes contribute zero. Each B is bounded by the existing audit
unit/batch settings. Publish both this computed ceiling and the per-stage counts.
Existing call and audit deadlines apply; coverage/composition use the configured
Strands call deadline. Deadline/overflow/protocol failure is terminal for that
stage; it cannot start an undeclared pass or emit an unaudited draft. Cancellation
drains child tasks and cancels later stages. Evaluation additionally has a finite
manifest wall-clock ceiling. Raising a deadline or changing transport attempts
requires a new frozen measurement, not an invisible retry.

The single coverage call runs only after the final supported candidate is selected.
It sees the original and resolved questions, original requested aspects, and exact
final supported observations. It must check for planner-omitted subjects/time aspects
against the question, rather than merely marking every generated requirement done.
A malformed response, timeout, omitted-aspect mismatch or coarse planner fallback
cannot report complete coverage. The mapping and statuses are interpretation
metadata, not new factual content. Use fixed, nonfactual gap categories such as
`not_answered`, `partially_answered`, `planning_unavailable` and `coverage_unavailable`;
free-form explanations asserting additional source/world facts must not be displayed
without their own normal source audit. Coverage failure can deliver already verified
facts with coverage explicitly unavailable; it cannot withhold/overwrite those facts
or present them as a complete answer. Overall request cancellation still cancels
delivery. Native evaluation must test false-complete coverage independently.

Bind every coverage receipt to hashes of the original/resolved question, requirement
set, immutable source snapshot, pipeline version and final delivered candidate digest.
Proposed composition mappings are never final coverage receipts. Repair replaces IDs;
subset extraction can renumber them. Discard earlier mappings and assess the final
candidate anew after either operation, including duplicated/reordered observation
text. Validate all returned IDs against that final candidate and requirements.

Introduce a new explicit pipeline/policy cache version for activation, included in
cache admission before returning a hit. Legacy unaudited Quick entries and entries
without valid matching receipt bindings cannot satisfy the new all-mode path. No
global cache flush is needed. Persist the receipt with the exact response; stream,
HTTP, cache and restoration validate equivalent bindings. Legacy saved conversations
remain historical records with their original verification state, never silently
upgraded or represented as new-pipeline answers. Corrupted/mismatched restored
receipts show unavailable coverage and cannot present newly verified status. Test
preexisting Quick cache entries, changed question/snapshot, dropped first observation,
reordered repair, duplicate text, malformed coverage IDs, timeout and cancellation.

## Implementation sequence and gates

1. Review this design independently against the domain contract and the current
   original-table native results. Resolve failed positive cases without relabelling
   gold or expanding production heuristics for a particular private document.
2. Add minimal requirements and request-local reading interfaces with mechanical
   isolation, ownership, conservation, invalidation and cancellation tests. Preserve
   the old runtime default until the integrated candidate is qualified.
3. Implement structured reconciliation/composition and initial ObservationCandidate
   finalization. Test requirement/observation identity, lost subjects, swapped roles,
   malicious reader notes, stale source snapshots and incomplete provider responses.
4. Integrate all four modes and coverage delivery. Test mixed-supported answers,
   meaningful history plus latest subjects, current-state uncertainty, comparison,
   short lookups and follow-ups. HTTP and SSE must conserve identical final payloads.
5. Freeze the complete candidate and updated SDK/runtime for native end-to-end
   development and representative stress. Compare required-aspect preservation and
   raw/delivered false approvals, not just parser success. Keep bounded manifests,
   all failed attempts, usage and timing. Stop on false approvals or lost critical
   positives; diagnose the responsible stage before changing the next candidate.
6. Only after development gates pass, have the independent custodian run the sealed
   holdout under its predeclared release contract. Any adaptation consumes that
   holdout; do not claim it remains independent after inspecting/tuning on results.
7. Build the exact candidate, exercise real UI controls and inspect screenshots of
   actual rendered answers, sources, saved history, graph zoom, loading, cancellation,
   partial and failed states. Include desktop and narrow layouts. Distinguish live
   model/original-source acceptance from synthetic browser contracts. If native
   computer control stays unavailable, evaluate a supported browser-control fallback
   explicitly; a claimed visual pass requires actual interaction and inspection.
8. Review exact-head CI and immutable image, merge only the qualified application,
   update homelab GitOps, and verify Flux/runtime revision. Preserve all indexed data
   and ingestion history. No resync, re-extraction or migration is inherently needed.

## Release evidence

Publish a concise ledger of reproduced defects versus model interpretations and
architecture hypotheses, development and independent holdout results, all-mode
end-to-end coverage, actual visual receipts, dependency versions and production
revision. Report remaining gaps directly. A retained-case pass, SDK update or clean
code review alone does not close issue #33.

## Development selection after the original-table run

The completed three-arm original-table experiment had no false approvals and no
execution failures, but lost 10/12/11 positive observations. It does not pass the
original development gate. Two independently reviewed implementation follow-ups
preserve intact table rows and permit one scope-metadata protocol correction only
when no row in the batch contains a negative factual decision. The latter cannot
fix the mixed-label evaluation batches and is not represented as doing so.

The remaining signed-credit paraphrase requires a different composition, not an
absolute-value exception: a candidate may describe the adjustment using its printed
negative amount. One separate synthetic balance/statement association was disputed
by both source reviewers; retain its original gold and failed scores unchanged.
Neither issue justifies retries until an approval appears or relaxing source roles.

Proceed with the already-designed integrated pipeline as an inactive development
candidate so the next evaluation can assess answers composed from originals, rather
than continuing to optimize fixed paraphrases in isolation. This changes the order
of engineering work, not release acceptance. Existing failed receipts remain failed.
No default, cache policy or live model route changes until the complete candidate
passes native end-to-end development and independent holdout gates. Initial tests
must include printed signed adjustments, unchanged rejection of wrong-sign facts,
and ambiguous associations reported as unresolved instead of forced into an answer.

The first implementation slice adds a validated initial ObservationCandidate input
to the finalizer. It preserves the exact canonical units/digest from the first audit,
checks all structured candidates even in Quick mode, and retains the existing
repair/subset re-audit and provenance gates. Legacy string callers keep their
existing behavior until all-mode integration is separately reviewed. Directly
constructed invalid candidates must be rejected before any model call. This slice
alone is infrastructure, not a qualified answer improvement or production activation.
