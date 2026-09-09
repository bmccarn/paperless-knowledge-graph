# Recover requested facts omitted during answer construction

Status: design independently reviewed; implementation under review and native
effectiveness not yet measured. The stopped v1 run remains failed.

## Reproduced problem

The frozen all-mode development run stopped at case17 (credit, Deep). Both reviewers
found the same loss: the original and source reader contained the original charge,
but the composer omitted it and marked the charge requirement unresolved. The final
coverage stage correctly reported partial coverage. The pipeline returned that
partial answer without attempting to recover the source-supported missing fact.
The first17 cases passed; this failure and its artifacts remain unchanged.

This is a question-completion defect, distinct from unsupported-claim detection.
More retrieval would not address this reproduction: the necessary original was
already supplied. The change must work for every domain and mode, without field,
document, currency, date or question-specific rules.

## Behavior

After the first independently audited answer and its valid coverage assessment,
allow one source-grounded completion pass when named requested aspects remain
partial or unresolved. A complete assessment needs no additional model call.
Unavailable coverage, a coarse plan, an invalid final candidate or a failed source
audit must not trigger this pass. A plan that omitted requested aspects remains
incomplete; this step cannot silently invent or certify a replacement plan.
Only an unchanged first composition that passed its first complete audit is
eligible. Answers reached through editor repair or supported-subset salvage do
not enter completion, avoiding a new opportunity to reverse a prior rejection.

The completion worker receives the original question, requirements, full immutable
original evidence and reading notes, existing verified observations, and the named
coverage gaps. It proposes only additional observations that address those gaps.
It may return no additions where evidence is absent or ambiguous. It must preserve
date precision, actor/action/value roles, conflicting records and uncertainty. A
gap is not permission to invent a fact or turn an application into a completed event.
Reading notes, prior answers and coverage assessments remain untrusted proposals.

Validate the structured proposal: every added observation has known original span
references and maps to at least one targeted requirement; every target is accounted
for; reject unknown IDs, malformed responses and exact duplicates of existing
observations. Source references are proposals, not support certificates.

Construct a combined candidate by appending validated additions to the exact
existing observations. Independently audit the entire combined candidate through
the existing original-source auditor and deterministic guards. Do not invoke the
general answer editor on this completion attempt: it could rewrite or remove
previously supported observations. Any subset produced by the finalizer must not
be accepted as the combined result unless its complete observation sequence is
identical to the submitted combined candidate. Rebuild coverage and Timeline from
the new final candidate; never attach the previous ledger or coverage to new text.

Accept the combined result only if every combined observation is independently
supported and the new coverage receipt is valid complete or partial.
Require the new final coverage assessment to attribute every added observation
to at least one of the targeted gaps; proposal mappings alone cannot establish
relevance. A supported but unrelated appendix is not a successful completion.
For all other unsuccessful extensions, retain the
unchanged original verified answer for a technical extension failure, no additions,
or rejection confined to new observations. If a newer semantic audit rejects or
conflicts with any original observation, do not restore its earlier approval:
withhold the factual answer with an identifiable unverified recovery failure.
Inspect the full combined ledger before any finalizer subset can remove rejected
observations; a subset must not conceal revocation of prior support. Record
recovery disposition; retaining a prior answer must not hide any failed raw audit
attempt from evaluation. Cancellation remains caller-owned and cancels the whole
request. No second completion pass or rerun-until-success is permitted.

Partial coverage is a legitimate outcome when originals lack the requested facts.
The pass guarantees an explicit opportunity to recover an omission, not that a
model will recover every fact. Independent native grading remains the acceptance
criterion; model coverage scores are never gold.

## Implementation boundaries

Implement the worker alongside structured composition and call it from the inactive
question pipeline after coverage. Keep the existing first-pass composer, original
reader and factual-support policy. Add content-free completion stage metrics and
correct the native call ceiling to include one completion call, the combined audit
batches with their existing single protocol correction, and a second coverage
assessment. No additional editor or subset audit is allowed in recovery. Bind the
changed algorithm to a new pipeline version so caches and saved receipts do not
misrepresent the earlier implementation. Existing historical text stays readable.

Retain recovery disposition and hashes identifying the before/combined candidate in
final diagnostics; raw inputs, native outputs and every audit stay in evaluation
artifacts. No production flag change, ingestion or index repair is needed.

## Verification and rollout

First add offline regressions that reproduce an omitted known fact and require a
new original-source audit before it appears. Cover all modes, complete/no-call,
missing evidence/no-addition, malformed/unknown reference, invented addition,
supported but irrelevant addition, repaired/subset first-pass ineligibility,
changed or removed prior observation, newer rejection of a prior approval,
unavailable reassessment, cancellation,
and restored HTTP/SSE/cache/history/Timeline bindings. Tests use synthetic sources
outside the single reproduction as well as the retained reproduction contract.

Independently review the implementation before native evaluation. The stopped
all-mode run cannot be resumed under changed application code or counted as a
pass. Freeze a new candidate and rerun the initial twelve-case development slice,
then the full all-mode gate, without changing sources/questions/rubric to fit the
candidate. Continue to the separately reviewed live-retrieval run only on a complete
pass. The sealed holdout, actual browser delivery and exact-head GitOps release
checks remain required; passing a focused regression does not authorize deployment.
