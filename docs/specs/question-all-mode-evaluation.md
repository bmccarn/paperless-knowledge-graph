# All-mode question pipeline development evaluation

Status: implementation independently reviewed on both axes; the 12-case initial
slice passed. Native all-mode qualification is pending. The production query
pipeline remains inactive.

## Purpose and boundary

Exercise the actual planner, original-source reader, composer, finalizer and final
coverage in Quick, Deep, Timeline and Strict with identical supplied originals.
This qualifies mode-dependent post-retrieval behavior, not live retrieval or archive
completeness. Preserve the existing development rubric and all additional-fact
checks. The sealed holdout remains separate.

## Admission and immutable execution

Retain the original Strict initial-stage contract. Add an explicit all-modes stage
with 48 scheduled case-runs: dataset order, then Quick, Deep, Timeline and Strict
within each case. Freeze this exact schedule, stage, all code/runtime/dependency
hashes, dataset bytes, and budget in a new manifest. Budget: 900 native calls,
3,600 active elapsed seconds and an estimated 5 million tokens. There is no output
token cap. Native call and elapsed budgets are hard stops.

Require all 12 initial case artifacts and independent exact-result-SHA pass receipts.
Validate the complete original Strict manifest contract, including retry/cache,
retrieval and all other execution policies; only the named code-hash exceptions
below may differ. Validate case identity, result/coverage bindings and absence
of execution errors. Require the identical dataset, application code, dependency
lock and runtime. Only explicitly reviewed extension paths may differ:
`scripts/eval_question_pipeline.py` and the named evaluation spec files. Require
complete code-key-set and hash equality everywhere else, including
`scripts/eval_source_audit.py`, application modules and the dependency lock. This
extends execution controls, not the candidate. Bind the admitted manifest and all
initial case/result/review and attempt-input/attempt-output hashes into a reviewed
artifact manifest. Missing or changed raw artifacts invalidate admission. Capture
bytes once for validation and hashing, then bind this artifact manifest into the
new manifest and revalidate before each run.
Initial approvals admit the experiment but do not substitute for any all-mode grade.
Reject initial slices that exceed either frozen aggregate call or elapsed budget.
Require successful planning and a complete or partial restored coverage receipt;
valid unavailable coverage or coarse planning does not qualify for this stage.
A valid conservatively partial coverage receipt remains admissible when both
independent graders passed the delivered required aspects; do not require the
model to claim complete coverage.

The request identity must include the manifest, stage, schedule index, case ID
and exact mode. Carry it through plan, finalization and the coverage binding;
validate the expected scheduled identity when accepting results. Different modes
of one case must never share a request identity.

Each result must identify the scheduled mode and case index. Check result mode,
final mode, plan mode and case identity against that schedule when accepting a
prior review. Bind every prior review to its exact result and current manifest. Each new result
must include the exact input/output attempt hash inventory captured at creation;
continuation checks must require every raw file and match that inventory before
accepting the review. Missing or changed raw verdicts cannot retain admission.
Run one scheduled case per invocation and require both independent original-source
reviews before the next. Stop on a false approval, missing required aspect,
unavailable or incomplete run; retain every failure. Do not silently skip or rerun.

For Timeline, validate the production `restore_timeline` result and require
`ready` or valid `no_dates`, never unavailable. Independently grade every projected
date, precision and claim/source association against originals, preserving the
full observation context so signature, request and effective dates are not
relabelled. A correct answer with invalid or wrong projected events fails this
mode-dependent stage. Test tampered/missing events and projection receipts.

Every native attempt, repair, final candidate and receipt is retained privately in
exclusive-create artifacts. Count raw and delivered false approvals independently;
a deterministic rejection does not erase an incorrect raw approval. Record
conservative coverage under-reporting and unnecessary repairs separately. Usage
that was not reported remains unknown. No ingestion, serving-runtime change or
GitOps activation is part of this experiment.

## Verification before native calls

Exercise schedule completeness and ordering, all-mode admission rejection for a
missing/failed/altered initial review or different candidate/runtime/dataset,
mode-swapped prior results and immutable input capture. Verify invalid admission
fails before model invocation or output creation. Existing Strict invocation and
its frozen artifacts must remain readable and unchanged. Independent review must
approve the extension before generating the all-mode manifest.
