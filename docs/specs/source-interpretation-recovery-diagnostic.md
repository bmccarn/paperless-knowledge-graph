# Source interpretation recovery diagnostic

Status: protocol draft; diagnostic module reviewed separately. Native execution
requires frozen inputs, gold, code/runtime, wire preflight and two independent
admission receipts. No native calls have run under this protocol.

## Question

Does one document-local omission review recover required source meaning while the
unchanged independent source auditor rejects incorrect interpretations? This is
alternative A from [source interpretation recovery](source-interpretation-recovery.md).
It tests the recovery mechanism on known development failures, not generalization,
whole-query quality, archive completeness or deployment readiness.

## Immutable inputs and matched arms

Use all twelve full-original (F) primary readings from the completed B2 run: six
cases in both frozen repetitions. Bind B2's original manifest, gold, run, result
inventory and both grades/adjudication; never modify those artifacts. Copy the exact
originals, question, requirements, evaluation date, date order, windows and primary
readings into a new exclusive private package. These are known development inputs.
The retained (R) arm is not rerun. The four short cross-domain controls remain in
scope; no failed long original may be replaced by a synthetic example.

- Baseline: audit the complete unchanged frozen primary inventory.
- Recovery: run one omission review, then audit the primary inventory followed by
  every addition. Preserve primary order, text, references and duplicate occurrences.

Both auditor contexts contain the identical original source, question, requirements,
evaluation date, date order and frozen primary notes. Only candidate observations
change. Recovery cannot receive baseline audit judgments or another document's
notes. Construct prepared evidence directly from admitted frozen snapshots; calling
QuestionEvidence.prepare would rerun the reader and invalidate this comparison.

Use the existing source auditor and AnswerFinalizer with repairer=None,
allow_subset=True, four-observation batches and unchanged source/temporal/scalar
acceptance. The finalizer's supported subset must receive its normal independent
re-audit. No selector, fact exclusion, planner, composer, coverage model, editor,
retrieval, embedding, database mutation or ingestion may execute.

## Schedule and bounded execution

There are twelve matched pairs. Repetition 1 follows B2 case order and executes
baseline before recovery/combined audit. Repetition 2 reverses case order and
executes recovery/combined audit before baseline. Each baseline gets fresh immutable
inputs, including when it runs after recovery. No adaptive case/prompt selection.

Limits proposed for exact admission: 32 native attempts per pair, 384 aggregate,
300 seconds per pair and 3,600 seconds aggregate active execution. The owner passes
one inherited monotonic deadline through recovery and both finalizations; corrections
and subset audits cannot restart it. Existing lower provider timeouts remain in
force. No application output-token cap or SDK/agent retry is added.

Recovery allows one initial call and only one correction for nonempty malformed
reading protocol. Existing audit protocol corrections and subset audits count
against the same pair budget. Valid semantic failures never earn retries. Budget
exhaustion fails the affected pair and preserves all artifacts; do not truncate
observations, drop a batch, resume a package or repurpose unused attempts.

Individual transport, protocol, deadline and budget failures are recorded, then
scheduled controls continue within the remaining aggregate budget. Shared integrity
or capture failure and external cancellation stop execution, join owned work and
mark unrun pairs explicitly. A capture failure must not become an empty response.
Record pending pair/stage state before dispatch; account spent calls before cleanup.

## Admission and wire checks

Bind every exact input file, primary reading, original, unchanged gold and this
protocol; all application/scripts/lock hashes; model, provider route hash, SDK/runtime,
timeouts, concurrency, cache bypass and retry settings; and the fixed schedule and
budgets. Two independent reviewers sign that exact admission subject.

Serialize every known recovery request and baseline audit request through the pinned
SDK with local MockTransport before native admission. Measure system/schema/context
and complete request bytes; verify closure of every mock client. Candidate audit
bodies depend on real additions, so they cannot be predeclared as measured. Freeze
and validate their deterministic construction, measure/hash each actual request
before dispatch and preserve it. Provider capacity stays explicitly unknown unless
verified. An oversized/unaccepted request is a recorded feasibility failure, never
permission to summarize or omit source text. No 25 MB global inventory is sent in
this document-local diagnostic; G4 remains separate.

## Artifact and grading contract

Retain every recovery response and raw auditor attempt, including malformed and
failed attempts; native terminal evidence, request/response hashes, timing/usage;
primary/addition occurrence IDs and execution receipts; original audit decisions,
subset selection/rejections, subset re-audit and final delivered ledger. A successful
subset replaces the finalizer's returned ledger, so capture pre-subset decisions
separately at dispatch/response time. Never infer them from surviving claims alone.

Both reviewers independently inspect the original first, then every primary,
addition, raw audit decision and final delivered observation. Preserve B2 primary
errors as immutable known inputs. Separately score:

1. Newly recovered and still missing supplied required meanings, with exact sources.
2. Every new unsupported assertion, condition/role loss or reference defect.
3. Every false raw audit support approval, even if a later attempt rejects it.
4. Correct primary facts retained or wrongly rejected; all occurrence dispositions.
5. Final meaning retention and support, distinct from execution and conservation.

Combined delivery passes only if every supplied required meaning survives with owned
supporting references, no unsupported assertion survives, no new addition introduces
a semantic defect and no raw auditor falsely approves a claim. Protocol correction
cannot erase an earlier semantic defect. Rejecting a false primary keeps its
occurrence conservation partial, even when an addition recovers the requested
meaning. The unchanged baseline may reproduce omissions; it is never relabelled.

A correct addition cannot provide missing context to make a false sibling observation
true. Deterministic adversarial coverage must verify this full-observation rejection
and subset re-audit before admission. Native semantic results cannot be inferred
from that synthetic check. Reviewer disagreement blocks advancement until resolved
against frozen evidence without obtaining more outputs or editing gold.

## Decision

Failure preserves G3's open status and its exact failure mechanism. Success permits
a separately reviewed integration and fresh reader-plus-recovery comparison; it
does not close G4–G6. G4 must resolve global audit/coverage/completion payloads before
whole-query, all-mode, live, fresh held-out and matched-baseline evaluations. Actual
visual browser acceptance and GitOps deployment remain subsequent gates.
