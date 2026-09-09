# Source reading before claim verification

Status: evaluation candidate plan; no production activation. Implements the user's
request to test a shared, domain-independent source-reading stage. Parent:
[query reliability evaluation](query-reliability-evaluation.md), issue #33.
Accuracy and preservation of useful supported facts outrank latency, as explicitly
clarified by the user. Latency and cost remain measured, not hidden.

## Evidence and decision scope

The frozen context comparison reproduced six false semantic approvals, including
source-valid broad context. Reordering and removing noncontiguous items each left
two failures in three observations. A four-document control rejected the negative
throughout but exposed three adapter false rejections. See the G1 report. This
justifies candidate experiments, not release. Neither insurance-specific rules nor
source truncation are candidates. Remaining representative and end-to-end G1/G5
work cannot be replaced by a successful compact assertion result.

## Three matched implementations

- `flat`: unchanged native audit request, schema, parsing and finalization.
- `grouped`: identical request except source windows are grouped by original
  document identity. Every window retains all fields, exact text, identity and
  original ordinal; no source is omitted, summarized, merged or made authoritative.
  First-occurrence document order and within-document order are preserved.
- `source_first`: the grouped verifier receives an additional prior source reading.
  A separate stateless Strands invocation receives only the original user question,
  evaluation date/date order and complete grouped sources. It never receives answer
  units, answer/conversation context, prior verdicts or correction instructions.
  It returns one reading per supplied document: source-linked observations and
  explicit limits/ambiguities, identifying field roles, selected actions, date roles,
  quantities and contradictions relevant to the question. A document can have no
  relevant observations; it must still be represented. A second fresh invocation
  assesses the exact candidate with all original windows and the reading notes.
  Notes are explicitly untrusted interpretations, never certifying sources. The
  verifier must independently check them and select original handles only.

This is one reader call per native audit invocation, not per document. It tests
candidate-blind reading without changing source volume. It may still fail under
broad context; per-document reading would be a different, separately bounded
experiment. No persistent reading cache or new database state is introduced.

## Interface and failure behavior

Keep the production `audit_answer_units(question, units, spans, plan)` interface.
A constructor-only strategy defaults to `flat`; the application has no new runtime
setting or default activation. The evaluator freezes the named strategy in its
manifest and uses the same application implementation as future callers.

The source-reading module owns grouping and a strict request-owned reading schema.
Require exactly the supplied document identities, no duplicates or foreign handles,
and references belonging to the observation's document. Reject malformed, empty,
missing-document or extra-field responses. A schema-valid note is still not proof.
Reader failure makes the audit unavailable, without silently falling back to flat
verification. Cancellation propagates. No semantic retries are added. Existing
protocol corrections can invoke the strategy again and all reader/verifier calls
count against the experiment ceiling. Existing call timeout, finalizer deadline,
concurrency, model route and unlimited model output remain unchanged for this
experiment; an overrunning candidate fails rather than extending them implicitly.

Do not combine known comparison-metadata or handle-protocol fixes into this
experiment: that would obscure which change caused an improvement. Record their
raw versus normalized effects. Justified protocol fixes belong in the subsequently
reviewed complete design before release qualification.

## Implementation and validation sequence

1. Independently review this candidate plan. Add behavior tests at the native audit
   interface for answer blindness, exact source conservation, original-only
   attribution (including a schema-valid false note with valid original handles),
   malformed/missing/foreign reading rejection, cancellation, strategy
   default and manifest binding. Controlled transports prove mechanics only.
2. Implement grouping, reading protocol and source-first orchestration. Freeze the
   code after both reviews pass; no production deployment or model call yet.
3. Prepare immutable matched manifests for all three strategies on the existing
   64-assertion, four-domain development set, three repetitions. Per strategy:
   36 case runs; ceilings 96 flat/grouped or 192 source-first native invocations,
   including corrections; 1,800 seconds; estimates 300k flat/grouped or 600k
   source-first tokens. At the previously captured maximum output rate the estimates
   are $1.125/$1.125/$2.25, not billing guarantees or token output caps. Request-level
   proxy cache bypass; no global flush or independence claim.
4. Prepare matched private manifests using only the already approved four claims
   and source-valid 183-window case at the same destination. Three repetitions per
   strategy; ceilings six flat/grouped or twelve source-first calls including
   corrections; 600 seconds per strategy; estimates 600k/600k/1.2M tokens
   ($2.25/$2.25/$4.50 at the previously captured output rate). These are new named
   experiments, not reuse of an earlier exhausted experiment allowance. Admission
   must inspect the concrete manifests and current user authorization before calls.
5. Retain every result. A candidate must eliminate false approvals in matched
   retained cases and preserve all critical positives to advance; source-first must
   show an advantage over grouped to justify its added stage. Existing protocol
   false rejections remain failures, classified separately. Neither a convenient
   rerun nor a post-result gold-label change counts as success.
6. If successful, complete representative stress and question-coverage evaluation,
   write/review the measured architecture decision, implement the complete justified
   change and qualify frozen holdouts and end-to-end cases. Quick currently bypasses auditing in `AnswerFinalizer.finalize`; G3/G4 must
   explicitly integrate the selected verification contract into Quick and test its
   completion, partial and error behavior. This candidate alone does not improve
   Quick. All four query modes, multiple domains, original source links, saved history and actual visual browser
   acceptance remain required before the GitOps release gate. If candidates fail,
   record the result and revise hypotheses, without another production patch.

Before qualification, include a frozen native-model fault control with deliberately
false reader notes and valid original handles. The verifier must reject the false
assertion using the unchanged originals; a mock-only source-value rejection cannot
prove resistance to semantically false notes. This is an adversarial control, not
a third architecture candidate, and needs its own frozen case/call budget.

This experiment evaluates the audit stage used by non-Quick query methods.
Passing it alone does not establish retrieval completeness or end-to-end mode
behavior. No claim of holistic improvement precedes those later measurements.
