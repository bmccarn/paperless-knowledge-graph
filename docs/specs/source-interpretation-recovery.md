# Source interpretation recovery

Status: the diagnostic-only module and runner are implemented and independently
reviewed. The exact v2 package has both technical admission receipts; native
execution is blocked on explicit downstream model-provider approval. The user
approved transfer to the pod and the remote package passed validation. No recovery
model calls have run. B2 grades completed FAIL. The completed B2 run remains immutable;
this design cannot relabel any of its failures.

## Problem and scope

Complete source transfer does not establish complete reading. In completed B2
outputs, a supplied replacement qualification is absent from both full-source
readings. Both independent grades fail the run; losses also include effective-time
association and a material condition, with additional record-role and reference
defects. The reader-retention gate remains closed. The module must preserve material conditions,
negative facts, scopes, selected options and action/date roles across any domain
and query mode. It must not contain document IDs, insurer names, particular dates,
amounts or gold facts.

The measured 919-document inventory also creates a distinct downstream payload
problem. This recovery proposal does not authorize sending that entire source
inventory to one auditor or coverage model. G4 requires separate stage-local
payload feasibility and verification design.

## Invariants

- Original Paperless text and exact source handles retain factual authority. Reader
  and recovery outputs remain unverified interpretations.
- Preserve the primary reading and every native attempt as immutable diagnostic
  evidence. Additional model output cannot silently rewrite, delete, approve or
  certify a prior observation.
- Keep the original question independent of planner hints. Each observation must
  identify its own source subject/record, scope and time sufficiently for audit.
- A clean model-produced omission checklist is not proof of source or answer
  completeness. Source acquisition, interpretation execution, factual support and
  answer coverage remain separate dimensions.
- No answer draft, expected gold, sibling document or sibling interpretation may
  enter a document-local recovery invocation. The primary reading of that same
  document is untrusted comparison material, never evidence.

## Proposed module and interface

Keep the caller-facing seam at QuestionEvidence.prepare. Internally, a source
interpretation module owns the primary reader and one bounded recovery pass per
document. It returns the conserved interpretation inventory plus execution
receipts; callers do not orchestrate another agent or infer confidence from it.

Conceptually:

    interpret(question, requirements, original_document, reader)
        -> InterpretationInventory(primary, additions, execution_receipt)

The initial reader remains unchanged. The recovery adapter receives the exact
original windows, original question/requirements and immutable primary reading.
It inspects which material source meaning is missing or overgeneralized and may
propose independently framed, source-referenced additions only. Empty additions
are permitted, but confer no completeness authority. Unknown document/span IDs,
extra protocol fields and replacement of primary observations are rejected.

The existing independent source audit remains mandatory for every primary and
added observation. Unsupported primary readings are not made true by additions;
their eventual rejected disposition must remain in the conservation ledger. A
semantic contradiction between primary and additional interpretations is not
resolved by picking the newer model output. Original-source audit decides whether
either observation is supportable; unresolved conflicts constrain completeness.

## Execution and failure ownership

Use bounded workers and the request owner's inherited deadline. No semantic retry
or open-ended reread loop. At most one recovery invocation plus the already
reviewed one protocol-only correction for nonempty malformed output. Transport,
empty, timeout and semantic omissions do not earn retries. Exact budgets belong
in a new preregistered experiment, not in a hidden application output-token cap.

A recovery failure retains the original reading for ordinary source audit and
marks interpretation review incomplete. It must not certify complete coverage or
discard facts already independently supported later in the pipeline. External
cancellation joins owned workers and clients before returning. Record source,
question, primary reading, addition and implementation identities so saved/cache
restoration cannot mix stages or promote an incomplete execution.

## Validation plan before activation

1. Finalize B2's independent grades, preserving all raw omissions, unsupported
   assertions and reference failures. Identify whether recovery can address the
   observed mechanism; do not silently redefine B2's pass rule.
2. Independently review this module interface and failure/authority rules. Compare
   the addition-only approach with a fresh blind second reading and explicit
   source-local verification before selecting the implementation.
3. Add deterministic conservation, ownership, failure, cancellation and cache
   regressions at the public interpretation seam, with cross-domain fixtures.
4. Predeclare a new matched diagnostic over the frozen original questions and
   gold. The B2 primary outputs may serve as immutable inputs to an isolated
   recovery experiment; their known failures remain recorded. Score newly supplied
   meanings and all new errors separately. Merely adding correct words cannot
   earn an end-to-end pass while unsupported earlier assertions remain deliverable.
5. Only a candidate whose independently audited delivered facts preserve required
   source meaning and reject unsupported assertions may advance. Run fresh whole
   reader/recovery/audit comparisons with unchanged whole-query acceptance rules;
   add held-out cases before production activation.

The review resolution below fixes primary-error treatment and receipt authority for
the isolated slice. Additive recovery benefit remains unmeasured. Live integration
of coverage/restoration and G4's large-source audit strategy remain separate work.

## Alternatives to resolve during review

A. **Addition-only review before audit.** The primary interpretation and original
   enter a distinct omission-review task. This fits the existing QuestionEvidence
   seam and conserves inventory, but cannot remove earlier unsupported assertions.
   Its meaningful acceptance surface must therefore include the ordinary audit;
   a correct addition alone cannot repair an incorrect delivered inventory.
B. **Independent blind reread.** A second reader sees no prior interpretation. This
   reduces anchoring, but the existing two full-source repetitions already show
   that unchanged repeated reading can omit the same qualification. Repetition
   alone is not an evidenced solution and increases work without targeting loss.
C. **Source-local audit followed by omission recovery.** Audit each primary
   interpretation against its owning original, then compare that original with
   retained supported meanings. Propose missing material observations and audit
   each addition. This handles omission and unsupported primary interpretations
   in the same module, but moves the seam and must preserve rejected dispositions,
   cross-document comparisons and the final answer audit. It overlaps G4's need
   to avoid copying the entire 25 MB inventory into every verification call.

Choose based on the completed failure ledger and the smallest public interface
that can conserve required meaning through independently verified delivery. A new
experiment must grade primary, added, rejected and delivered observations
separately. B2 remains failed; a later full-pipeline pass cannot rewrite it.

## First implementation slice (independently reviewed)

Test alternative A through an isolated diagnostic interface before changing
QuestionEvidence. `recover(payload, primary, adapter)` receives exactly one original
and its frozen primary reading. It returns immutable primary and addition readings,
raw attempt records and a content-bound execution receipt. A narrow Strands adapter
owns only the omission-review native call. Reuse the existing owned-reference
reading protocol; every returned row remains an unverified interpretation.

This slice does not add a configuration flag, alter production routing or cache
identities, replace the primary reader, or change finalizer acceptance. The existing
independent auditor/finalizer must test the combined inventory in a subsequent
admitted diagnostic. That diagnostic retains each prior observation's rejected,
unavailable or delivered disposition and re-audits any supported subset through the
existing finalizer. No new module may independently declare facts verified.

The first deterministic tests cover:

- Mutation isolation: modifying an adapter input or a returned copy cannot alter
  the primary record, original source or receipt.
- Source isolation: only the owning original, question, requirements and its primary
  reading reach recovery; no other document, prior answer or evaluator input.
- Conservation: every primary occurrence survives byte-for-byte before additions;
  even a wrong primary statement cannot be deleted by the recovery response.
- Failure: empty/transport/timeout output produces an incomplete receipt retaining
  primary facts; only nonempty malformed protocol earns one correction. Every raw
  attempt remains recorded, including corrected attempts.
- Ownership: foreign handles, duplicate documents, extra protocol fields and
  fabricated source identity cannot enter additions.
- Cancellation: cancellation propagates to the owning caller and cannot return an
  apparently completed review. Native clients remain owned by the existing adapter.
- Authority: a protocol-valid addition has no supported/complete truth label, and
  an empty valid addition list means review execution completed, not that the source
  or answer is complete.

A receipt binds implementation/prompt/schema, original question and optional planner
fields, complete source payload, primary reading, additions and raw attempts. This
is diagnostic identity, not a saved-answer certification mechanism. Any later live
integration requires separately reviewed coverage/cache restoration contracts and
fresh pipeline identity.

## Review resolution and selected contract

Both independent reviews select A for the next diagnostic implementation, with the
following requirements resolved before coding. B does not target the repeated
observed omission; C changes audit placement at the same time as recovery. Neither
alternative is established as ineffective generally. G4 may still require C.

The diagnostic interface is `recover(document_request, primary, adapter)`.
`document_request` has exactly question, evaluated_at, source_date_order and one
source_documents entry, plus resolved_question/requirements together when present.
These fields are copied exactly from the frozen primary request, not replanned.
Extra context, answer, gold and sibling document fields are rejected, not forwarded.
Validate request shape and primary reading before a call. Failed/malformed primary
reading is an invalid precondition; a valid empty primary is eligible for one
review because it can itself have omitted source meaning.

The returned immutable inventory has stable occurrence identities bound to request,
origin (`primary` or `addition`), ordinal, text and references. Preserve duplicate
occurrences; no semantic deduplication or replacement is authorized. Independently
bind request, primary, additions, occurrence inventory, raw attempts, prompt/schema
and implementation version. Return fresh copies to callers.

Execution status is exactly `completed`, `unavailable`, `invalid` or `timeout`.
A valid empty addition reading is `completed`; absent/empty text is `unavailable`,
two malformed responses are `invalid`, and an adapter timeout is `timeout`. Only
nonempty malformed reading protocol earns one correction, with unchanged originals
and primary notes. Other adapter/integrity errors propagate. Cancellation propagates
and does not produce a completion receipt. Pending status belongs to the owning
runner's durable schedule, not a returned successful result. Raw attempts are
retained even when invalid; protocol recovery cannot erase semantic defects.

`execution_complete` describes only protocol execution. There is no fact-support or
source-completeness certificate in this module. Failed recovery preserves the
primary with empty additions, explicitly incomplete execution. This first slice
has no live, cache or restoration path; later integration must bind receipts and
keep incomplete execution from becoming complete coverage on every such path.

For the subsequent native diagnostic, use the original frozen primary notes in
both matched auditor contexts. Audit the entire primary and the entire union with
identical originals, unchanged source-audit prompt/guards and no editor/repairer.
Capture the pre-subset assessments as well as any final subset: the finalizer
replaces its final ledger after successful subset verification. Every original
occurrence remains accounted for, including a rejected one. Rejection keeps
primary conservation partial even when a corrected addition recovers the required
meaning. Report meaning retention and occurrence conservation separately.

Acceptance requires every supplied required meaning in independently audited
surviving observations, zero unsupported surviving assertions, and zero false
support approvals in any raw audit attempt. A correct addition cannot launder an
unsupported primary into a supported verdict. Add a deterministic adversarial
control where corrected and incorrect versions coexist. Grade all new assertions,
all rejections and all raw attempts; preserve B2's failure unchanged. This is a
recovery-mechanism test, not whole-query, all-mode, held-out or deployment evidence.

The next native protocol is [recovery diagnostic](source-interpretation-recovery-diagnostic.md).
Its exact v2 package is technically admitted but has not executed. After explicit
user approval, private inputs were uploaded and remote validation passed. Automatic
approval review separately rejected model execution pending authorization to send
the source text through the configured LiteLLM route to Google Gemini. Unused B2 attempts are not its budget.
See the evaluation report for the frozen identities and remaining execution steps.
