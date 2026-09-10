# Source-grounded interpretation and verification

Status: both native model-comparison grades and their reconciliation are complete;
both routes FAIL. The deterministic date-spacing repair is reviewed and implemented.
The next source-scope hypothesis needs concrete interface review before implementation.
This plan does not activate a route or change factual acceptance. Parent:
[question evidence pipeline](question-evidence-answer-pipeline.md), issue #33.

## Outcome

Answer requested aspects using supported document facts, including conditions,
negative observations and justified comparisons. Preserve source precision and
distinguish printed values from calculations. The same behavior must apply to
lookup, inventory, comparison, current-state and history questions in every domain.

The completed reader/verifier comparison supplies the evidence for selecting the
next change. Its 36 executions completed without transport failures; independent
semantic grades and reconciliation are frozen in the linked diagnostic. Execution
success does not establish accuracy. Preserve disputed labels and prior failures;
new candidates cannot revise the frozen acceptance rules retrospectively.

## Reproduced implementation constraints

At application revision 6d72699, `QuestionEvidence.prepare` validates acquisition
and retains its inventory digest, but `audit_payload` supplies grouped windows
without a document-level coverage declaration. `source_reading.group_sources`
conserves fields and positions, but does not establish complete-original coverage.
The reader and verifier both warn that the supplied documents are not a complete
archive. These are correct archive limits; they do not distinguish a complete
individual original from excerpts of that original.

`answer_finalization.value_mismatches` requires literal numeric membership and
source-bound units. It intentionally rejects new computed quantities because no
calculation evidence protocol exists. Removing that check would also remove a
defense against incorrect model arithmetic. `source_dates` preserves unknown
centuries; the verifier explicitly requires matching year precision, while the
reader has only a general instruction against assumptions. Neither a model route
change nor another unsupported approval can repair these contract mismatches.

The native grades must separately establish which observed losses each constraint
caused. A code path that could explain a failure remains an architecture candidate
until joined to the exact reader, raw audit, resolved references and final output.

## Invariants

- Original OCR, immutable source identity and exact intervals are evidence.
  Interpretations, inferred labels and model verdicts are not new originals.
- Complete-original coverage describes supplied text, not OCR accuracy, all pages
  of an underlying physical record, archive completeness or present-world state.
- A document-local absence claim needs the complete relevant original and a
  semantic review of its scope. Coverage alone cannot establish absence. Explicit
  unchecked fields can support scoped observations even in a partial original.
- Preserve raw failed and rejected interpretations. A corrected observation or
  supported subset cannot erase a wrong approval or redefine the accuracy gate.
- Calculated values must be identified as calculated, bound to exact original
  operands and independently checked. A printed total remains a printed total even
  when it disagrees with a verified computation.
- Date formatting cannot silently supply a century, timezone, action date or
  event association. Unit formatting cannot turn nearby currency into authority
  for an unrelated numeric field.
- No document identifiers, insurer vocabulary, private amounts or gold-answer
  exceptions enter implementation. No ingestion, reindex or corpus reset is needed.

## Implementation sequence and review gates

1. **Close the diagnostic.** Reconcile both complete grades against originals;
   report required-meaning retention, false approvals, wrong rejections and
   unsupported deliveries separately for each route and trial. Keep calendar
   policy and uncertain labels separate. Record whether a route improvement is
   sufficient or only one dependency of the next candidate.
2. **Select one measured mechanism.** Join each candidate to its captured failure
   and choose the smallest falsifiable change. Missing original-coverage metadata
   is a hypothesis, not an established cause of omissions or wrong approvals.
   A confirmed precision or representation defect remains eligible as the first
   slice. Freeze positive and negative controls for the selected hypothesis.
3. **Align interpretation with the selected evidence contract.** If the grades
   select coverage, extend the existing immutable `QuestionEvidence` snapshot,
   deriving a per-document record from certified original identity, full content
   digest, authoritative extent and supplied intervals. Pass the identical record
   to reader and audit. Partial and unknown remain distinct; invalid certified
   inputs fail. Legacy inputs without authoritative extent remain unknown.
   Caller-provided booleans, hash prefixes and a final span's end offset cannot
   establish original extent. Keep precision policy separate: coverage establishes
   no dates, units or meaning. Add an internal module only where it concentrates
   shared validation; do not introduce a parallel lifecycle. Review the concrete
   interface and acquisition/diagnostic adapters before implementation.
   Keep general semantic
   responsibilities: record type, subject, predicate, conditions, role and
   alternative records. Do not require literal wording when structural source
   meaning establishes a paraphrase, and do not infer meaning from numeric
   membership. Confirm that the selected change improves measured
   retention without new false approvals before layering further changes.
4. **Add explicit derivations only if selected by review.** Design a separate
   source-bound calculation protocol: operation, exact operand references, signed
   decimal values, units, rounding and proposed result. Deterministic evaluation
   establishes arithmetic; semantic audit still establishes operand roles and
   whether the calculation answers the question. Reject unsupported operations,
   mixed units, foreign operands and silent rounding. No expression evaluation or
   free-form model formula execution. Do not weaken literal guards for ordinary
   claims. A first slice can report original printed values while explicitly
   withholding an unavailable calculation; it must not claim calculation support
   is complete until that protocol is reviewed, implemented and tested.
5. **Qualify the candidate.** Freeze code, source inputs, prompts and scoring before
   another matched native run. Account every fixed challenge and fresh assertion;
   independently grade raw verdicts and final delivery. Preserve all failures and
   all source-readable required meanings, with no semantic retries or selective
   case reruns. App/provider caches and repeat dependence remain explicit.
6. **Complete the wider pipeline gates.** A local interpretation pass is not the
   complete-query result. Finish scalable counterevidence, whole-query all-mode
   evaluation, fresh held-out custody and actual visual UI checks. Then use the
   existing GitOps release process and verify the running immutable image before
   closing the accuracy task. Schedule restoration remains a separate operational
   decision under the governing reliability plan, eligible for reviewed GitOps
   restoration during prolonged evaluation; it does not require an accuracy pass.

## Behavior checks for the first implementation slice

Use synthetic complete and partial originals with identical visible excerpts to
prove that an excerpt cannot forge full coverage. Cover missing middle/tail spans,
overlap, reordered windows, duplicate or conflicting handles, altered content,
wrong digest, another document's certificate, whitespace-only intervals and
mutation after construction. Both readers and auditors must consume the same
snapshot; changed acquisition, requirements or originals invalidate reuse.

Native challenge pairs must distinguish document-local nonselection or absence
from an unsupported claim that an event never happened; short-year preservation
from century invention; a source-supported paraphrase from a changed predicate;
and a printed amount from an invented calculation. Keep existing source ownership,
negative-sign, unit-role, date-role and contradictory-record tests. These checks
test authority and loss mechanisms, not a copy of the implementation.

## Open design decisions

The final grades determine the first measured defect to target and the diagnostic
route. The source-context module's precise schema, legacy behavior and diagnostic
adapter remain to be independently reviewed. Calculation support requires its own
concrete protocol review; this draft is not permission to add an unbounded general
math engine or another unconstrained agent. Production model routing is not chosen
by the number of reader observations or a lower abstention count.

## First confirmed repair: numeric date spacing

Independent native review traced repeated baseline losses to a literal short date
with spaces around its slashes. The raw audit supported the observation with the
original windows; `value_mismatches` rejected its compact equivalent solely under
`dates`. A synthetic reproduction at 6d72699 confirms `source_dates` returns no
date for a spaced short year, and only the year token for a spaced full year.
Neither formatting difference changes the original event or year precision.

This deterministic repair can be reviewed and implemented while the complete
grades finish. It is not a new model candidate, a retrospective regrade, a solution
to arithmetic inference, or authorization to deploy. Native results remain frozen.

At the existing `source_dates` interface, recognize slash-separated calendar forms
with horizontal spaces/tabs, including common nonbreaking spaces, around separators.
Retain exact original offsets/text. A short-year date remains unspecified-century;
ignore only separator whitespace when comparing two valid short-year forms.
Continue respecting configured date order, invalid calendar dates, identifier
labels and numeric-token boundaries. Do not join separate lines, accept a substring
of a longer slash-number chain or convert a short year into a four-digit year.

Behavior checks: spaced versus compact two/four-digit dates in both date orders;
short-year expansion rejected; ambiguous order and impossible dates rejected;
identifier-labelled dates cannot supply calendar authority; multiline fields and
longer numeric chains cannot become dates; original offsets and quotes unchanged.
Exercise the finalizer's public value-mismatch interface with a supported synthetic
observation to verify the actual false rejection disappears, with negative controls
for a changed day and an invented century. Run the existing temporal/value suites.

The first repair is implemented locally. Its regressions reproduced 18 failing
subcases before the fix. Fifty focused date/finalizer/structural-quantity tests now
pass. Independent Spec review found and closed numeric-chain backtracking and a
prose-separator overreach before clearing the final delta. The final parser retains
named/ISO dates and numeric dates followed by slash-delimited prose, while refusing
date-shaped prefixes or suffixes of numeric and identifier chains. Independent
Standards review is also clear, including 100 additional spacing/order/year cases
with chain controls. The final backend suite passes 1,018 tests with 58 expected
skips (50.823 seconds). Commit 35f6460 passed exact-head CI 34522789737, including
disposable datastore checks and production browser contracts. No native result was
regraded and no production deployment occurred.
