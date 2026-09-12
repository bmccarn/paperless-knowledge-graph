# Source-grounded interpretation and verification

Status: the matched 96-execution source-scope diagnostic and both independent
grades are complete; both routes FAIL acceptance. Scope metadata removed tested
partial-original absence errors but lost supported positive answers. The current
authorized execution checklist is in [query reliability evaluation](query-reliability-evaluation.md#current-bounded-closure-cycle--september-12).
This document retains the existing contracts and prior implementation decisions;
it does not qualify or activate a production route.

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

## Selected source-scope prototype: concrete review contract

Three independent interface proposals compared an opaque span-handle catalog,
a new single-body/range presentation, and explicit typed support references.
Select typed references with expansion into the existing original windows. This
keeps passage and complete-original selection distinct without a second source
presentation, synthetic quotes, new I/O or a finalizer rewrite. The present source
window text, order and ownership stay unchanged. This is a diagnostic opt-in within
the inactive pipeline; ordinary production and existing diagnostic paths keep their
current protocol until a new measured candidate qualifies.

The causal hypothesis is specific and falsifiable: some native rejections explicitly
assumed the supplied original might omit sections, while some supported negatives
selected citations covering only part of their asserted record scope. Providing
validated original coverage and an explicit way to select that scope may reduce
both failures. It does not prove that a model will select or interpret it correctly.

### Pure module

Use one immutable `SourceScope` module with `bind`, `view` and `resolve` operations.
`bind` receives admitted original records (document ID, exact content, expected full
digest and authoritative extent) and existing source spans. It validates and freezes
these before any await. Production acquisition and frozen-original diagnostics are
the input adapters; neither may derive expected authority from model output. No
network client or semantic classifier belongs in this module.

`view` accepts the actual supplied spans, checks exact membership in the bound
inventory and exposes grouped original windows plus per-document supply coverage.
It must not borrow coverage from windows outside that invocation. Complete means
the Unicode-offset union covers the authoritative full extent, including whitespace.
Partial has known missing intervals. Unknown has no admitted original authority.
Missing original authority must not be manufactured from the last span end or a
chunk digest. A supplied but mismatched original, context, digest, owner or interval
is invalid and fails before dispatch, rather than degrading to unknown.

Span positions must use the existing certified chunk-to-original context. A direct
whole-original span can use its own full digest and offsets; a different chunk
digest without certified original offsets cannot gain complete-original authority.
Preserve original quotes and structural/date context. Views expose no sibling
document IDs, coverage records or reference handles to a document-local reader.

The proposed uniform wire reference shape is:

```json
{"kind":"passage","handle":"existing span_id"}
{"kind":"complete_original","handle":"offered original-scope handle"}
```

Both fields are required, with no extras. The two `kind` values form a small enum;
handles are locally checked rather than enumerating the corpus in provider schema.
A complete-original handle binds document identity, full digest, extent and exact
supplied-window inventory. It is offered only for a complete view. Guessing an
unoffered handle, using the wrong kind, selecting another view's unavailable source,
or attempting whole-original selection in partial/unknown scope fails closed.

`resolve` returns ordinary existing `span_id` references and a separate immutable
resolution receipt. A passage remains that passage even when a complete original
is available. A complete-original selection expands to all its actual original
windows, in deterministic source order; no extra source text is introduced. Retain
every raw typed selection in the receipt, while deduplicating expanded handles.
Bind the view, original identities, selected scopes and expanded reference list.
Do not pretend the model individually returned the expanded window IDs.

### Integration and failure ownership

Keep `QuestionEvidence` as the lifecycle owner. Add a constructor-only diagnostic
opt-in that binds the scope digest into its existing immutable question/source
snapshot. Ordinary callers retain the existing behavior. Changed questions,
requirements, evaluated date or sources cannot reuse that snapshot. The diagnostic
adapter must construct the scope from independently admitted originals; direct
construction with no scope retains the old protocol and grants no new authority.

The existing Strands reader and auditor adapters consume the scoped view and shared
reference schema when explicitly enabled. Each document-local reader resolves only
its own view. Preserve strict outer schemas, duplicate-key checks, observation
ownership and existing content-free protocol correction rules. A typed selection
is resolved before the existing finalizer validates ordinary source references.
Keep raw reader/auditor selections and resolution receipts separately from resolved
interpretations. Reader notes and scope receipts remain nonfactual metadata.

Audit receipts must survive in the claim's existing semantic diagnostic data, so
initial and independently re-audited subset results identify their actual selections.
The finalizer's original quote, value, date, candidate-binding and subset acceptance
rules remain unchanged. A subset audit must resolve references against the sources
actually supplied to that new invocation. No prior approved scope substitutes for
the fresh audit. There is no new persistent restoration path or live activation in
this prototype; future saved-state integration must preserve these same bindings.

Coverage metadata uses explicit complete-original wording. It never declares a
complete archive, current-world validity, accurate OCR or exhaustive interpretation.
The model must still distinguish a scoped observation that a record lacks a field
from an unsupported assertion that an event never occurred. No claim-text keyword
classifier, automatic supported verdict or semantic retry is introduced.

### Implementation tasks and acceptance

1. Independently review this concrete interface and resolve material findings.
2. Implement the pure binding/view/reference module with tests through its three
   operations: mutation, forged authority, gaps, overlap, whitespace, bad offsets,
   foreign/unknown handles, passage versus original selection and partition isolation.
3. Integrate the explicit diagnostic opt-in through existing reader and auditor
   adapters. Capture exact model request schemas, raw typed selections, resolutions,
   final ordinary references and independently re-audited subset receipts in synthetic
   end-to-end tests. Legacy paths must retain their current protocol and behavior.
4. Obtain both implementation reviews and run focused plus required full checks.
5. Before native calls, write a bounded matched diagnostic comparing unchanged
   originals, questions and fixed assertions with and without this contract. Freeze
   code, both actual SDK request forms, cases, labels, three repetitions and shared
   call/time limits. Grade raw decisions, selected scope and final references
   separately; include explicit world-absence negatives and partial-original controls.
   A new false approval or loss of a required positive rejects the scoped hypothesis.

This prototype does not implement arithmetic derivations, unit inference, reader
precision normalization, scalable whole-corpus counterevidence, or a new model route.
Those measured obligations and the full G3–G6 plan remain open. A local or scoped
diagnostic pass is not whole-query qualification.

## Prototype implementation validation

The constructor-only opt-in is implemented through `SourceScope`, `QuestionEvidence`
and the existing Strands reader/auditor adapters. Exact original identities and
actual supplied windows determine coverage; typed selections expand into existing
ordinary references. Reader receipts are replayed against each immutable local view
before admission. Canonical comparisons reject altered nested scalar types as well
as missing or foreign selections. Audits validate their actual incoming source view.

Standards review reproduced missing reader receipts and then a boolean/integer
receipt-equivalence defect; both are repaired with regressions and independently
cleared. Spec review found no remaining implementation blocker. Thirteen focused
scope/integration tests pass, including subset re-audit and failed reader correction.
The full backend suite passed 1,031 tests with 58 expected skips; the final stricter
receipt/view comparisons additionally passed the focused suite. Default reader and
verifier prompt bytes and the flat auditor request match the pre-prototype revision.

A local actual-SDK serialization exercise covered both existing model profiles,
control and scoped paths: twelve synthetic requests, zero native provider calls.
Reader and auditor schemas serialized and the controlled finalizer retained exact
original references. This is protocol validation only; it neither proves provider
schema acceptance nor satisfies the frozen native diagnostic admission gate.

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


The [matched source-scope diagnostic](source-scope-diagnostic.md) now implements the
next evaluation step. Its reviewed constructor extension certifies explicit admitted
chunk coordinates while keeping partial canonical evidence and parsing context
strictly local to supplied text. The runner and challenge set are reviewed; local SDK
preflight is complete. Exact remote admission and native grading completed; both routes failed. The bounded closure checklist above supersedes this historical next-step status.


## Selected closure repair: source labels are metadata, not source values

The unchanged captured execution 28 has a semantically supported observation
beginning `In Document 5,` and a validated reference owned by document 5. The
finalizer rejects scalar `5` because it compares every number only with source
quote text. A local replay at `720e242` reproduces `{'values': ['5']}`; the minimized
`In Document 5, payment requested.` against document 5 / `Payment requested.`
fails, while removing the source label passes. The captured trace preserves the
validated ID, so reference loss is not the cause. Two other frozen losses use
`Document 112` inside an original-local absence assertion.

The repair contract recognizes only an observation-leading `In Document N, `
attribution frame in a revalidated `ObservationCandidate`, rendered with its exact
canonical `- ` bullet. Legacy prose and direct value-check callers retain their
existing behavior: normalization alone cannot establish metadata authority.
Use canonical ASCII positive-integer spelling and an exact positive integer ID
from a validated selected reference as metadata authority. Eligibility is decided
at the plain-text observation boundary before presentation normalization;
links, code and padding are rejected there. Quoted or embedded labels do
not qualify. The separate mid-sentence `Document 112` failures remain unchanged. It must not add that number to the pool of factual source values. Mask
only the matched identifier occurrence for scalar comparison, preserving the
original candidate, offsets, references and semantic verdict. All other numeric
occurrences, quantities, dates, signs and units retain their existing checks.

Malformed/partial numeric labels, foreign IDs, boolean/string IDs, quantities,
policy/account IDs and nearby repeated numbers must not acquire this authority.
An unbound label receives no metadata exemption. Labels do not establish that the
source content prints that identifier, any statement about another record, original
completeness, or truth of the accompanying assertion. Literal quoted document
identifiers remain source claims and cannot use metadata as their evidence.

Validation must exercise actual finalizer/reference validation as well as the
minimal value-check seam: supported source attribution survives; forged reference
ownership, changed amounts and supported model verdicts with false values still
fail. Replay all frozen audited claims to enumerate every changed guard result;
separate an unchanged captured semantic verdict from any new accuracy conclusion.
No new model call is required for that deterministic comparison.

## Selected closure repair: meridiem markers are not measurement units

Captured execution 06 / fresh u3 is model-supported but rejected only for missing
unit `m`: the answer spells a time with `a.m.`, while the original uses `AM`.
The minimized checker also rejects `12:01 a.m.` against `12:01 AM.` because
`source_quantities._unit_tokens` recognizes the last letter of the abbreviation
as metres. This is a lexical defect, separate from whether the cited time has the
correct event role. A prose range separator also produces a scalar sign mismatch;
that remains unresolved because distinguishing ranges from subtraction needs its
own occurrence-level contract.

At the existing quantity tokenizer, exclude the `m` inside a complete standalone
`a.m.` or `p.m.` marker, case-insensitively. Do this equally for candidate and source
text; an abbreviation must neither demand nor supply measurement authority.
Keep real `m`, compound units, identifiers, adjacent tokens and numeric checks
unchanged. No time-of-day equivalence, timezone, event role or new date precision
is established by this lexical correction; semantic auditing remains mandatory.
Test both AM/PM spellings, true metre claims and false measurements that formerly
borrowed an abbreviation's `m`, then replay all captured guarded observations.

Both selected lexical repairs are implemented and independently reviewed. The
full backend suite passes 1,049 tests with 58 expected skips; 61 focused checks
pass. Full captured replay changes eleven of 1,881 guard results, clearing five
and preserving separate failures in six. No known false semantic approval is
identified among those newly guard-clear observations, but the existing uncertain
compound interpretation retains its status. This does not pass semantic closure.
See the [evaluation record](../audits/2026-09-09-query-evaluation.md#september-12-deterministic-repair-slice-and-remaining-acceptance-boundary).
