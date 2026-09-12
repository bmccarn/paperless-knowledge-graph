# Source-reader retention diagnostic

Status: proposed execution protocol; requires independent plan and concrete input/gold
review before implementation is admitted for native execution. This is the bounded
B2 diagnostic in [source-opportunity acquisition](source-opportunity-acquisition.md),
not whole-query qualification or factual certification. No calls are authorized by
this document alone. The failed live query and its grades remain unchanged.

## Question and scope

Does the unchanged document-local reader retain the predeclared material meanings
when it receives the complete original, including the supplied replacement qualifier
it omitted in the retained live query? Can it do so without losing conditions,
negative facts, subjects, amounts or date/action roles in development controls?

The experiment changes only source transfer within each matched pair. It does not
change the reader prompt, schema, parser, correction instructions, model, sampling
settings or output limits. It adds no semantic reviewer, selector, reread or repair
stage. A reading remains an unverified interpretation for the independent source
auditor. Successful retention cannot establish archive completeness or a true answer.

## Fixed cases and original custody

The six development cases and their exact original/capture paths are frozen in
private input and gold manifests. They cover record replacement, a late application
section, conditional hours, measured versus proposed values, posted credits, and
requested versus completed refunds. These are development inputs, not sealed
holdout data. Original documents and identifying personal facts are not published
with this specification.

Copy and hash the exact originals and retained inputs into a new private, exclusive
diagnostic package. Verify document ownership and source text against those originals
before constructing either arm. Do not copy previous reader outputs into a prompt.
The retained outputs may be bound as historical diagnostic provenance, never gold.

Retain each captured `question`, `resolved_question`, `requirements`, `evaluated_at`
and `source_date_order` exactly in both arms. In particular, preserve the original
question independently of its planner hints. The reader must not receive answers,
gold, another document, another reading, evaluator comments or conversation text that
was absent from the retained reader input. Do not rerun planning to build this pack.

## Required meanings and forbidden substitutions

These are targeted diagnostic requirements, not a revision of the unchanged
whole-query rubric or a claim that the lists exhaust each long original. The frozen
gold must associate each meaning with exact original intervals, document identity,
and whether its supporting text is present in each arm. All additional assertions
in every output are still checked against original evidence.

1. **replacement:** retain the explicitly stated replacement scope, effective date,
   requested change and premium adjustment. Do not promote replacement of a summary
   to replacement or cancellation of the underlying policy.
2. **late-application:** preserve proposed application periods, selected limits and
   worksheet amounts with their respective record roles. Do not promote proposals
   to issued, active, paid or completed coverage.
3. **conditional-hours:** preserve conditional authorization, independently recorded
   training completion and absence of recorded attendance. Permission is not work.
4. **measured-target:** preserve subject-specific measurements, proposed targets,
   sample dates and signatures; missing post-treatment evidence does not prove
   achievement or completion.
5. **posted-credit:** preserve account identity, original charge, signed credit,
   revised balance and distinct dates. Credit is not payment or settlement.
6. **refund-action:** preserve account/action associations and selected/unselected
   controls. A requested refund and a separately completed refund remain distinct;
   signature dates cannot replace transaction dates.

The private gold binds the exact dates, quantities, subjects, original intervals
and per-arm availability for each required meaning.

Requirements deliberately include explicit negative/qualification meanings that
may have been optional supporting details in a prior whole-query rubric. This
prospective diagnostic tests retention of those exact supplied meanings; it does
not retroactively change any previous grade. Source-relative absence must never
be converted to a claim that an event did not occur in the world.

## Matched arms and byte preflight

**R (retained):** use the exact captured reader payload and its original windows,
ordinals, span IDs and metadata. Do not manufacture shorter windows for controls.

**F (full):** keep the non-source payload fields identical and replace only
`source_documents` with the output of the reviewed production full-original transfer
and grouping path for that same document. Use local adapters over the frozen bytes;
no Paperless, database, graph, search or model calls are needed to build these arms.
Validate exact post-filter interval coverage of `[0, len(original)]`, complete body
and context bindings, and every emitted span's document/offset identity. Do not
substitute an ad hoc long-text prompt for the production reader input.

For short controls, R already contains the full text. Record semantic interval
equality explicitly. F can still differ in production metadata/span serialization;
do not claim the payload bytes are identical unless their hashes match. These are
controls for transfer-induced regressions, not artificial omission cases.

Before admission, measure and bind for every arm: original UTF-8 bytes and codepoints,
covered/missing intervals, number of windows, source-document bytes, serialized
prompt bytes, system/schema bytes, and complete provider request bytes under the
frozen runtime. Report duplicate context overhead separately. The large bundled
original is intentionally included; no truncation, summary, silent split or case
substitution is permitted to make it fit. Provider context capacity must be recorded
as verified or unknown, not inferred from raw byte counts. Infeasible measured
inputs block admission pending a separately reviewed decision.

## Frozen schedule, attempts and failure ownership

There are **24 logical document-reader invocations**: six cases, two arms and two
predeclared repetitions. Run sequentially in isolated reading contexts, without a
response cache:

- Repetition 1: cases 1 through 6, R then F for each case.
- Repetition 2: cases 6 through 1, F then R for each case.

Both repetitions execute regardless of whether the first looks favorable; no best
sample selection, adaptive prompts or additional seeds. Mode permutations are not
added: the document-reader payload has no mode field. This diagnostic earns no
all-mode evidence by itself.

Invoke the unchanged `read_question_sources` / `document_local_corrected` path.
Each invocation has one initial native attempt and at most the existing one
protocol-only correction if `parse_reading` rejects a nonempty returned text.
The correction uses the existing content-free protocol error and unchanged
original input; no expected fact or prior reading is supplied. An empty response,
transport failure, timeout or semantically incomplete valid reading does not earn
a new correction/retry. Never invoke correction to add the replacement qualifier.

The hard budget is **48 native attempts maximum**, **120 seconds per logical
invocation including any correction**, and **3,000 seconds aggregate active execution**.
Existing lower provider timeouts remain in force. A single inherited monotonic
deadline owns each invocation; a correction cannot restart it. No hidden SDK retries
or additional embeddings/planner/auditor calls are permitted. Unused correction
slots are not a reserve for repeat sampling. Capture every attempt, provider model
identity, input/output hash, terminal finish evidence, errors, timing and usage.

An individual invocation failure is retained and the predeclared controls continue
within the fixed budget; there is no semantic grading-driven branching during this
schedule. A shared capture/integrity failure, exhausted aggregate budget or external
cancellation stops execution, joins workers/clients and marks remaining cells not
run. Do not resume or rerun that package. Admission must explicitly authorize this
fixed diagnostic continuation policy; it differs from whole-query stop-on-failure.

## Admission, independent grading and decision

Before any call, freeze this protocol, both arm payloads, originals, required-meaning
gold and availability annotations, case order, code/prompt/schema/parser hashes,
model identity, SDK/runtime/configuration/output limits and budgets. Two independent
reviewers inspect originals first and sign exact input/gold hashes. A new exclusive
output directory and immutable manifest prevent a failed run from being overwritten.

After execution, both reviewers independently inspect all raw attempts and terminal
readings, without reading the other grade. Bind grades to the manifest, input/gold
hashes, run record and complete artifact inventory. For each case/arm/repetition
record: missing supplied required meanings, source-unsupported or misassociated
assertions, reference/ownership errors, material meanings put only in `limitations`,
protocol failures/corrections and transport/nonterminal failures. Check every extra,
not just the predeclared checklist. Preserve semantic defects in a malformed initial
attempt even if a later protocol correction succeeds.

Score missing facts separately by opportunity: a full-only meaning absent from R
is a transfer limitation, not a reader omission; a supplied meaning absent from a
valid R or F reading is a reader omission. Referenced observations must retain the
meaning and its subject/scope independently; facts only in processing limitations
do not satisfy the checklist. Do not use exact wording or observation count as gold.

The full-original arm clears this bounded retention diagnostic only if both
repetitions of all six cases preserve every supplied required meaning, introduce no
unsupported/misassociated assertions in any attempt, and complete with valid owned
references and normal transport. A permitted protocol correction is reported
explicitly and can recover structure only; it cannot erase a semantic defect. An
interpretable malformed first attempt that omits a supplied required meaning fails
semantically even if correction adds that meaning. If its semantics cannot be
assessed, record that uncertainty and do not award a clean retention pass.
The retained arm is a comparator and may reproduce the known loss; it is never
relabelled a pass because F succeeds. Disagreements block advancement until resolved
against the frozen evidence, without changing gold or obtaining new outputs.

If F still loses a supplied material meaning or any control regresses, keep B2 open
and stop advancement. Review a separate interpretation-recovery design before any
implementation or new experiment; an append-only reread remains only a candidate.
If F clears, report the limited matched result, including both repetitions and all
corrections, not general reliability. Fresh whole-query Strict/all-mode/live and
remaining held-out, matched-baseline, browser and release gates are still required.
