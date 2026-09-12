# Independent authority for omitted observations

Status: v4 integration contract cleared by both independent reviews; implementation
and concrete probe inputs still require review before native execution.
The v3 native candidate remains failed and disabled. This is not a retrospective
change to its grades or permission to resume its run.

## Reproduced problem

V3 case02 delivered all requested balance-history facts with valid source support.
The selector nevertheless labelled a repeated March balance `outside_request`.
That balance was relevant history already conveyed by another selected observation;
the frozen protocol required an explicit duplicate target. The independent review
correctly rejected the exclusion and conservation stayed partial. Both reviewers
ultimately found one raw selector contract violation. Preserve the original grades,
the standards addendum, and the failed aggregate without overwriting any artifact.

The coordination defect is that the proposal stage must both choose answer content
and classify why another observation can be left out. The review stage can only
accept or reject that classification. It cannot express the valid duplicate
relationship without an additional proposal or repair call.

## Authority and interface

Keep the existing reader, selector, one review per omission, factual audit,
completion, final conservation and delivery sequence. Add no agent, retry, vote,
source truncation, output-token limit or special domain logic.

The selector returns exhaustive rows with exactly `observation_id` and `status`.
Status is `delivered` or `omitted`. An omission is an untrusted proposal, not an
assertion of irrelevance or duplication. Selected observations retain exact text,
source ownership, stable IDs and selected order. Empty selections remain failures.

For each omitted ID, the existing independent reviewer receives the original
question, originals, full inventory, actual delivered IDs, exactly that omitted ID,
and the same bounded, labelled, untrusted conversation context. It returns one row:
`observation_id`, `decision`, `target_id`, with no extra fields.

- `outside_request`: no requested meaning requires this observation; target is null.
- `covered_by`: one explicitly named delivered observation preserves its full
  requested meaning; target is that exact selected ID.
- `reject`: exclusion is unjustified or uncertain; target is null.

Originals govern factual interpretation. A shared topic or quantity is insufficient.
Conditions, subjects, quantities, action stages and date roles must be preserved.
Outside-request classification cannot substitute for an available duplicate when
the omitted observation contains relevant requested meaning. The reviewer owns
this judgment independently; omission does not suggest which classification to use.

Only one direct selected target is admitted. No omitted targets, chains, cycles,
self-targets or implicit links. Meaning distributed over multiple selected targets
is conservatively rejected in this version, avoiding a new combination judgment.

Parser/transport failure remains unavailable. Rejection remains unresolved and
does not append an observation, restore a prior draft or trigger another review.
An approved exclusion never certifies a selected claim's truth.

## Final and saved-answer behavior

Keep occurrence-aware, exact whole-unit, source-compatible final matching. A
`covered_by` exclusion counts only when its explicit target survives final factual
audit, editing, subset selection and completion. Removed or rewritten targets cannot
leave positive conservation behind. Outside-request acceptance needs no final unit.

Version the question pipeline to v4 and conservation receipt to version 2. Store
proposal dispositions separately from reviewer classification/target. Bind every
review field to final coverage and saved-answer restoration. Old v3 receipts cannot
certify v4 output. Failed audits preserve their failure disposition and unavailable
conservation. Public UI meanings and the internal completion receipt stay unchanged.

The existing bounded workers and whole-phase/per-call deadlines remain. The call
ceiling remains one selector plus at most one review for each omitted observation,
in addition to the existing planner/reader/audit/coverage/completion stages.

## Validation and fresh qualification

Before implementation, independently review this contract and freeze controls for
unique requested omissions, valid irrelevant omissions, a full duplicate, duplicates
losing condition/subject/date role, rejected unjustified omissions, and targets later
removed or rewritten. Use existing synthetic cross-domain originals and the retained
case02 observation; do not alter original text or required requested meanings.

Offline tests must cover strict schemas, exhaustive IDs, illegal targets, immutable
inputs, source ownership, cancellation, timeout, final-target survival, receipt
tampering, HTTP/SSE and history. Preserve the existing follow-up, multipart and
derived-comparison controls. Review the complete implementation diff before freezing.

Run a separately frozen nine-call native classification probe: the eight original
exclusion challenges from the fact-conservation diagnostic plus retained v3 case02.
Strip the former proposed reason/target from inputs; freeze the correct new
classification and explicit target independently before calls. Originals and reader
text stay exact. Use the same locked model/runtime, capture, normal-termination and
once-only controls. Ceiling: nine calls and 900 active seconds. Any false authoritative
classification, transport failure or missing decision fails the whole probe.
Derive the new gold independently after removing the former proposed kind; old
accept/reject labels are not the new gold. Predeclare every valid equivalent single
target, if more than one exists, and accept any such target. A wrong outside-request
classification or an incomplete/wrong target fails even if final conservation later
withholds completeness.

A pass permits a fresh twelve-case Strict run and then a fresh forty-eight-case
all-mode run on exactly the same candidate. The prior selection proposal-kind gate
is superseded only for this new protocol: proposals no longer assert a kind. Keep
raw omission statistics diagnostic. Every false authoritative exclusion still fails,
even if runtime later avoids false completeness. Every final missing requested
meaning, unsupported fact, false audit approval or false-complete answer still fails.
Do not grade exact phrasing as semantic loss or accept parser-valid labels as proof.

The remaining live-corpus, sealed-holdout, visual UI, activation and GitOps gates are
unchanged. No deployment until all required qualification is complete.

## Inactive release wiring before freezing

Add `QUESTION_PIPELINE_ENABLED=false` to application settings and documented sample
configuration now, before freezing the candidate. A normal QueryEngine reads this
setting only when no explicit constructor override is supplied; explicit true/false
overrides remain for isolated evaluation and controlled callers. Default behavior
stays disabled. Validate false default, enabled setting, and explicit false override.
Also validate explicit true with a false setting and reject non-boolean overrides
before constructing a model client.
This permits a later reviewed GitOps environment change to activate the exact
qualified application image without an additional post-qualification code change.
Do not set the serving environment to true during implementation or evaluation.
