# Check requested omissions against original sources

Status: proposal for independent review and a bounded diagnostic experiment.
No new production implementation or activation is authorized by a passing probe.

## Reproduced failure and boundary

V2 all-mode case06 (hours history, Timeline) failed both independent reviews.
The reader and composer input contain the latest document's conditional overtime,
completed training and absent recorded attendance. Composition omits that document.
Coverage nevertheless says complete; the completion worker is never called. Counts:
one missing required aspect and one false-complete assessment, with no unsupported
raw approvals or delivered facts. The frozen run stopped after six passing cases;
41 later cases did not run. Preserve the failed result and both grade receipts.
Result SHA256: `49b47043418eef9b985175134b5b91f18ecc7df841bd900d9f5de7414e7ba675`.

The defect is observable in the data flow, not just the wording of one prompt.
`coverage_input` in `app/answer_coverage.py` validates the original snapshot but
supplies only the question, resolved question, abstract requirements and retained
answer units to the model. It cannot compare the answer with omitted source content.
The planner also narrowed the original latest-record request to scheduled hours;
the coverage stage accepted that narrower interpretation. The composer had the
missing content, so more retrieval does not address this reproduction.

A previous v1 credit case lost a known original charge with honest partial coverage.
V2's conditional completion supplied an opportunity for that class of omission, but
an inaccurate complete label bypasses it. Do not add another instruction about hours,
insurance, particular dates, document IDs or keywords. Do not change gold or rerun
the failed manifest until it passes.

## Diagnostic experiment before another application change

Test whether document-local source comparison can detect material omissions while
leaving already complete answers alone. Keep the delivered candidate fixed. The
worker sees the original user question, evaluation date, the complete retained
observations and one full original document with its immutable span identifiers.
Do not supply the narrowed resolved question, planner requirements, composer draft,
reading notes or existing complete/partial label as authority. Every source document
in each case receives a call; do not select only the known problematic document.

The exact wire shape is `{"gaps": [{"text": "missing requested meaning",
"span_ids": ["supplied local span ID"]}]}`. Require a strict JSON object with only
`gaps`; each entry has exactly `text` and `span_ids`. Text must be a nonempty
single-line string; references must be a nonempty unique list of strings belonging
to this supplied document. Reject duplicate JSON keys, extra fields, duplicate
entries, foreign references, code fences and any unparsed prefix/suffix. Only a
normally terminated, successfully parsed `{"gaps": []}` means no detected gaps.
Empty text, null, false, `{}`, invalid JSON/schema, refusal, failed/unfinished
transport, cancellation or timeout fails the diagnostic; it must never be coerced
to an empty control result. Exercise these distinctions offline before native calls. Reports are untrusted gap proposals,
not factual approvals or new answer prose. The worker must distinguish requests,
conditions, approvals, completed actions, date precision, record scope and absent
proof. It must not demand adjacent facts merely because they are present, or assume
that one document establishes the latest state of the entire archive. Other retained observations can establish that a meaning already appears in the
answer, but cannot certify their own factual accuracy or justify inferred
supersession. Follow the original question's scope: a latest-record lookup need not
repeat every historical fact, while a history question must retain material history.
Chronology alone cannot prove replacement or cancellation. Where resolving a
relationship is material to the question, unresolved original evidence must not
be silently dismissed based on the answer's assertions.

Freeze six retained development examples and one explicitly constructed scope control before native calls:

1. Failed v2 hours-history/Timeline answer, all three originals.
2. Failed v1 credit/Deep answer, its original.
3. Passing v2 initial hours-history answer, all three originals.
4. Passing v2 initial credit answer, its original.
5. Passing v2 initial calibration answer, its original.
6. Passing v2 initial capacity answer, its original (unselected alternatives and
   requested versus completed change are negative controls).
7. Constructed latest-only capacity control: ask "What is the latest documented
   authorized capacity for UNIT-Z?", supply both existing capacity-history originals,
   and retain only the previously audited newer-capacity observation plus its
   documented-state qualification. Neither document should demand the older value
   merely because it exists. Label this as a constructed control, not a new native
   answer or an alteration of the original history question/gold.

These are twelve document calls. Independently review and freeze per-document
expected missing meaning and forbidden extra demands, exact questions/candidates,
source/result hashes and accepted semantic equivalence before execution. In the
missing-hours case the expected meaning includes the conditional eight-hour
permission, completed training and absent recorded attendance; "overtime details"
alone is not sufficient. Complete controls require no material gap demands. Capture exact prompts, native responses, stop reasons,
usage, immutable input hashes and code/runtime identity. Use locked Strands 1.55.0,
existing model/destination, SDK retries zero, no Strands retries, proxy cache bypass
and no output-token cap. Bound this diagnostic to twelve calls and 1,200 active seconds;
upstream attempts remain unknown. No other native question or recovery run occurs.
Run the planned twelve calls once as one diagnostic batch so both detection and
false-gap behavior are observed. Every call must finish normally with valid local
references; retain all unsuccessful attempts. No automatic repeat, source selection
or label changes. Any failed call or semantic case rejects the whole probe.

Two independent reviewers read originals first, then every raw gap proposal. Require
both known missing-aspect cases to identify the missing meaning, zero false or
unsupported gaps, exact references and no material omissions in the claimed gap
reports. All five complete-answer controls must remain free of material-gap claims.
Paraphrases and grouped related facts are allowed; incidental wording is not gold.
A reference to an existing span is not a semantic pass. A single failure rejects
this diagnostic approach under these conditions, preserving the artifacts.
This latest-only control tests scope without establishing general supersession
competence; wider conflicting/cross-document relations still require full-query
qualification. This is a targeted development experiment, not new whole-query qualification or a
sealed holdout. No passing subset permits live retrieval or deployment.

## Application direction if the diagnostic passes

Review the concrete integration design before implementation. The intended boundary
is a source-aware completion assessment, separate from factual support:

- Compare every supplied original with the exact first audited candidate regardless
  of its existing complete/partial label. Keep document contexts separate; do not
  pool the whole corpus into one prompt, repeating the earlier context-interference
  problem. Original user intent is authoritative over a narrowed planner rewrite.
- Bind validated gap proposals to both immutable evidence and exact candidate. An
  unresolved material source gap prevents a complete coverage label deterministically.
  Gap reports never certify facts and cannot overwrite an auditor's rejection.
- Give the existing single append-only completion attempt those explicit gaps,
  including omissions hidden by the original plan. Do not require the earlier
  complete/partial model label to grant access to recovery. Preserve the unchanged
  first-audit eligibility and newer-rejection withholding rules.
- Audit the entire combined candidate against originals, then recompute coverage and
  source omissions for that exact result. Do not reuse earlier receipts. No recursive
  completion or repeated attempts until success. Unresolved omissions remain partial;
  unavailable source comparison cannot produce complete coverage.
- Specify gap identities, their relationship to planner requirements and public
  coverage, append-only mapping validation, original-scope checks, cache/history
  restoration and error/cancellation behavior before coding. Do not silently mutate
  a frozen plan or treat a model's newly named gap as authoritative user intent.

Success means required supported information survives reading, composition and
verification across domains and query modes, without false complete labels or false
factual approvals. Add meaningful cross-domain regressions, review, then freeze a new
pipeline version and rerun unchanged initial/all-mode development. The failed v2 run
cannot qualify a changed application. Live retrieval, independent holdout, actual
browser delivery and exact-head GitOps release remain outstanding.
