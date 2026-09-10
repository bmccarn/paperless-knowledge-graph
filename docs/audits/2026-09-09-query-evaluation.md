# Query reliability evaluation: G1 and G2

Spec: [evaluation before implementation](../specs/query-reliability-evaluation.md).
Starting production revision: `809046021573a5c0e9eeb65866f5e49dcfe6ae5b`.
Initial native baseline harness reviewed at `c682f58faf1e12dd61ed080f79dd20b68ab65be2`.
Latest evaluator and cache controls reviewed at `e358a0c039cbd0a94a269eb66af399edceabd1e4`.

## Status

The controlled reproduction criteria are met; representative expansion and
end-to-end qualification remain outstanding. The two G2 candidate approaches have
been implemented and tested without establishing an accepted improvement. G3–G6
remain unstarted. Production behavior, model route, output cap, indexed data,
review history and deployment remain unchanged. The updated SDK/dependency lock
is implemented and independently validated locally, not deployed.

## Completed work

- Native evaluation uses the production auditor and original-source finalizer,
  with immutable dataset/code/runtime contracts and bounded, explicitly admitted
  experiments. Captures include native nonterminal output, protocol corrections,
  interrupted cases and unknown usage. No missing case can disappear from counts.
- The grader fails on raw model false approvals even when subsequent semantic or
  source checks reject them, and also fails on missing positive observations.
- Development gold review covers 64 assertions, balanced 32/32, across operations,
  employment, billing and measurement records. It includes selected and completed
  actions, requests, existing state, date roles, table columns, conditions,
  multi-record history/latest comparisons and conflicting same-date records.
  Ancillary supported controls are not required answer aspects.
- A separate custodian holds 16 hidden assertions (8 positive/8 negative) in eight
  families, with 16 synthetic documents. Manifest SHA256:
  `8186276d41e4f9706607847b2abca0d5b517554f8e56a5da4a4c8be411396b36`.
  No candidate development or model evaluation has inspected these contents.
  Same-account separation is policy-based; representative stress extensions remain
  outstanding. Qualification consumes the set for any subsequent adaptation.
- Standards and Spec reviews passed at the reviewed harness revision. Review
  exposed and closed gaps in raw-verdict scoring, configuration binding,
  nonterminal capture, timeout denominators, dataset snapshot consistency and
  missing usage accounting.

## Validation and baseline results

`python -m unittest tests.test_eval_source_audit tests.test_eval_harness -v`:
28 tests passed at the latest reviewed evaluator revision.
At `54ee55a7`, the full offline backend suite passed 579 tests in 22.0 seconds, with 48 expected
disposable-datastore skips. These checks do not establish model or live UI accuracy. Controlled transports in capture tests measure instrumentation,
not model semantics.

`python scripts/reproduce_audit_protocol.py`: exit 1 on the production baseline.
Both required behaviors fail: an unknown source handle does not receive native
protocol correction, and otherwise-unused comparison metadata rejects a supported
historical observation. This is a deterministic protocol reproduction only.

The first native synthetic baseline experiment is frozen at the reviewed harness
revision: 64 assertions in 12 cases, three repetitions, up to 96 provider attempts,
1,800 seconds, estimated 250,000 total tokens. Configured destination/model,
installed dependencies and deadlines were captured before admission. Proxy pricing
metadata implies approximately $0.94 if the entire token estimate were charged at
the output rate; this is an estimate, not an output cap or a billed-cost guarantee.
Proxy/provider repetition independence is unverified and must not be claimed.
The baseline completed all 36 case runs (192 assertion observations) in 110.0
seconds, using 48 native invocations. It failed: six distinct supported assertions
were rejected in all three repetitions (18 false rejections); no negative assertion
was approved in these controls. Four distinct failures occurred in source/value
validation, one in temporal-facet normalization, and one in the semantic assessment.
No labels were changed after scoring. This does not reproduce the retained live
false approval and cannot advance the semantic gate.

Reported usage was 95,820 input and 105,405 output tokens. Proxy/provider cache
fields were absent. Case median was 0.115 seconds and maximum 11.95 seconds;
those fast repeats are consistent with caching and cannot establish independent
sampling or general model latency. These are reported usage totals, not verified
billing. Every input, output, failure and normalized ledger remains in private
artifacts bound to the frozen manifest.

An independent source review also labelled the retained first batch: one
unsupported assertion and three supported assertions, all high confidence. A
local-only reconstruction preserves its four atomic units and all 83 retrieved
items (206 canonical windows from 36 originals). Production chunking and evidence
identities reproduce from the captured originals. A subsequent continuity check
identified nine source items with copied table headers that are not contiguous
original passages. That captured input is classified as an invalid reconstruction; it cannot
pass evaluation. A diagnostic admission can preserve it unchanged to reproduce the
production failure, but cannot promote it to source-valid evidence. This is explicitly a
reconstruction; the original raw native request was not retained. The user subsequently approved the scoped replay. The first execution stopped
before any model call because saved ledger units already contained their rendering
prefixes. An explicitly declared saved-unit encoding now roundtrips through the
production parser without changing text or offsets; both reviews passed at
`5da5d347530b2268d2ea901f39f587c1adb8c546`. The zero-call failure is retained.

The corrected full-context replay completed three native calls in 19.9 seconds.
The unsupported unit was approved by both the model and source finalizer, while
all three supported controls were accepted. This reproduces the semantic failure.
The repeated responses were identical; the second and third native calls took
approximately 0.13 and 0.11 seconds, so these are dependent observations rather than
three independent confirmations. Reported usage: 488,550 input / 12,972 output
tokens. Invalid-source classification still prevents this reconstructed input from
passing overall qualification.

The remaining three approved calls tested the same four claims against only their
four independently identified source documents: eight items / 20 contiguous source
windows. All four units were correctly assessed, including rejection of the bad
inference, with no lost supported facts. This condition completed in 26.0 seconds
and reported 47,511 input / 25,998 output tokens. Its repeats were also dependent.
All six approved native calls are now spent. No production behavior was changed.

Removing unrelated context also changed source positions and removed nine invalid
source items, so this contrast does not identify the precise cause. In the full
manifest, the unsupported unit's source occupies positions 61–68 of 206; in the
small manifest, it occupies 11–18 of 20. The next controlled comparison must
separate these factors before choosing an implementation.

The live proxy has Redis response caching enabled with a 3,600-second TTL.
Read-only inspection of its installed asynchronous handler confirms request-level
`cache.no-cache` skips cache reads and `cache.no-store` skips writes. Evaluation
support for those controls preserves messages/schema/model/output allowance and
makes no shared-cache mutation. Upstream independence remains unproven.

Initial hypotheses, ranked from this contrast: broad context interferes with
source/field association; placement within that context changes source use; copied
header windows contribute to the failure. These are hypotheses, not a diagnosis.
A four-condition comparison is prepared: full context, removal of the nine
noncontiguous source items, identical full context with the four target documents
last, and the four-document control. Removing those items also removes their other
spans (206 windows become 183); this is not a pure copied-header formatting test.
All use the same claims and existing source scope.
The user approved that comparison, which is now complete as recorded below.

## Cache-controlled context comparison

The frozen comparison completed all 12 case runs using 12 native invocations
(including zero protocol corrections), within its 24-call / 1,800-second bounds.
Elapsed time was 250.8 seconds; case median 20.2 seconds and maximum 40.0 seconds.
All calls completed; no assertion was unavailable. Reported usage was 1,466,016
input and 63,877 output tokens (1,529,893 total). These are usage receipts, not a
billing statement. The unused correction allowance does not authorize new cases.

| Condition | Windows | False approvals / 3 negative observations | False rejections / 9 positive observations |
| --- | ---: | ---: | ---: |
| Full original order | 206 | 2 | 0 |
| Remove nine noncontiguous source items | 183 | 2 | 0 |
| Full context with target documents last | 206 | 2 | 0 |
| Four-document control | 20 | 0 | 3 |

All six false approvals survived both native normalization and the original-source
finalizer. The 183-window condition contains only contiguous original source items,
so malformed reconstructed windows are not necessary for the semantic failure.
Moving the relevant documents to the end also failed to remove it. This finite
sample supports context-sensitive interpretation as the next design target; it does
not prove that context length alone causes the errors or establish a population rate.

The three positive rejections all occurred in one four-document run. The model
approved their historical source observations with comparison scope null and
comparison check not applicable, but populated the unused comparison-document list.
The adapter rejected each solely as `semantic_comparison`. This now reproduces the
previously deterministic metadata defect in a captured native model call. The same
run correctly rejected the negative on predicate, record-role, condition and
assumption grounds; fixing irrelevant metadata must preserve those grounds.

Request-scoped proxy cache bypass was declared on every invocation. All 12 response
hashes differ, including within repeated identical inputs. Seven invocations report
upstream cached-input tokens; cache telemetry is absent on the other five. This is
not evidence of complete-response reuse, but upstream sampling independence remains
unverified. No global cache was cleared and no production configuration changed.
The two conditions containing noncontiguous items remain invalid for qualification,
even when an individual assertion score passes. No labels or inputs were changed
following the run, and no failed condition was rerolled.

The next candidate comparison should preserve the complete eligible source set and
test document-grouped evidence presentation against the current flat window format.
A separate source-first assessment remains the second ranked approach if grouping
fails. These are experiment proposals, not an accepted production architecture;
G2 must freeze their interfaces, controls and resource budgets before execution.
The four-document reduction is diagnostic only and is not a proposed production cap.

## Remaining evaluation work

- Retain the variable baseline failure pattern and source-valid 183-window
  reproducer. Reconstructed inputs are not byte-identical captures of the live
  native request; the four-document control is not a source-selection policy.
- Review source-interpretation and context-isolation hypotheses against the failed
  G2 results below before proposing another candidate. No architecture has passed.
- Add representative large-context/disjoint-window/source-order conditions and
  end-to-end question-aspect evaluation. Controlled reproduction does not complete
  representative qualification.
- Freeze fresh experiment counts, explicit retry accounting, cache observations,
  cost/time bounds and decision criteria before testing another design. Keep the
  holdout unconsumed until a candidate passes development gates.

## Source-reading candidate comparison (G2)

Candidate plan: [source reading](../specs/source-reading-experiment.md). The user
explicitly requested implementation and evaluation, with accuracy over latency.
The application now supports constructor-only `flat`, `grouped` and `source_first`
strategies at the same native audit interface; production remains `flat`. The
reader sees the original question and complete grouped sources before seeing any
candidate. Notes remain untrusted, and the verifier/finalizer retain every original
source window. Quick currently bypasses auditing and is explicitly outside this
implementation's claimed benefit until a reviewed integration change.

At SDK 1.54, the repeated source-valid retained case produced:

| Implementation | Native invocations | False approvals / 3 negatives | False rejections / 9 positives | Result |
| --- | ---: | ---: | ---: | --- |
| Flat | 3 | 0 | 0 | This small sample passed; earlier unchanged baseline runs failed |
| Grouped | 3 | 3 | 0 | Failed |
| Source-first, final compatible schema | 6 | 3 | 0 | Failed |

The final source-first run completed in 155.6 seconds, median 51.2 seconds per case,
with 919,384 reported input and 33,849 output tokens. All six native stages completed
and all three false approvals survived normalization and source finalization.
Inspection found the same unsupported action inference in a reader note before
the candidate was supplied. Candidate blindness alone therefore did not remove
the source-interpretation failure. The three arms were launched with overlapping
execution; timings are operational receipts, not an isolated latency benchmark.
Upstream sampling independence remains unverified.

Two preceding source-first manifests failed before returning any reading (three
provider 400 errors each). The first wire schema used unsupported integer enums.
After removing them, the large source-handle enum still failed. A six-call synthetic
transport probe at 183 handles/36 documents held the prompt/sources fixed: three enum
calls failed and three parser-bound calls returned valid readings. The final schema
uses integer document IDs and string handles, with exact coverage and same-document
reference ownership enforced locally. No source text, prompt or labels were changed
to rescue a semantic result; failed manifests remain retained.

On the 64-assertion four-domain development set, each strategy schedules 36 case runs
(192 assertion observations, 96 positive/96 negative). Flat used 48 native invocations
and rejected 18 supported observations; grouped used 48 and rejected 16. Neither
approved a negative in this compact set. The initial source-first wire version
used 95 native invocations, rejected 18 positives, and left four assertions unavailable
in one case. Missing required positives totalled 20. Its usage totals remain unknown
because one native invocation lacked metrics. This result is retained separately
from the final wire-schema experiment.

The final compatible source-first run completed all 36 synthetic case runs using
96 native invocations in 532.9 seconds. It falsely rejected 17 of 96 positive
observations, with zero false approvals among 96 negatives and zero unavailable
assertions. Reported usage was 160,530 input and 157,398 output tokens. The small
positive-count difference from the baseline is not a passing result or established
statistical improvement; the retained false approvals fail the critical gate.
No candidate may advance merely by improving malformed-output counts or losing
fewer facts. The final candidate was reviewed at `a589ff6`; the subsequent SDK 1.55
lock refresh has separate engineering checks and has not inherited these SDK 1.54
model results as release qualification.

The latest SDK review also identified an instrumentation limit: native invocation
counts are not proven upstream HTTP attempt counts, because Strands has a separate
default retry strategy in addition to the OpenAI client. The manifest ceilings
bounded native invocations and total elapsed time. Future strict transport-budget
claims require explicit SDK retry configuration or HTTP attempt instrumentation;
no retry behavior changed mid-experiment. See [SDK review](2026-09-09-strands-sdk-review.md).

Both tested approaches fail the current release criteria. Return to source-level
interpretation and context-isolation hypotheses before another candidate; do not
append another prompt rule or deploy this extra stage. Preserve the known adapter
false-rejection regressions for the eventual complete design. Holdout, end-to-end
all-mode coverage and live browser/original-source qualification remain unconsumed.

## Document-local candidate and bounded reference recovery

The subsequent experiment at `8632ebf` isolates each reader to one original
document. The corrected variant permits one fresh, candidate-blind same-document
call after a nonempty parser-invalid reading. It supplies a content-free protocol
marker, never the failed generated notes. Provider failures and empty readings do
not receive this correction. Auditor corrections reject foreign source handles;
evaluation counts schema-valid raw approvals across every attempt even if later
correction or deterministic validation rejects them.

Matched retained-case results (three repetitions, one negative and three positive
assertions per repetition; 35 eligible documents and 183 original windows):

| Strategy | Complete runs | Native calls | False approvals | False rejections | Unavailable assertions |
| --- | ---: | ---: | ---: | ---: | ---: |
| Broad source-first | 2/3 | 5 | 2 | 0 | 4 |
| Document-local | 2/3 | 83 | 1 | 0 | 4 |
| Document-local with reference recovery | 3/3 | 109 | 0 | 0 | 0 |

The corrected candidate passes this finite retained-case gate and preserves all
nine required positive observations. Its 109 calls comprise 105 readers, three
verifiers and one reader protocol correction. Usage was 980,486 input and 123,514
output tokens; elapsed time 252.6 seconds, median case time 81.4 seconds. This is
an audit-stage result, not retrieval, final-answer or browser acceptance. Runs
overlapped; timings are not an isolated latency benchmark. Upstream sampling
independence remains unverified. Runtime was SDK 1.54.0, OpenAI 2.54.0 and
Gemini 3.8 Flash; SDK retries were explicitly disabled in the evaluator. The SDK
1.55 dependency refresh has not inherited this live accuracy qualification.

The three broader synthetic arms each completed only nine of 36 case runs before
the proxy's request-count recycling caused connection failures. Each has 152
unavailable assertions. Broad/local/corrected arms recorded 58/112/114 native calls,
zero raw false approvals and 3/2/3 false rejections, respectively; incomplete usage
is unknown. These are failed operational runs, not comparative accuracy results.
All immutable manifests and raw receipts remain private and preserved. The proxy
exited cleanly after its 10,000-request limit with a single worker; Kubernetes
restarted it. A separate reviewed GitOps availability repair precedes fresh runs.

The table-quantity changes after this experiment address independently reproduced
lexical false rejections and boundary defects. They were not part of these model
results. Full-pipeline integration, representative qualification, untouched holdout,
all-mode delivery and actual interactive browser acceptance remain outstanding.

## Original-table development results after proxy recovery

GitOps PR #166 merged as `398ba41` and Flux applied it before execution. The proxy
removed only the count-triggered exit threshold, retained the pinned image and
single-worker configuration, returned 200 on both health endpoints and had zero
restarts throughout the recorded run. Working set rose from 1249 MiB to 1309 MiB during
qualification; this finite observation does not rule out slow memory growth.

All three fresh arms completed 36/36 case runs (96 positive and 96 negative observations
per arm) with no unavailable assertions and no raw false approvals:

| Strategy | Native calls | False rejections / missing positives | Input tokens | Output tokens | Elapsed seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Broad source-first | 96 | 10 | 161489 | 193673 | 626.0 |
| Document-local | 150 | 12 | 177746 | 172467 | 547.6 |
| Document-local with reference recovery | 150 | 11 | 178006 | 141827 | 469.3 |

These results fail the frozen acceptance gate. They do not establish superiority
on the compact set; timing overlaps and upstream independence remain unverified.
Code was frozen at `6026052` (application `9953b89`), SDK 1.54.0. No full answer pipeline,
holdout or SDK 1.55 qualification is implied.

The corrected-local 11 rejected observations comprise three signed-credit magnitude
paraphrases, three table measurements, two temporal-metadata inconsistencies and
three statement/balance associations disputed during subsequent independent review.
The table's complete original quote was present, but Markdown's table parser treats
a following pipe-free prose line as a table row. The strict structural adapter then
rejects the entire block, losing an otherwise complete numeric row. This is a
reproduced lexical false rejection, not evidence the reader misunderstood the value.
Temporal failures used `not_applicable` alongside historical/source-observation
metadata for an undated selected-form action; the adapter downgraded the inconsistent
metadata. Keep semantic contradictions separate from protocol consistency.

Both independent reviewers, given the original synthetic text and assertion without
the expected label/model output, found the statement-specific balance association
ambiguous: the balance accompanies an earlier credit adjustment; the later statement
issuance does not explicitly identify that balance as a statement field. Completed
gold and scores remain unchanged. Record this as a disputed fixture association;
any future corrected assertion must be independently reviewed and use a new dataset
version/manifest, never retroactively turn these runs into a pass.

## Reviewed follow-ups and graph release

Scope correction at `577fed4` passed both reviews and 631 offline backend tests
(48 disposable integration skips). The native original-table scores above are
unchanged; this implementation has not received a new native accuracy measurement.

The separate graph-only release in application PR #51 (`8ee3ed77`) is deployed
through homelab GitOps PR #167 (`293105c9`). Flux apps is Ready at that revision.
The frontend index digest is
`sha256:6c890f73546ddc342bdf05aec885b281c90a5d7d9691b8c80a36041f5ff9df15`.
API/worker images were unchanged. Real Chromium browser interaction against the
live 630-node/980-relationship graph preserved initial wheel zoom
(4.287878 -> 4.287878), accepted further wheel zoom to 39.403831, and preserved the
entire early-pan transform after layout completion. Explicit Fit view worked;
desktop and narrow inspector screenshots were visually inspected, with no horizontal
overflow or page errors. A subsequent uncovered 3D canvas rendered successfully
and was visually inspected; returning to 2D worked. This does not establish every
possible 3D interaction or performance characteristic. Private screenshots/measurements remain outside the repository. Native CUA
startup failed; this acceptance used actual Playwright browser control instead.
These graph receipts do not qualify query answers or all application features.


## Inactive integrated question pipeline

The implementation now runs explicit requested-aspect planning, immutable original
reading, source-led composition, initial structured-observation audit and final
coverage in all four modes when the constructor-only development flag is enabled.
The default remains disabled. Coverage describes answered aspects independently
of factual support. HTTP/SSE and saved history preserve receipts; cache admission
requires the current question/history/mode/model/corpus/date identity. Saved source
panels derive from validated references. Invalid restored receipts retain answer
text but clear positive certification panels; legitimate execution failures retain
their original states. Cache freshness checks use the bound source manifest.

At `d3c340c`, 671 backend tests passed with 48 disposable integration skips. The
production-build browser runner passed all five suites, including 19 completion UI
checks, complete/partial/unavailable coverage, saved-history reload and invalid saved
receipts. The invalid-receipt screenshot was visually inspected: saved text remains,
coverage/source checks are unavailable, and no supported-claim badges or quotes
remain. Browser artifacts are local synthetic acceptance, not native source accuracy.

The separately reviewed question-development corpus retains all original source
text while asking explicit questions across 12 synthetic cases. Its rubric is
independent of the prior fixed-assertion benchmark; the old failures remain failures.
The runner freezes code, source data, runtime and prior review identities. It runs
one case per invocation and cannot continue without both independent reviews of the
exact prior result. This isolates post-retrieval answer production; live retrieval,
all-mode native stress and sealed holdout remain later gates.

An isolated temporary environment in the API pod installed the hash-locked SDK
1.55.0 dependencies successfully, alongside OpenAI 2.54.0, HTTPX 0.28.1 and Pydantic
2.13.5. The serving application remains on its prior runtime. The captured route is
Gemini 3.8 Flash via the existing proxy, with 90-second native call and 120-second
audit deadlines, concurrency four. No output-token cap is introduced. At this record,
preparation made no native model calls and the new pipeline is not quality-qualified.

### First question-level native experiment: stopped on frozen rubric failure

Candidate code `3c788a9`, Strands 1.55.0, original question-development dataset:

| Case | Calls | Seconds | Tokens | Independent grade |
| --- | ---: | ---: | ---: | --- |
| Capacity | 5 | 13.925 | 7610 | Both pass; zero false approvals or missing aspects |
| Handover | 5 | 27.696 | 8945 | Both pass; zero false approvals or missing aspects |
| Hours | 5 | 16.790 | 8631 | Both fail; one omitted required component |

All 15 calls terminated normally; usage was available for every call. There were
zero raw or delivered false approvals. The hours answer omitted the blank approval
field. Both graders counted one false-complete coverage receipt under the frozen
rubric. The source reader had identified that field; composition omitted it. Both
reviewers also noted that the question did not request approval status, making this
a rubric/question alignment concern rather than evidence that the application
should emit every incidental source detail. No remaining cases ran. Preserve this
experiment as failed; do not retroactively change its grade.

A separate revision of the development evaluation is proposed for review. The hours
question explicitly asks about approval, and all required-aspect components must be
checked for alignment with their questions before a fresh run. No application code
change follows from this finding. Full backend/frontend CI passed at `11d3e85`;
the merge from main resolved only the graph spec's stale status.

### Question-level native revision 2: initial slice passed

Both independent reviewers accepted the pre-run question/rubric alignment at
`3345a38`. Original source text is unchanged. The fresh manifest is
`2b5fe15ab876780795f7f53517e914369dc07d60fdaa6dc3678e2298e68748d8`.
The application and harness are unchanged from the previous experiment; this is
a separately frozen development evaluation, not a relabeling of R1.

| Case | Calls | Seconds | Tokens | Independent grade |
| --- | ---: | ---: | ---: | --- |
| Capacity history | 7 | 17.232 | 12609 | Both pass |
| Hours history | 7 | 37.374 | 18886 | Both pass |
| Balance history | 9 | 34.916 | 24173 | Both pass |
| Treatment history | 7 | 33.747 | 19416 | Both pass; coverage under-reports two aspects |
| Credit | 5 | 100.784 | 37882 | Both pass; coverage under-reports one aspect |
| Refund | 5 | 14.989 | 8937 | Both pass |
| Leave | 5 | 21.267 | 9128 | Both pass |
| Sampling | 7 | 35.907 | 22198 | Both pass; one temporal classification repair |
| Calibration | 5 | 16.323 | 10750 | Both pass |
| Capacity | 5 | 16.233 | 7791 | Both pass |
| Handover | 5 | 14.925 | 7640 | Both pass |
| Hours | 5 | 14.951 | 8094 | Both pass |

Across all twelve reviewed cases: 72 native calls, 187504 reported tokens, no raw
or delivered false approvals, no missing required aspects and no false-complete
coverage receipts. Treatment and credit deliver the required supported facts but
conservatively label three aspects partial. This is a measured coverage limitation,
not permission to force a complete label. The credit composer took 84.21 seconds
within its 90-second deadline; all calls terminated normally.

The sampling auditor initially labelled a source-scoped absence statement as
present-world, causing a repair and re-audit. Both drafts remain source-supported;
the classification error is recorded separately from factual false approvals.

Both reviewers passed every case. Active native elapsed time was 358.649 seconds.
This passes only the initial Strict post-retrieval development slice. All-mode native testing, live retrieval and sealed
holdout remain outstanding. No production query activation follows from these
partial development results. Private raw artifacts are retained under
`/private/tmp/kg-question-native-r2-20260909`.

### All-mode harness extension

The next development stage schedules each of the same 12 cases in Quick, Deep,
Timeline and Strict, with fresh per-case independent reviews and a separately
frozen budget. Initial admission binds every raw artifact from the passing slice;
new results bind their own raw input/output hash inventory. Scheduled request
identities prevent mode relabeling. Timeline requires production projection
restoration plus independent date-role/source grading. Both independent reviewers cleared these controls at harness SHA256
`90bb0592903d1920a91a48eebd6744d3259d86587c3620fc2d75366fd6b6573d`.
This review preceded the frozen native execution reported below.

Eleven harness admission tests pass, including removal/alteration of raw files,
failed initial review, different candidate/runtime/policies, initial budget excess,
valid unavailable/coarse coverage, missing/swapped mode and invalid Timeline
projection. The final focused harness, pipeline and Timeline check passed 47 tests.

The all-mode experiment is frozen at commit `40ee9d3`, manifest
`fba25f34992acae5fc4f2c48446b6a46d20652e754f0c001cab89880081dd545`.
The first twelve case-runs passed both reviewers: capacity-history, hours-history
and balance-history in all four modes. Timeline includes four, five and five
independently checked date projections respectively. Balance history preserves both
conflicting issuer balances without selecting a winner. These twelve cases used
92 native calls, 327.305 active seconds and 209,104 reported tokens. All raw/delivered
false approvals, missing required aspects and false-complete counts were zero.
Backend and frontend CI passed at that commit.

The run subsequently stopped at case17 (credit, Deep). Cases00–16 passed both
independent reviews, including treatment history in all modes and credit in Quick.
Case17 omitted the required original charge even though the original and reader
output supplied it to composition. Both reviewers counted one missing required
aspect, zero raw/delivered false approvals, zero unsupported extras and zero false
completeness. Its coverage receipt honestly reported partial. Thirty later cases
were not started. This is a failed candidate, not an incomplete passing run; it
cannot qualify live retrieval or be resumed after application changes.
Result SHA256: `6ec6b442326cd497d3915c6ed920d42c907df1c9877c47f07745f474349040df`.
The proposed general completion recovery is specified separately in
`docs/specs/question-coverage-recovery.md`; its effectiveness is unmeasured.
The implementation adds one append-only source-grounded completion attempt for an
unchanged first-audit-supported answer with named coverage gaps. Combined facts
receive a fresh complete audit and coverage check; unsupported additions cannot
be delivered, and newer rejection of a retained fact withholds the answer. Review
reproduced a multi-batch timeout that lost an already returned negative verdict;
the completion auditor now records completed negative batches before siblings can
fail. Recovery failure retains content-free before/combined hashes. The full offline
backend check passed 713 tests (48 opt-in integration skips), including nine new
completion tests across four modes and two domains. These are controlled contract
tests, not native accuracy evidence. A fresh candidate manifest and native run are
required; no result from the stopped v1 experiment qualifies v2.

The recovery implementation passed both independent review axes at `e486535` and
backend/frontend CI, including disposable datastore checks and production browser
regressions. Its fresh initial native experiment uses unchanged R2 questions and
rubrics, manifest `7a4737fbd89a0904a9c4a1003aea3677120a598ec32004c4d05c02758a544f17`.
Cases00–03 passed both reviewers with zero false approvals, missing required facts,
unsupported extras or false completeness. Treatment coverage conservatively
under-reported one aspect; its completion worker proposed no additions. These
results do not yet demonstrate a native recovery of an omitted fact. The initial
slice, all-mode run, live retrieval and holdout are not complete.

Retained v2 complete and partial answers were replayed in the built frontend.
Desktop/mobile coverage, factual checks, source drawers and private history replay
passed and screenshots were visually inspected with no page errors or horizontal
overflow. The initial complete replay omitted the public source-summary projection,
so its source-check locator timed out; adding the production formatter's projection
fixed this fixture failure without a new native call or application change. These
remain rendering checks, with replay timing and absent fixed-pack summary metrics,
not live retrieval or real conversation persistence. Artifacts are private under
`/private/tmp/kg-recovery-v2-ui-20260909`.

Native computer control was retried and still failed native-pipe startup. A local
Playwright fallback replayed retained native complete, conservative-partial and
Timeline answers through the built frontend. Source excerpts, separate coverage
and factual checks, saved-history replay and narrow layouts worked without page
errors or horizontal overflow. Screenshots of actual native answers and source
panels were inspected. This isolates rendering: the replay uses supplied originals
and an in-memory history adapter, not live retrieval or database persistence.
Its fixed-originals input lacks production pack summary metrics, and displayed
latency measures only replay. Zero summary/replay values do not qualify production
measurements.

Two replay setup failures were corrected: the conversation-list query string was
initially unmatched, and Timeline had two legitimate source controls with the same
label requiring explicit selectors. Neither established an application defect.
Private screenshots and results: `/private/tmp/kg-native-answer-ui-20260909`.


## Live retrieval preparation (not a candidate result)

A read-only Paperless inventory found 929 eligible documents. Live API freshness
reported the same 925 indexed IDs across graph, embeddings and completion records,
with four newer unindexed documents and no extra IDs. No ingestion, drift repair,
reindex or schema initialization was triggered. Equal processed counts are not a
claim that all 929 originals are indexed or that the archive is complete.

Private rubric preparation captured 128 original documents, matching each selected
inventory OCR hash and modification timestamp. These include insurance records from
2022 onward, Starlink service history and a pair of utility bills. Candidate prompts
will not receive the rubric or a preloaded source allowlist. A six-request live
retrieval plan is drafted in `docs/specs/question-live-retrieval-evaluation.md`;
its design passed both reviews. Isolated admission, model/stage capture and reader
utilities have focused offline checks and independent slice reviews; full runtime
and browser wiring remain incomplete. All-mode admission failed and is still required
before native live-retrieval execution. Retained source copies establish preparation
evidence only, not an immutable whole-corpus snapshot or a passing model result.

The delivery utility additionally guards the actual engine query invoked by its
real stream implementation, uses a private conversation antecedent, and checks
exact final SSE conservation. Review reproduced acceptance of an unterminated SSE
frame and bytes whose send failed; both now reject qualification while retaining
diagnostic bytes. Both axes cleared the slice; five actual-route tests pass and all
29 live utility tests pass. This still does not supply an executable live harness.

The fresh v2 initial cases00–08 now pass both independent reviews, with zero raw or
delivered false approvals, missing required facts, unsupported extras or false
completeness. Credit conservatively under-reported one aspect, as did treatment;
both completion calls returned no additions. Native omission recovery remains
unproved. The latest helper-only commit `ee80a7a` passed backend/frontend CI.

Live-browser session ownership and no-model/private-history adapters passed both
review axes. Six additional actual-route tests cover normal delivery, disconnected
requests, owner cancellation, explicit model selection, frozen follow-up history
and saved metadata projection; all 35 live utility tests pass. The built frontend
then submitted one retained-native-output replay through the actual HTTP/SSE route,
with exact model/request and final payload conservation. Desktop/mobile answers,
source drawer and saved private history were visually inspected; no page errors or
horizontal overflow occurred. Artifacts: `/private/tmp/kg-live-browser-session-ui-20260909`.
The first UI fixture used Uvicorn's signal re-raise and lost its teardown receipt;
the fixture now owns SIGTERM shutdown, and a second no-model rendering run retained
the successful conservation receipt. This is test-server wiring, not a production
change or new native retrieval result. Full admitted live runtime integration is
still pending; these helpers do not activate the candidate.

The fresh v2 initial gate is now complete: all twelve Strict cases passed both
independent reviewers, with zero raw/delivered false approvals, missing required
facts, unsupported extras or false completeness. Conservative under-reporting was
one aspect each for treatment and credit. The run used 72 native calls, 298.050
active seconds and 155,384 reported tokens. Neither completion attempt added facts;
a native recovery of a known omission is still unproved. The separate 48-case run
is frozen as `523ace74cbe68d923e2ec7f8787945fd245bf433cf65ec79aba6ea7027d79bb8`,
binding all 192 initial result/review/attempt artifacts. It has not passed yet.

The private live-manifest builder passed both review axes after correcting a
reproduced aliasing defect: caller mutations could previously change an already
built configuration/corpus snapshot and conceal drift. Strict JSON snapshots now
preserve the earlier bytes, and five regressions pass. Live input preparation
retains the previously reviewed six questions/history and 128 original documents,
with explicit `gemini-3.8-flash` selection. This is preparation, not fresh corpus
admission or an executable live harness. Browser-session helper commit `2d156c3`
passed backend/frontend CI.

Fresh v2 all-mode cases00–05 passed both independent reviews: capacity history in
all four modes, then hours history in Quick and Deep. All error counts and coverage
under-reporting are zero in these six results. Case06 has executed and awaits
review; the rest have not run. This is not a passing all-mode qualification yet.

Corpus and loopback-server helpers passed both review axes. Corpus checks preserve
exact source/index IDs, shared generation, processing fingerprints, stale/missing
records and open feedback, without repairing state. Failed concurrent reads join
siblings before reader teardown. Server review reproduced double cancellation
that interrupted ASGI cleanup and a listener-close ordering deadlock; both are
corrected with regressions using actual Uvicorn shutdown and Python 3.12 Server.
A real loopback HTTP forced-shutdown check completed asynchronous cleanup with one
cancellation and a closed listener; no model calls were made. All 52 live utility
tests pass. Full live runtime admission/browser orchestration remains incomplete.

V2 all-mode qualification stopped at case06 (hours history, Timeline): both
reviewers found one missing required latest-record aspect and one false-complete
coverage assessment; all raw/delivered factual approvals remained supported.
The source reader and composer input contained the omitted information. Coverage
received no original content, accepted a narrowed plan and declared complete, so
conditional recovery did not run. Cases00–05 passed; 41 later cases were not run.
The failed result and exclusive failure aggregate are retained under
`/private/tmp/kg-question-recovery-v2-all-modes-20260909/case-06`. This candidate
does not qualify live retrieval or deployment. The source-aware comparison proposal
in `docs/specs/question-source-coverage.md` describes a twelve-call diagnostic with
retained failures and complete-answer controls before another application change.


The revised seven-case/twelve-document source-coverage diagnostic and its concrete
input/gold pack passed both independent reviews. Input SHA256:
`48c8bc35fa77395a09e93b4cbab3aae8a9904c6e2123c54bf47bd60efe3f2223`.
The harness (`scripts/eval_source_coverage.py`) also passed both reviews after
closing input replacement between admission/execution and missing second-reviewer
identity. All 15 focused parser, execution, capture and locked SDK transport tests
pass offline. Refused, truncated, unavailable and invalid responses cannot count as
empty gap assessments; failed calls remain failures while later controls are still
measured. Gold and previous coverage/planner labels never enter model input.

No diagnostic model calls have run. Automatic approval review rejected staging
repository code and synthetic fixtures in the existing KG pod's new temporary
directory, including after verifying every original exactly matches the repository's
synthetic-only dataset. Explicit permission for that transfer is pending. Artifacts
and the staged command are local at `/private/tmp/kg-source-coverage-20260909` and
`/private/tmp/prepare-kg-source-coverage-runtime.py`. No production mutation,
publication or deployment occurred. A passing probe would permit the next reviewed
integration design, not production qualification or activation.

After explicit user approval, staging and the once-only diagnostic completed.
All twelve calls terminated normally in 16.698540 seconds, with 10,292 reported
tokens. Frozen manifest SHA256:
`6dedcbe73bf415c889468b419a6fa9be1b9a3156bd299e0b90691c0536040477`;
result SHA256:
`2447f8e319eb075147a1e31c1ac9b8d0badbbeba5f27601113a95e9d631f48a0`.
The missing-charge case was detected. The missing-latest-record case returned
`gaps: []`, losing three required meanings in one requested aspect. All five
complete controls returned no gaps. This fails the frozen diagnostic; the source
checker is not justified for application integration. No retry was performed.

The retained original reader had already extracted each lost latest-record fact.
A new reader is therefore not the next experiment. The local proposal
`docs/specs/question-fact-conservation.md` instead makes source-observation
selection/exclusion explicit and tests false irrelevance and incomplete duplicate
judgments separately from deterministic ID accounting. It remains unimplemented
and unqualified. The approved draft PR #50 status update was published; no query
activation or GitOps deployment occurred.


The retained-fact selection/exclusion diagnostic passed both independent reviews:
seven selection cases and eight fixed exclusion challenges, fifteen normally
completed model calls in 40.388774 seconds with 25,037 reported tokens. All
selection losses, unnecessary retained material, false exclusion approvals,
false exclusion rejections and transport failures were zero. The source inventories
retained exact reader observations and original source identities; one deliberately
unrequested delivery/receipt observation was correctly excluded. The original
missing-hours and missing-charge meanings survived, and incomplete duplicates
losing condition, subject or date role were rejected.

Input SHA256: `1917a14d90acafd06e18d666fa1ed0c490e34bd2040e6cc245d750d6dd892570`.
Manifest SHA256: `f99c4fd630eea95e8ba9e3da4e2acc0be8e2ec3a8c78d71ccab8e6645ca82420`.
Result SHA256: `552f8904a3b637473d8f3c353e2777b7a887e74b4dbf2111e5395b1997dfbb51`.
Private captures/grades: `/private/tmp/kg-fact-conservation-20260909/results`.
The conditional alternative-model arm was not triggered and remains unrun.

This qualifies only the focused disposition diagnostic, not factual audit, reader
recall, full query accuracy or deployment. The next concrete integration proposal
is `docs/specs/question-fact-conservation-integration.md`, pending independent
review before application implementation. The reviewed harness/protocol and
21 focused tests are saved in local commit `07a4cbc`; production is unchanged.


The concrete v3 integration design passed both independent reviews and is now
implemented locally with qualification still pending. Initial free-form composition
is replaced by exhaustive reader-observation selection, with independent reviews
for exclusions. Final source-supported units are matched back to individual retained
observations; public completeness and saved-answer restoration include this receipt.
Conversation context is a bounded, untrusted selector/exclusion hint. The original
source reader remains candidate-blind, including in the existing audit strategies.

Frontend lint, type checking and all 12 data/format regressions passed. The normal
Turbopack build was blocked by a local compiler-worker socket permission error;
the supported webpack production build passed without changing production build
configuration. The normal build remains a CI requirement. All browser suites passed:
21 completion cases plus accuracy, source inspection, graph zoom and hub contracts.
Screenshots of the two new coverage states and mobile graph inspector were visually
inspected. Private browser artifacts: `/private/tmp/kg-fact-conservation-ui-20260909`.
These are synthetic delivery/UI checks, not live-query accuracy qualification.

The full offline backend suite passed 772 tests in 24.735 seconds, with 48 expected
disposable-datastore skips. All 60 app/script Python files parsed. The initial test
run exposed and fixed two implementation defects: forwarding prior conversation
text into candidate-blind audit readers, and relabelling a source-audit timeout when
binding conservation. A direct-snapshot oversized-context review finding was also
fixed with a before-dispatch regression. Saved timeout failures retain their original
disposition and unavailable conservation cannot certify a saved answer.

Both final integration reviews are clear. The additional standalone multipart /
narrowed-plan regression passed after the full-suite run; the final focused slice
passed 34 tests. The reviewed fact module SHA256 is
`d6f868f9abb003ba903fd91c4d919efb7ac65d5b3c57f0683717299dd265e1f7`.
Fresh v3 native qualification is next; no previous candidate's approval is reused.


The frozen v3 candidate `17139a7` passed full CI run `34425866468`, including the
normal production frontend build/browser suites and disposable datastore checks.
Its fresh native initial run passed cases00 and01 under both reviews, then stopped
at case02 (balance history). All five delivered observations and every requested
meaning were supported, but the selector classified relevant repeated March balance
information as outside-request instead of naming its existing duplicate target.
The independent exclusion reviewer rejected that proposal and public coverage stayed
partial. This is one raw selector contract violation, zero false audit/exclusion
approvals, zero missing required meanings and zero false completeness. The original
standards PASS is preserved alongside an addendum resolving the contract as FAIL;
no artifact was overwritten. Cases03-11 and the new all-mode run remain unstarted.

V3 initial manifest SHA256:
`ee033525a3a484c53b1466d6c34574988c875ace52ca9dd4b6ac4d2554696c91`.
Failed result SHA256:
`b86065eeb3c6237bbd66fe441af5754df49480efcdf852ca67639e8da67f0048`.
The three cases used 24 native calls, 127.995 active seconds and 70,339 reported
tokens. Private artifacts: `/private/tmp/kg-question-fact-v3-20260909`.
The next proposed authority separation is documented in
`docs/specs/question-exclusion-authority.md`. It is not yet implemented or qualified.
The pipeline remains disabled; no deployment occurred.


The v4 authority-separation plan passed both reviews before implementation. The
selector now proposes delivered/omitted observations without classifying exclusions.
The existing independent review owns outside-request / explicit-single-target
covered-by / reject decisions. Receipt version 2 and question-evidence-v4 bind those
classifications and targets; final source-compatible target survival is still required.
No model stage, retry or output cap was added. The live admission helper now also
requires exact integer zero false exclusion approvals in both grades and aggregate.

V4 focused integration passed 63 tests. The completed full offline suite passed
787 tests in 26.459 seconds, with 48 expected datastore skips; all 61 app/script files
parsed. The new diagnostic has 12 offline tests, including locked Strands/OpenAI
mock transport, complete captures, truncated EOF, continued fixed controls and client
cleanup. The nine concrete cross-domain inputs preserve old originals/reader notes
and add the retained v3 balance case; input SHA256 is
`71feb03629da7d5cb3d4a645e008c6a115fab6facdcdc569cf57c0a75c43c2c3`.
Independent implementation and concrete input review must complete before native
calls. No v4 native call, activation or deployment has occurred.

Both independent implementation/harness reviews and both concrete input/gold reviews
are clear. Before freezing, the reviewed disabled-by-default
`QUESTION_PIPELINE_ENABLED` setting was added so GitOps can activate the exact
qualified code later. Explicit constructor booleans override the setting; invalid
values fail before client construction. Configuration controls were red before the
change and green afterward. The final full suite passed 789 tests in 24.907 seconds,
with 48 expected datastore skips. Serving configuration is unchanged.


V4 candidate `e90ce0e` passed full CI run `34427811841`. The frozen nine-call
Gemini authority probe failed: eight correct decisions and one conservative false
rejection of a full balance-history duplicate; zero false exclusion approvals,
wrong targets or transport failures. Both independent grades are FAIL. The full
requested March balance meaning survives in the selected March transaction target.
Primary manifest SHA256:
`699e1dafe263cc84f9e3bce8f11ba0804db46ec6c065348ad5b344f5432cbd01`;
result SHA256:
`56a87799e8ab6bff3f7248366f064097d041e67fa6c34fb081931f426c26ff6d`.
Nine native calls took 25.747 active seconds and reported 18,065 tokens.

A separately frozen matched GPT-5.5 comparison changed only the model route and
produced the same nine classifications: also FAIL, one false rejection and no
false approval, wrong target or transport failure. No model superiority was
measured. Comparison manifest SHA256:
`8bf7324f987cd7bfc8cf7782eb1a64a88facc4befe4811f4d2a32157b7a4d852`;
result SHA256:
`faba70380b0395bba57f7c074dd5623eb7f546a4aec598da0d110c36e138b838`.
Nine calls took 29.065 active seconds and reported 12,382 tokens. Both reviewers
verified identical inputs/code and complete captures; both failed results and
all grades remain preserved. No further model or prompt arm is authorized by
that comparison contract.

Both reviewers accepted the separately documented prospective post-hoc development
admission in `docs/specs/question-conservative-coverage-admission.md`. This relaxes
only the internal zero-false-rejection prerequisite for fresh whole-query
measurement, not the meaning/accuracy gates or release criteria. Its policy and
failed-probe evidence must be immutably bound before execution. Conservative
rejection counts and exact surviving targets must be independently graded.
The existing Gemini route remains selected. No v4 whole-query run, activation or
deployment has occurred; baseline improvement remains unproven.


The development exception is frozen as private receipt SHA256
`a00d96d4446928bfec5f7df3a7026bdf84da52f1293d4fc11318fe5b0bdc2d19`,
binding 113 artifacts, both failed probes and two independent policy approvals.
The new admission loader checks the raw capture chains, sole known duplicate
rejection, unchanged application/lock/runtime and exact policy. Initial/all-mode
manifests retain the exception; continuation binds individual grades, scheduled
identity, aggregate zero-error counts, coverage-underreporting counts and exact
supported surviving duplicate targets. Live admission threads the same receipt.
Both final reviews are clear. Review caught and closed the claim-key mismatch,
contradictory aggregate-grade admission and skipped Strict schedule validation.
The full offline suite passed 802 tests in 25.096 seconds with 48 expected
opt-in datastore skips; 38 focused checks passed and all 62 app/script files parsed.
Application and prompt files remain identical to the failed v4 probe candidate.


Fresh v4 development run (published harness `36ca251`) stopped after case01 FAIL.
Case00 capacity-history passed both independent reviews, with all counts zero.
Case01 hours-history omitted one required aspect: conditional overtime, completed
training and absence of recorded attendance. Original-source reader attempt003
captured it in two observations; selector004 omitted both; exclusion reviews005/006
correctly rejected both omissions. Candidate construction still used only selected
rows. Coverage008 accepted narrowed requirements and did not trigger completion.
Public conservation correctly stayed partial, but required meaning was missing.
This is ineligible for the duplicate exception. Both grades and aggregate are FAIL;
cases02-11 and the v4 all-mode run are unstarted and must not resume.

Manifest canonical SHA256:
`6db1b0e79a276a7808703dc9fd531e5f67db24119065ce02489764dc2e1607b8`;
package SHA256:
`e1f9688bdf008364f7f44cd9da05ce50b63cf039fee1c367127f53eddba679af`;
failed case01 result SHA256:
`6cfb72f9442b7e012e8d8e4acda7ac8dec7d97fb5f0eca80b0aa1624f8d4e897`.
The two cases used 17 native calls, 47.291 active seconds and 30,497 reported tokens.
One missing required aspect, zero factual/exclusion false approvals, unsupported
extras, public false completeness or conservative duplicate rejections were found.
The new proposed correction is documented in
`docs/specs/question-reviewed-retention.md`: a rejected omission must affect the
candidate before factual audit, not only the post-delivery coverage label.
No activation or deployment occurred.


The reviewed v5 correction is implemented: valid rejected omissions join the exact
candidate before the existing factual audit. One retained-ID rule also drives final
conservation. Original proposals/reviews remain intact; approved exclusions still
require their original selected target. Empty retained sets are explicit unverified
failures with zero trust in every mode. Receipt version3 and question-evidence-v5
prevent old saved judgments from certifying the new behavior. Both model prompts
are unchanged; no extra stage/pass/retry was added, though more retained units can
increase existing audit batches.

The public narrowed-planner regression fails against frozen v4 in all four modes
and passes v5: both wrongly omitted units now reach the auditor and final answer.
Focused controls cover two real audit batches for six retained units, zero dispatch
and no truncation at 81 units, source-unsupported recovery followed by editor/fresh
audit, source substitution, rewritten/removed units, original duplicate targets,
all-omitted recovery, zero-retained saved failures, cancellation and tampering.
Both final reviews are clear. The full offline suite passed 811 tests in 25.179
seconds with 48 expected datastore skips; the independent app review ran 53 focused
tests and the v5 harness slice passed 39. All 62 app/script files parsed.

Fresh manifests now bind the v5 contract and grading version2; complete raw/dual
review bindings, aggregate zero-error/coverage agreement and scheduled identities
apply without the v4 exception. That old receipt cannot admit changed application
code. No v5 native execution, activation or deployment has occurred yet.


Fresh v5 candidate `8f23e80` failed initial case00; cases01-11 and all-mode are
unstarted. Reader observations captured granted approval separately from the signed
selection. Exclusion attempt005 wrongly approved outside-request for approval,
leaving an answer about a selected increase without its approved status. Both
reviewers record one missing required aspect, one false authoritative exclusion
approval and one false-complete final coverage judgment. The delivered facts were
supported, but incomplete. V5 therefore does not qualify, despite its successful
controlled repair of correctly rejected omissions. Both failed grades and aggregate
are preserved; no subsequent case or deployment follows this failure.

Manifest canonical SHA256:
`65aeff8b74b0ddf7239296eab70cc52bc5b244d0cb16709f7e08e6c73f1ece32`;
package SHA256:
`deff9c9fb58aaad93b9bc12237c00ba947529564d0c8cb863b8443ad9088ceb3`;
failed result SHA256:
`2a44845bc9ff849c3a53979ce845c31925552d6f6e4224e39df8fd9dacb310b3`.
Nine native calls took 27.337 active seconds and reported 17,175 tokens.
This identifies the independent relevance/exclusion decision as an unsafe deletion
authority, not merely a missing reaction to rejected omissions. The next proposed
contract, `docs/specs/question-reader-inventory.md`, removes runtime selection and
exclusion calls and source-audits the complete question-led reader inventory.
Original-based coverage, whole-query qualification and release gates remain intact.


The v6 full-reader-inventory contract passed both independent design reviews before
implementation. Runtime selection and exclusion are removed; the source reader now
explicitly produces standalone source-scoped observations, with material negative
facts and conditions in referenced observations. Supported incidental details and
repeated occurrences can increase verbosity. Original-source factual and requested-
meaning qualification remain mandatory; no earlier failure is rescored.

The harness independently passed 26 focused controls. It denies retired filter
stages before model dispatch and requires matching input/output stage identities,
zero filter calls and dual result-bound grades. Shared live capture provides an
explicit reader-inventory guard while retaining historical classifier diagnostics.
All 21 capture/diagnostic controls passed. Full CI on the previous v5 commit passed
(run `34431166807`), but does not qualify that failed candidate's accuracy.

Initial v6 full-suite execution ran 810 tests with 48 expected skips and three
failures: two shared-capture historical diagnostic incompatibilities (fixed) and a
follow-up coverage fixture under correction. Independent application review also
reproduced a saved-inventory deletion that could falsely upgrade partial coverage;
that restoration defect is being repaired before freezing any native v6 run.
No native v6 execution, activation or deployment has occurred.


Final v6 implementation reviews are clear. The immutable prepared inventory now
anchors reader_inventory_digest outside the mutable conservation receipt. Removing
the last row or a whole document and recomputing receipt/public coverage digests
cannot upgrade partial coverage; missing, wrong and boolean anchors also fail.
The same prepared anchor is reapplied after completion without changing the base
coverage binding contract. This is integrity binding under the existing saved-object
trust model, not a claim of cryptographic authenticity for rewritten whole objects.

The final full offline suite passed 812 tests in 25.098 seconds with 48 expected
datastore skips. Independent Standards ran 67 focused app tests, the implementation
slice ran 58, and the root completion/harness slice ran 38; all passed. All 62 app
and script files parsed, and whitespace checks passed. Controls include distinct
source-owned occurrences, substituted/rewritten/removed facts, source-resolved versus
ambiguous references, real six-unit 4+2 audit batching, 81-unit failure without
truncation, and timeout/cancellation. Frozen v5 loses granted approval in all four
controlled modes; v6 retains and audits it. The earlier hours repair remains intact
while its model selection/exclusion calls are removed. Fresh native qualification,
actual-corpus/browser work and release gates are still pending.


V6 candidate `f207cc8` passed exact-revision CI (`34433188788`). Its fresh Strict
manifest canonical SHA256 is
`3e7989373682ac80f132ac5ad11c78e709fbb17e023ca2cbd4662135e53e76f0`;
package SHA256 is
`a56416e583104a71e36c42a99076b995327a2baf67a4a606ac134f6c2ffa4a35`.
Cases00-05 have now passed both independent original-source grades, including
capacity approval, conditional hours, conflicting latest balances, treatment/sample
roles, posted credit and distinct refund action stages. Each case has zero raw or
delivered false approvals, missing required aspects, false completeness, unsupported
extras and false exclusion approvals; no retired filter calls occurred. Three
aspects were conservatively underreported. Some exact-text conservation remains
partial after editing even though all requested meaning survived. In the hours case,
a source-label number triggered the existing value guard and an unnecessary edit;
this limitation is recorded without modifying the frozen candidate mid-run.
Six cases used 45 calls, 157.664 active seconds and 134,472 reported tokens. These
passing cases do not qualify the unfinished twelve-case or all-mode runs, much less
live retrieval or release. No activation or deployment occurred.

Separately reviewed live utility assembly now owns remote control entry/body/exit,
read-only readers, captured native/direct model clients, real delivery/server scopes,
original rechecks and before/after corpus identity. Readiness exposes only port/url;
admitted request and corpus values are detached immutable JSON snapshots. Exact SSE
bytes and content-free exception chains survive failures. Review reproductions closed
second-cancellation cleanup races, successful-entry cancellation leaks, mutable-corpus
admission and cleanup masking of primary errors. Ten control and seven runtime
ownership tests passed independently. An additional date-rollover control checks
that midnight during corpus preflight prevents browser readiness and model start;
all eight runtime tests pass. These utilities grant no admission: concrete CLI,
process supervision, frontend assembly and live browser qualification remain pending.

The utility assembly full offline suite passed 830 tests in 26.656 seconds with
48 expected datastore skips. Both reviewers cleared the date-rollover regression.
Strict case06 also passed both original-source reviews with all error and
underreporting counts zero (four native calls, 12.395 seconds, 7,268 tokens).
The frozen application and fixed-original evaluation harness remain unchanged.

## September 10 continuation: native v6 stopped, live bridge reviewed

All twelve initial Strict cases finished once: eleven passed both grades and case11
failed both. Seventy native calls used 255.306 active seconds and 188,924 reported
tokens. The failing result SHA256 is
`9f8016b5e13fb59ac3bae84db7e25d95e8acbf7b5877cf48a757eedf83a09e73`.
Both grades report three missing required aspects and zero other hard error counts.
Six raw factual approvals were source-correct, but the final current-status gate
withheld all facts. Actual coverage/conservation restoration returns None. The
failure and dual aggregate receipt are frozen; all-mode, live and holdout execution
remain unstarted. No source-reporting improvement is claimed as qualification.

A captured-response replay reproduces the actual withheld answer without native
calls. A minimized temporal-gate control isolates a source field containing current
and none/none temporal metadata. Changing only that word removes the gate; explicit
source-observation metadata qualifies the unit; adding a separate non-temporal unit
makes the whole answer unresolved again. The proposed general contract correction
is in source-relative-temporal-acceptance.md and is under review before app changes.

The independent local/remote process and frontend utility reviews are clear. They
closed forced-exit/local-cleanup races, swallowed deadlines and an oversized-output
pipe deadlock. Twelve process controls and four local-child controls pass. The
optional actual Node24 standalone frontend smoke also passes through an owned
synthetic loopback backend, including denial of a mutation route and joined children.
The full offline suite passed 847 tests in 31.611 seconds, with 49 expected opt-in
skips; the standalone smoke was then run explicitly and passed. This is process and
HTTP validation, not actual native browser-query qualification. Concrete admission,
CLI/browser execution and result packaging remain unfinished.

The source-relative temporal contract is implemented and both reviews are clear.
An undated source report now retains its explicit source_observation assertion frame
without inventing a calendar date. Current-bearing units require that frame; other
units cannot lend it. Current-question demand stays conservative: an unscoped unit
remains unresolved even beside a source report. Active comparison metadata outside
the documented comparison contract is rejected at parser and finalizer boundaries.
Unit IDs and fixed temporal failure codes reach repair and public current-state
metadata, with an accurate status explanation instead of falsely claiming missing
source support. Finalization policy is source-audit-v26; query cache identity derives
from that version. The fresh evaluation manifest also binds the reviewed temporal spec.

Both review reproductions first failed new controls, then passed after correction.
Fourteen new tests cover the native parser, all-mode finalization, complete question
pipeline, saved success/failure restoration, current-question laundering, and foreign
or missing comparison IDs. Root's 69 focused tests and independent 25/67 focused
runs passed. The final full offline suite passed 861 tests in 31.652 seconds, with
49 expected opt-in skips. The prior bridge commit passed exact CI (34436837545).

A controlled intervention in the captured hours replay changes only ambiguous audit
scope to explicit source reporting and supplies a controlled coverage result. All
three original observations then survive with complete restored conservation. This
is a causal code-path control, not native model evidence. Unchanged captured none/none
responses still withhold the current-bearing unit; the original v6 result stays failed.
No fresh model call, activation or deployment has occurred for this new contract.

## Live browser owner implementation (September 10)

The live harness now has remote `prepare`/`run-case` commands and a local executable
owner. This remains inactive evaluation tooling, not production activation or a
successful live retrieval run. The frozen temporal-v1 candidate is unchanged.

The remote command repeats all-mode and predecessor-result admission before opening
readers. Private input admission binds independent reviews of the exact request,
rubric, inventory and original bytes. The local owner binds the repository build,
Node, Playwright and actual Chromium installation, owns forwarding and the browser,
waits for the nonce-bound remote stop acknowledgment, and retains remote artifacts
without overwriting any differing bytes. Result packaging additionally checks actual
engine question/mode/request identity, SSE conservation, source/corpus stability,
provider termination, complete captures and the independent grade chain.

Review reproduced and closed request-file replacement after manifest validation,
a wrong-question final passing a copied browser request, an unknown native capture
kind bypassing truncation checks, repeat cancellation interrupting child cleanup,
temporary-path traversal, and a different frontend resolving a different browser
installation. Public source projection now uses the same effective external
Paperless URL as the real HTTP routes. These changes affect evaluation tooling only.

A synthetic built-frontend run exercised the real HTTP/SSE handlers, source drawer,
Timeline source/answer links, one contextual submission, mobile restoration and
copied-answer equality. Its mobile screenshot was visually inspected. A second
assembled control exercised actual remote-control protocol, local forwarding,
Chromium and artifact collection with a disposable fake cluster. Browser-startup
failure drained the remote runtime, retained failure evidence and produced no
passing result. These tests establish transport/ownership behavior, not native
model accuracy. Qualification prerequisites and factual result packaging are
explicitly substituted in the fake-cluster control and tested separately.

Final offline validation ran 892 tests in 36.832 seconds with 53 expected opt-in
skips and no failures. Thirteen local owner/browser checks passed in 16.871 seconds.
An independent reviewer reran both assembled browser controls successfully in 6.312
seconds. The exact Node preload regression proves an external NODE_OPTIONS file
cannot execute: identity capture, the frontend and the browser all use the same
minimal OS environment, with explicit application listener settings. Cluster
context, namespace, pod and UID are also bound to the frozen manifest. The six real-corpus questions, all-mode native admission, sealed
holdout and GitOps release remain uncompleted gates. No deployment was performed.

## Fresh source-relative temporal qualification (September 10)

Candidate 9e79182f66f2a1289554e37527457ffd87d02823 completed all twelve Strict
development cases once, with independent specification and standards passes for
every exact result. All six failure counts are zero. Two requested aspects have
conservatively under-reported coverage despite their meaning being delivered;
these remain recorded separately. Total execution was 73 native calls, 299.914
active seconds and 217,214 reported tokens. The formerly failing hours case now
delivers all three source-supported meanings with complete restored coverage and
conservation. The failed predecessor remains failed and preserved.

The initial manifest is 601d40fcd314107c7f77ae83b1ff4a091e528bdd0b8199fcd644742a177ed442.
The unchanged application and initial 52 bound files, plus the all-mode spec,
are frozen for the separate 48-case all-mode run: manifest
c08af3c78a9856be3608a82777348d60cdd5a4bb2847743e4a25d4b0d38c60d8,
package a5921e0742c6b436ed2b89fe047ce55a51aaaa0d615330b5711a2f4617950920.
Its first Quick case passed both independent reviews; broader qualification remains pending. This is
synthetic fixed-original evidence, not real-corpus retrieval qualification.

Both reviewers approved the expanded live preparation: 135 originals and 139
input artifacts, canonical input-map hash
9c7554ce90467ad0f14f6a83ebaf6cd7551f436f561e4f7edac09b0714e53a9e.
The extra source review closes category omissions without requiring incidental
claim-administration detail. A fresh real corpus/index snapshot and all-mode
qualification are still required before live admission. The legacy 60-second
stream-verification setting is unused by the current query path; the actual SSE
loop emits status while awaiting the owned query task. No serving setting changed.


## All-mode rejection and audit-contract correction (September 10)

The temporal-v1 all-mode run stopped permanently at case 12 (treatment history,
Quick). Cases 00–11 passed both reviews; case 12 failed both because all three
required meanings were withheld. No raw or delivered false approvals occurred.
The 35 remaining cases were not executed. Aggregate execution: 109 native calls,
428.458 active seconds and 343,938 reported tokens. The exact failed result hash is
901952e63c03a82aeaeb29a56cf52a238083a4fc5bede7647bca2d890433ebe4.

The reproduced cause and independently reviewed plan are in
[audit contract consistency](../specs/audit-contract-consistency.md). An affirmative
historical comparison returned incompatible temporal scope and assertion fields.
The parser rejected this metadata but did not route it through the existing single
protocol correction. The implementation now checks cross-field coherence after
whole-batch negative precedence, shares valid temporal pairs with the finalizer,
clarifies the native instructions and changes the finalization policy to v27.
It does not normalize a contradictory response into acceptance or retry a negative
factual verdict.

The unchanged captured run still fails closed. A separate controlled replay retains
the original bad response, then supplies a coherent correction and coverage response;
it delivers all three requested meanings. This is an offline causal control, not
native accuracy evidence. The broader backend suite passed 903 tests with 53 expected
skips in 36.807 seconds. The new regression module subsequently passed all 12 tests,
including the added deadline and cancellation control. New native qualification,
live corpus/browser qualification, heldout evaluation and release remain pending.

## v27 development passes and live failure (September 10)

Published candidate `75ada6d` passed exact-head CI. Fresh twelve-case Strict
qualification passed both independent original-based reviews: 73 calls, 316.729
active seconds and 205,173 reported tokens. Fresh forty-eight-case all-mode
qualification also passed both reviews: 272 calls, 1,434.876 active seconds and
780,332 tokens. Both runs had zero hard failures; coverage was conservatively
under-reported for two and ten aspects respectively. These fixed-original results
do not establish real retrieval completeness.

The first real-corpus built-browser request then failed. It used 61 native calls
and 195.886 active seconds. All transports completed normally, and the browser
completed desktop/mobile source inspection and conversation restoration, but the
delivered answer was the generic fallback. Thirty-five source readers retained
155 observations; the hard eighty-unit ceiling prevented all audit dispatches.
The reconstructed candidate digest matches the retained result and has 38,576
characters with 209 eligible source windows. Runtime SHA is
`2956b80fd0344354845fa9f83fc10d9e0c4a3473ae991ffadeefd42d49baef9f`.
No passing `result.json` was emitted. Both failure reviews bind the runtime and all
226 retained artifacts; the remaining five scheduled cases were not run.

Original review separately found required records absent from source reading,
missing later sections of a retrieved document, and a material qualification lost
by a reader that had received its source text. The captures cannot distinguish
never-discovered from later-dropped documents in every case; those remain explicit
diagnostic limits. The rendered UI also leaked an incorrect indexed date and
unaudited draft follow-ups presupposing completed cancellation/current coverage.
Root visually inspected the actual mobile answer and desktop source/follow-up
capture. Browser transport success is not factual acceptance.

The reviewed next work is [large-evidence query execution](../specs/large-evidence-query-execution.md).
Its scheduling slice removes total-unit and total-repair-input eligibility gates,
retains four-unit batches/bounded workers and scales the deadline to actual work.
The new regressions first failed on the reproduced cliff and then passed; both
review axes cleared implementation. Seventy-four focused checks passed. The first
full suite ran 914 tests with 53 expected skips and two failures in legacy tests
that explicitly expected the retired size ceilings. Those tests now assert full
audit/conservation and preserved prose context; all 28 checks in their two modules
pass. Full validation after the remaining changes is still required.

Retrieval coverage and public presentation changes remain separate gates. No new
native qualification or GitOps activation is admitted while known omissions remain.

Slices A and C subsequently passed both independent implementation reviews. The
presentation fix removes unaudited date authority and draft follow-up presuppositions
from fresh, cached and saved pipeline responses; failed fresh source lists now
match restoration instead of exposing raw indexed headers. Successful source quotes
remain tied to validated references. Lower-level receipts retain their original
shape when no presentation fields are present.

Two actual built-browser controls passed in 6.667 seconds, exercising successful
source inspection and failed-answer presentation on desktop/mobile and private
conversation restoration. Root and the spec reviewer visually inspected the
captures. A separate production conversation-read control checks the stored
follow-up column; private browser history is not represented as that database test.
An initial browser control incorrectly expected background controls to remain
accessible through an open modal; that test expectation was corrected while
retaining the forbidden-content checks. All attempts remain in private artifacts.

Final backend validation ran 920 tests in 38.094 seconds: zero failures, 55 expected
opt-in skips. Earlier full-suite restoration equality failures exposed unwanted UI
fields being added to lower-level receipts; the implementation now normalizes only
existing presentation fields, and those original regression assertions pass unchanged.
The new execution spec is bound by evaluation manifests. No production activation
or fresh model qualification has followed the retained live failure.

## Source acquisition implementation (2026-09-10, not deployed)

Implemented the independently reviewed [source opportunity contract](../specs/source-opportunity-acquisition.md):
all emitted planned/gap/decomposition searches, both observed keyword samples,
paginated full-index lexical matches, and typed graph provenance retain their
discovered IDs. Full Paperless originals replace ranked document/window quotas
in the inactive question pipeline. Eligibility, completion, OCR/fingerprint freshness,
post-filter original interval coverage and the full reader-visible context are bound
before reading. Missing work prevents complete question coverage, while independently
verified facts remain deliverable. Acquisition identity is mandatory in coverage
and conservation bindings, including saved/cache restoration. Cancellation joins
workers and exposes retained completed/pending diagnostics to the capture owner.

Review reproduced and closed partial-context binding mutations, loss of completed
timeout diagnostics, stripping all acquisition metadata to upgrade saved coverage,
swallowed datastore/model failures, malformed gap planning defaulted to success, and
truncation of the already-observed union of two successful keyword searches. These
are deterministic transfer/delivery fixes, not evidence of improved model accuracy.

Validation: 940 offline backend tests passed in 39.188 seconds with 58 expected
opt-in skips. A unique disposable PostgreSQL schema exercised 521 synthetic
documents with lexical matches after character 4,000: 519 originals transferred,
one feedback-blocked and one unindexed lead retained as gaps. Desktop/mobile/source
inspection/reload browser tests passed twice, then passed again after the UI gained
a source-gap explanation. Visual inspection caught and corrected zero evidence-item
counts in the new pack. Webpack production build and lint passed; the default
Turbopack build hit a local port permission error. No production deployment or new
native model run occurred. The failed v27 live attempt remains failed.

Fresh production resource measurement is blocked by automatic approval review of
the diagnostic code upload and full production-document reads, including a revised
helper retaining only aggregate sizes/hashes. Neither rejected helper ran. The
unchanged-reader B2 diagnostic can still be prepared from already retained local
originals, with separate plan/runner review and native admission required.

### Approved measurement and boundary-cost diagnosis (September 10)

The user explicitly approved the previously blocked code upload and read-only
production measurement. That attempt ran, then was interrupted after more than
ten minutes without producing measurement.json. The helper exited with code 130;
the serving pod remained Running with zero restarts. No native model call or
production deployment occurred. Source-free progress and outcome artifacts remain
at /private/tmp/kg-acquisition-measure-20260910.

Progress records 36 completed discovery operations, 919 discovered documents,
903 supplied originals, nine ConnectTimeout states and seven citation interval
gaps. Observed original extent totals 17,807,343 characters; the largest is
1,211,567. Transfer snapshot status is complete, which does not mean source
coverage is complete. Packaging/sizing did not complete; final cancellation during
database cleanup does not identify the preceding CPU stack. This failed attempt
remains failed, with no resource-capacity or model-accuracy conclusion.

Independent local profiling reproduced roughly quadratic scalar-boundary work:
74k/148k/296k-character originals caused 1.40M/5.62M/22.78M characters to be
normalized. The same 1,211,567-character synthetic source spent 8.555 seconds in
span inventory before the fix and 0.893 seconds after it under cProfile. Tracing
Python allocations raised the original span time to 28.576 seconds. These are
local diagnostics, not measured production speedups.

Adaptive edge reads preserve the exact former boundary transforms, expanding
through arbitrary ignored runs and retaining legacy behavior for overlapping
markers. Original text, reader spans, and provenance are not shortened. A
separate reproduced issue allowed successful admission after synchronous
packaging consumed the acquisition deadline; before/after packaging checks now
withhold those originals and preserve transfer diagnostics. This prevents late
admission but does not promise preemption of synchronous CPU work.

948 backend tests passed in 39.263 seconds (58 expected opt-in skips), followed
by focused boundary/acquisition checks after the overlapping-marker review fix.
All six regenerated reader-retention input packages have exactly the prior
preparation hash 47a23363074360d65c3acb285e277075b2d4f2ff5c9b1aaee285d0d5d923de61.
No native diagnostic has run. Remaining work includes closure review, controlled
resource measurement with a process-owned timeout, citation-gap diagnosis, the
predeclared reader diagnostic and whole-query release gates.

### Whitespace conservation and completed acquisition measurement

The seven recorded citation gaps were reproduced against originals in memory;
all matched prior hashes and every missing character was whitespace. Nonblank
source windows now retain adjacent blank OCR runs with exact offsets and unique
handles. Entirely blank sources remain unavailable, and nonwhitespace gaps and
signed-value guards remain enforced. The former physical 4,000-character test
limit now applies to the nonblank core, allowing only the specified blank context.
Independent reviews passed Unicode, history-window, signed-value and exact-source
controls. The original-source replay then read all 16 target documents, with zero
read errors and zero citation gaps. No original text was retained by these helpers.

A new frozen model-free measurement completed in 171.006 seconds with 919 of 919
discovered originals supplied and complete interval coverage. Peak process RSS
was 707,854,336 bytes. First-reader maximum: 1,863,601 serialized bytes; total
reader bytes: 29,221,892; source inventory: 25,086,855; evidence pack: 38,433,687.
No native calls occurred, and the pod remained Running with zero restarts. Source-
free artifacts are in /private/tmp/kg-acquisition-measure-v2-20260910. Full later-
stage requests and provider capacity remain unestablished. Old timeouts did not
reproduce, but their historical cause remains unproven.

The reader-retention runner was independently reviewed against the preregistered
schedule. Nine local tests and separate actual-SDK MockTransport probes passed.
Review closed lost attempt accounting on capture failure, repeated-cancellation
cleanup, individual deadline classification (including a clock crossing), unsafe
post-validation rereads, incomplete runtime/lock/route binding and weak native
termination checks. These are runner controls, not native accuracy results.
First and correction request serialization for all 12 arms used local mock
transport only: largest requests 67,456 and 67,761 bytes respectively.

Final backend validation for this slice: 959 tests passed in 38.864 seconds with
58 expected opt-in skips. Concrete native admission is prepared locally, but
automatic approval review rejected uploading the sensitive document package to
the existing Kubernetes pod without explicit destination permission. That upload
did not run. Only code and non-secret runtime observation were subsequently sent;
no original, gold, prompt or wire payload was included in that allowed operation.
Native accuracy and downstream release gates remain open.

### Approved B2 native execution (September 10)

After explicit user approval, the exact independently reviewed package was uploaded
and executed in an isolated process. All 24 scheduled invocations completed in
124.217115 seconds with 24 native attempts, no corrections and no failed transport.
146 artifacts are frozen at /private/tmp/kg-reader-retention-native-v1-20260910.
Manifest SHA: 210ab31749914bf9b4ea2af97f91ed46b860a12e35d3ed933bdc6e3265107acf.
Run SHA: 3cafc99007d31af780fc27e9fe4c022777e66f69a56b93b71e66394b3324d14e.
Inventory SHA: 2293bcc8682b46d5467c2037f68447bf2576be85f7b5550ac5487192d8b53021.
Reported usage totals 99,994 input and 23,747 output tokens. Provider-reported
cache-read input tokens do not establish response-cache reuse; the configured
LiteLLM response-cache bypass remained bound in the requests.

Both completed independent semantic grades FAIL. Every one of the 106 observations
and 129 references was reviewed. Both full-source readings omit the supplied
coverage-summary replacement qualification; both arms lose its required
effective-time association. Further findings include a dropped condition,
misassociated record status, incomplete supporting references and unreconciled
printed arithmetic. Classification differences are being adjudicated separately;
the exclusive grade files and frozen gold remain unchanged. This keeps the
retention gate closed, regardless of transport success.
The unchanged failed v27 query and earlier measurement are not reclassified.
No deployment occurred. Published candidate 4262b01 passed CI run 34477375082.

The 24 captured native request bodies exactly match their frozen SDK wire
preflights, and all capture hashes validate. The omitted replacement qualification
is present in the first window of both arms at the same offset; the full-source
prompt is smaller. This is demonstrated interpretation loss, not a reproduced
context-window overflow or omitted tail transfer. Private grade hashes:
- Spec: 4b5f2cccb85f671b57514bab84f18178808c810c43adf34ac104442567dba6bc.
- Standards: c25d895f72a46cd1bbdc1994eac2095e5cd73f07e9a899cec86c9020a0eb5604.

### B2 grade reconciliation

The separate adjudication preserves both original grades and accepts the union of
source-grounded defects. Four effective-time losses and two summary-replacement
losses are six incompletely preserved required instances. The worksheet amount
itself survives; the dropped condition is classified as an extra assertion error.
The extra worksheet date is present in the original but absent from its cited
window, so reference support fails while ownership passes. Two outputs copy
printed figures but assert arithmetic relationships that those figures do not
satisfy; preserving individual numbers does not support the asserted sum.

Adjudicated R: 9/12 cells pass; F: 8/12 cells pass. All four short cross-domain
controls pass in both repetitions and arms. F preserves 40/44 required instances;
R preserves 40/42 supplied instances, with two separate transfer limitations.
Neither arm passes the fixed diagnostic. Adjudication SHA:
93517dc7abf16917f94b7680af6da6b54907266ee5f6b5d10c6f2439c5897fd2.
No new model output was obtained to settle a disagreement.

### Diagnostic-only interpretation recovery implementation

The independent design reviews selected addition-only review followed by the
unchanged source auditor. Implemented app/source_interpretation.py and a narrow
Strands omission-review adapter. No live pipeline flag, query routing, cache identity
or source-audit acceptance changed. Primary occurrences remain immutable and are
followed by referenced additions; receipts bind exact input/primary/additions,
attempts and implementation/prompt/schema identities. Execution success confers no
factual-support or source-completeness authority.

Review reproduced and closed nested metadata leakage and exception swallowing in
the actual native adapter. Canonical nested source shapes now reject extra metadata,
and omission review preserves timeout versus shared integrity failure while legacy
stages retain their existing fallback. Below-adapter regressions exercise these
failures. An adversarial false-primary/correct-addition test confirms that the false
primary still reaches audit, is rejected, and the surviving subset gets a fresh
audit; the final result remains explicitly partial.

Both reviewers cleared the slice. Thirty-one focused reader/recovery tests passed;
the full backend suite ran 972 tests in 37.970 seconds with 58 expected opt-in skips.
All frozen F canonical windows remained admitted unchanged in independent review.
No new native model calls occurred. The next matched recovery diagnostic is drafted
in docs/specs/source-interpretation-recovery-diagnostic.md; its runner, exact package,
wire preflight and admission remain outstanding. This is implementation evidence,
not a measured accuracy improvement or a release qualification.

Implementation commit 6dc7d143261f8207b5379860609236a11f6f11c7 passed CI
34481420307 (frontend and backend validation; image publication skipped on the
unmerged branch). Both reviewers also cleared the amended recovery diagnostic
protocol for runner/package preparation. It now explicitly requires all twelve
pairs, zero false raw approvals and wrong primary rejections in either arm,
uncertainty retention for uninterpretable raw attempts, pair-local execution stops,
and actual SDK-body capture for conditional baseline/combined calls. Protocol SHA:
c1f98867fb756320df6e6d0b307ccf58ebb27960bc00c1eaaa949a4db0ea4835.
This clears protocol review only; no runner/package admission or new native result
is claimed.


### Recovery runner admission and transfer block (September 10)

Diagnostic runner revision cf2c8c3 preserves matched frozen primary readings,
original source and auditor context, with no new retrieval or reader invocation.
It records actual SDK bodies, raw audit decisions before subset selection,
per-stage outcomes and spent calls. Review closed concurrent capture ownership,
pre-subset ledger write and captured-output read/hash failure paths. Shared
capture failures now stop the run; individual model failures stop their pair.
Ten focused runner tests pass, including full-run failure accounting and repeated
cancellation. The preceding full suite passed 982 tests with 58 expected skips;
CI 34484126557 passed at e6a14b7. The guarded-read fix is published at cf2c8c3.

Both independent reviewers approved exact subject
 a7f623dd8ed959e9f93c7d8c42a000ecf3016d9c12a1dcdcf1e65f996504bb68.
The private package is /private/tmp/kg-source-recovery-v2-20260910, with 518
bound input files and 80 code files. Fresh local pinned-SDK MockTransport preflight
serialized all 12 pairs into 68 requests, largest 78,862 bytes, with zero native
calls. Full code and runtime identity matched the isolated cluster interpreter.
Final manifest SHA:
73d50631c73216cb349af6875967363bf1ace7e0c45a76f28d178f422cea56b2.

Automatic approval review rejected upload of the private input tarball to the
existing diagnostic pod because it did not recognize specific authorization for
that payload and destination. No workaround or retry followed. Code-only upload
and runtime probing succeeded; private inputs were not uploaded by the rejected
operation, and no recovery native calls ran. This is technical admission, not an
accuracy result. The frozen package remains ready for the explicitly approved
transfer and run; no production activation or GitOps change occurred.

Remaining sequence: upload the admitted inputs after explicit transfer approval,
validate the complete remote package, execute the fixed 12 pairs once, freeze all
artifacts, obtain both independent semantic grades, then decide whether the
candidate can advance. Preserve the existing B2 failure and all later whole-query,
held-out, UI and GitOps release gates. No rerun or semantic retry is authorized by
unused diagnostic budget.


### Approved recovery upload; downstream execution block

The user explicitly approved the named private package transfer and comparison.
Upload to tools/paperless-kg-api-6bf8c7564f-x2pxz succeeded, and the remote runner
validated the final manifest, both receipts, input hashes, code and runtime before
any model call. Automatic approval review then separately rejected native execution:
the stated reason was that approval named the pod/package but not the downstream
model provider. No retry or workaround followed; zero recovery native calls ran.
Read-only route inspection confirmed litellm.tools.svc.cluster.local, alias
 gemini-3.8-flash, upstream gemini/gemini-3.8-flash (Google Gemini), with no custom
upstream base URL reported. Only selected routing metadata was output; credentials
and source text were not displayed. Explicit downstream authorization is the next
step; the same frozen package remains ready and must not be resumed or replaced.


### Approved recovery native execution (September 10)

After explicit downstream-provider approval, the exact admitted v2 package ran
once through the configured LiteLLM route to Google Gemini. All twelve pairs and
all scheduled stages completed in 454.269106 seconds using 55 native calls.
There were no failed pairs. The 441 result artifacts are frozen under
/private/tmp/kg-source-recovery-v2-20260910/results. Run SHA:
0c82c224ca56393d985e7d19170d27305b62e263a0d2ff66206a11bd69e66c38.
Result inventory SHA:
693d552b167d297a134f1e988928e06bdccf052eec4697d3eac3ed3c38976358.
All 307 registered artifact hashes and all 55 raw SDK body hashes validated.

Eleven recoveries returned no additions; one returned two. All twelve final
answer strings exactly match their corresponding baseline. Both replacement
recoveries left the known required omissions untouched, establishing diagnostic
failure. Both complete independent semantic grades FAIL; no success is inferred
from clean execution or the count of supported claims. CI 34486493350 passed at
a37ceae, which contains the frozen cf2c8c3 runner code plus status documentation.

A separate model-free reproduction isolates explicit currency amounts after compact
slash separators being dropped by the quantity tokenizer. Adding whitespace alone
makes the same amount pass. The proposed bounded correction is specified in
source-quantity-token-boundaries.md. Short-year date refusal is an explicit
unspecified-century rule and requires a separate context policy; it is not being
silently removed. Neither observation changes the frozen run or authorizes live
recovery activation, reindexing, or deployment.


Both grades agree that each arm delivers 36/44 required meanings and passes eight
of twelve pairs, with zero newly recovered required meanings and no unsupported
final assertions. Both find one unsupported new addition and seven false factual
raw approvals. The spec review additionally identifies two initial approvals whose
single cited window cannot support a whole-package absence assertion; the subsequent
subset rejection is classified differently by the original standards grade.
Reconciliation resolved this difference against the original claim/reference scope:
both initial absence approvals are false reference approvals, and the subset
rejections are not established wrongful rejections. All twelve windows remained
available; reduced source transfer is not the mechanism. Two short-year expansion
rejections are separately classified as explicit policy limitations, not proven
parser defects. Agreed totals are nine false raw approvals (seven factual, two
reference-scope), fourteen wrong primary deterministic rejections, one wrong
addition rejection and six collateral correct primary occurrences withheld after
subset failure. Both original grade files remain immutable. Adjudication SHA:
d8a5fa552aaa31738fa882ac6daba53861e1e6917f8166bdc245d160b9bd2645.
Grade hashes:
- Spec: 195c88eb824f520aaa108506bf1033d25f970c9642eb921b7ae62245a8312202.
- Standards: 34a9b892930dadfd8f3bc0b44f74c5f98d450d7cb0d6dbbff5f8bccecec6ba16.

The separately specified currency-boundary fix is implemented at 5238bd1. Both
independent implementation reviews are clear. Nineteen structural quantity tests
and the full 984-test backend suite pass (58 expected opt-in skips). An offline
replay of the frozen source-correct coverage claims removes eight slash-related
false value rejections; it does not run models, change native artifacts, correct
other unit/date policy boundaries or qualify the failed recovery approach.

The next interpretation design must address the demonstrated loss and false raw
approvals before live integration. Addition-only review of a prior summary has no
measured delivered benefit here. Evaluate source-local structured observations and
verification against original record sections as a separate design alternative,
with explicit qualification/date/action roles and retained rejected dispositions.
This remains an architecture candidate requiring spec/review and a fresh controlled
comparison, not a measured solution. G4 large-source payload design, whole-query and
held-out evaluation, visual UI acceptance and GitOps release remain open.


### Source-record candidate: first model-free slice

After the failed recovery result, three independently proposed interfaces were
compared: source-record compiler, relationship algebra and passage dossier. The
chosen source-record candidate keeps exhaustive exact-source accounting plus
explicit original anchors for later relationships. This is a new G2 hypothesis;
no semantic or production gate has passed. The implementation plan is
[Source record interpretation](../specs/source-record-interpretation.md).

Implemented an immutable source-block inventory and strict all-block reading
binder. Markdown block maps guide segmentation but never filter original text.
Every character remains represented once; large blocks are never truncated.
Binding requires every inventory block, unique source-owned IDs, own-block
references on interpreted observations and explicit unresolved reasons. Same-document
qualification references are permitted; foreign-document references are rejected.
Accounting completion never establishes support, question or archive completeness.
No external client, model stage, routing, storage or ingestion behavior changed.

Both plan/implementation reviews cleared the slice. Six public tests and the
full 990-test backend suite pass (58 expected opt-in skips, 39.644 seconds).
All six frozen full originals reconstruct exactly from 362 blocks; largest block
2,217 characters/bytes. Actual model requests remain unmeasured because context,
protocol and schema overhead must be bound with the next adapter admission.
The failed B2 and recovery runs remain immutable. The prior parser-fix revision
1e08f25 passed CI 34490163999. No deployment occurred.


### Source-record reader adapter and reader-only wire preflight

Implemented the separately reviewed source-record reader protocol: eight focus
blocks per sequential call, full owning original context, no previous interpretation
or answer, exact per-focus output accounting and no retries. Unresolved blocks remain
visible without semantic coverage credit. A source-bound projection maps every
interpreted occurrence to all overlapping admitted original citation windows and
fails before audit on changed identity, missing coverage or foreign references.
No observation is rewritten or silently discarded during projection.

Independent review reproduced and closed repeated/nested cancellation interrupting
cleanup, unbound citation identities, adapter errors being misclassified as parse
errors, and preflight rereads after hash validation. Tests cover the actual Strands
exception boundary and pinned-SDK MockTransport file-mutation case. The shared
owned-call helper is reused by the earlier diagnostic runner. Both review axes are
clear for this slice. The full suite passes 1,002 tests (58 expected skips, 40.502s).

The fresh reader-only preflight has 100 requests across all twelve cases, largest
109,369 bytes, total 9,142,590 bytes. All original projections validate. Zero native
calls were made. The source-record-native-diagnostic spec records the exact report
and inventory hashes. This does not prepare or qualify the complete comparison:
audit-side serialization/capture, complete-run ownership and exact budget/runtime
admission remain next. No claim of improved semantic accuracy or deployment is made.

## Source-record comparison ready; native execution approval blocked

At 2e28074 the complete comparison runner and both audit arms are implemented and
independently reviewed. Both arms receive the same full originals with empty prior
reading notes. All interpreted observations and their source mappings, raw audit
ledgers and final subset decisions remain captured. Full backend suite: 1,009 tests,
58 expected skips, 42.923 seconds; exact-head CI 34499836068 passed.

The actual pinned-SDK localhost preflight serialized 304 requests (100 reader,
16 baseline audit, 188 synthetic fresh-record audit), largest 109,411 bytes. Two
independent admissions bind the exact code and 1,711 private input artifacts; remote
validation passed. Automatic approval review then rejected native execution because
it requires specific approval for this experiment's private source text/prompts to
Google Gemini through the existing LiteLLM route. No native calls or result artifacts
exist. This is no new semantic result, improvement or production release.

The exact package hashes, scope, budgets, blocker and continuation command contract
are recorded in [source-record diagnostic](../specs/source-record-native-diagnostic.md).
The next step remains that admitted native comparison followed by two independent
all-output grades. Previous failed results and all later release gates are unchanged.

## Completed source-record and reader/verifier comparisons

After subsequent explicit approval, the source-record comparison ran once. Ten
pairs completed; two failed with reader timeouts. Both complete independent grades
FAIL: there was no net required-meaning gain and three unsupported final assertions.
The prior readiness/blocker record above is historical. Exact frozen receipts remain
in the [source-record diagnostic](../specs/source-record-native-diagnostic.md).

The next approved model comparison at frozen application 6d72699 completed all
36 route executions in 2,169.744 seconds using 261 calls, without transport failures.
Both routes received the same originals and prompts. Independent grades reviewed
all 191 fresh reader observations, 771 raw assessments and 72 final answers. Both
FAIL. They agree on fresh-final required retention of 54/66 for Gemini and 52/66
for the OpenAI comparison route; baseline-final retention is 58/66 versus 45/66.
The stronger reader's content presence improved, but verification and delivery
still lost required meaning. It is not a qualified route change.

Raw semantic categories and selected-citation scope have been reconciled separately
without changing either frozen grade. Truth against complete originals and sufficient selected references
are distinct obligations; zero definite false delivered facts cannot stand in for
complete attribution or coverage. The comparison route has five citation-deficient
deliveries despite zero definite false delivered facts. Distinct raw approval
failures across factual/calendar/reference axes are 33 versus 12, and definite
supported-meaning rejection errors are four versus twelve; uncertain compound
framing stays quarantined. The [model diagnostic](../specs/reader-verifier-model-diagnostic.md)
holds exact execution, inventory and frozen independent-grade identities.

One confirmed local repair, 35f6460, preserves short/full numeric date precision
across horizontal slash spacing. Before the fix, native supported worksheet facts
were withheld solely because the original spaced date and compact date failed
calendar matching. Eighteen synthetic regression subcases reproduced the failure;
both implementation review axes are now clear, and 1,018 backend tests pass with
58 expected skips (50.823 seconds). Adversarial chain/identifier and prose-separator
controls are included. This code result does not retrospectively change any native
grade or trigger a production release. The general next steps are tracked in the
[source-grounded contract plan](../specs/source-grounded-interpretation-contract.md).
