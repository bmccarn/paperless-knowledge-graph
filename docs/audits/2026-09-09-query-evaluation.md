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
