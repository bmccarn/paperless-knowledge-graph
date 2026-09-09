# Query reliability evaluation: G1

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

- The approved context comparison is complete. Retain its variable failure pattern
  and source-valid 183-window reproducer; do not reinterpret the four-document
  control as an accepted source-selection policy.
- Continue minimizing the now-reproduced semantic failure while preserving failure. Original native
  inputs were not retained for the live failure, so reconstructed inputs must be
  labelled as reconstructed, not asserted byte-identical.
- Add representative large-context/disjoint-window/source-order conditions and
  end-to-end question-aspect evaluation. Compact controls alone cannot pass G1.
- Freeze G2 experiment counts, cache observations, cost/time bounds and decision
  criteria before comparing any candidate. No architecture has been selected.

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
(192 assertion observations,96 positive/96 negative). Flat used 48 native invocations
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
fewer facts. The final candidate was reviewed at `a589ff6`; the subsequent SDK1.55
lock refresh has separate engineering checks and has not inherited these SDK1.54
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
