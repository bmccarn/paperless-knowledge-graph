# Query reliability evaluation: G1

Spec: [evaluation before implementation](../specs/query-reliability-evaluation.md).
Starting production revision: `809046021573a5c0e9eeb65866f5e49dcfe6ae5b`.
Initial native baseline harness reviewed at `c682f58faf1e12dd61ed080f79dd20b68ab65be2`.
Latest evaluator and cache controls reviewed at `e358a0c039cbd0a94a269eb66af399edceabd1e4`.

## Status

G1 is in progress. No production behavior, model route, output cap, indexed data,
review history or deployment has changed. G2–G6 remain unstarted. A working
harness is not a semantic reproduction or an accepted fix.

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
identified nine windows with copied table headers that are not contiguous original
passages. That captured input is classified as an invalid reconstruction; it cannot
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
table windows, so this contrast does not identify the precise cause. In the full
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
Further private calls require an extended bounded budget; none have run.

## Remaining G1 work

- Run the next frozen context comparison only after approval of its new call
  budget; the original six-call scope is complete. Retain all outcomes and labels.
- Continue minimizing the now-reproduced semantic failure while preserving failure. Original native
  inputs were not retained for the live failure, so reconstructed inputs must be
  labelled as reconstructed, not asserted byte-identical.
- Add representative large-context/disjoint-window/source-order conditions and
  end-to-end question-aspect evaluation. Compact controls alone cannot pass G1.
- Freeze G2 experiment counts, cache observations, cost/time bounds and decision
  criteria before comparing any candidate. No architecture has been selected.
