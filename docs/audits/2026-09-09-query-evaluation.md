# Query reliability evaluation: G1

Spec: [evaluation before implementation](../specs/query-reliability-evaluation.md).
Starting production revision: `809046021573a5c0e9eeb65866f5e49dcfe6ae5b`.
Evaluation harness reviewed at `c682f58faf1e12dd61ed080f79dd20b68ab65be2`.

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
26 tests passed (including subsequent original-window and continuity validation). Controlled transports in capture tests measure instrumentation,
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
reconstruction; the original raw native request was not retained. No standalone
private replay has run.

## Remaining G1 work

- Complete and analyze the frozen native baseline without changing labels or
  rerolling failed cases.
- Independently label and obtain specifically scoped authorization for the
  retained private-case model replay. Earlier automatic rejection still applies;
  local reconstruction and synthetic calls do not authorize that replay.
- Reproduce the semantic failure under representative source/batch context, retain
  exact input/output, then minimize while preserving failure. Original native
  inputs were not retained for the live failure, so reconstructed inputs must be
  labelled as reconstructed, not asserted byte-identical.
- Add representative large-context/disjoint-window/source-order conditions and
  end-to-end question-aspect evaluation. Compact controls alone cannot pass G1.
- Freeze G2 experiment counts, cache observations, cost/time bounds and decision
  criteria before comparing any candidate. No architecture has been selected.
